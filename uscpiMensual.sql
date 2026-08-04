-- =====================================================================
-- Deflactor de EEUU: CPI-U mensual, sin huecos.
--
-- public."USCPI" guarda la serie del BLS en formato (series_id, year, period,
-- value), donde `period` es M01..M12 y además M13 = PROMEDIO ANUAL. Esta vista:
--   * se queda con la serie CUUR0000SA0 (CPI-U, US city average, all items,
--     not seasonally adjusted),
--   * descarta M13, que no es un mes,
--   * mapea (year, period) a `fecha` = primer día del mes, para poder joinear
--     por fecha igual que ipc_largo,
--   * RELLENA los meses faltantes por interpolación geométrica y los marca en
--     la columna `interpolado`.
--
-- POR QUÉ HAY QUE RELLENAR
-- Falta octubre-2025: el BLS no lo publicó (la recolección se interrumpió por
-- el shutdown de ese mes), así que es un agujero permanente y no algo que se
-- arregle recorriendo el ETL de nuevo. Un hueco mensual hace que el
-- desestacionalizador X-13 saltee la serie entera ("la serie tiene huecos
-- mensuales"), así que sin relleno no hay serie real en dólares ajustable.
--
-- El índice venía planchado alrededor de ese mes (sep 324,800 / nov 324,122),
-- así que el valor interpolado (324,461) está muy acotado: implica -0,104%
-- mensual y cualquier camino razonable cae dentro de ±0,2%.
--
-- La interpolación es GEOMÉTRICA y general, no un promedio de dos vecinos:
-- recorre el camino de tasa constante entre el último publicado y el siguiente,
-- así que si algún día faltan dos meses seguidos sigue dando algo sensato en
-- lugar de un valor mal. Para un solo mes faltante equivale a sqrt(ant * sig).
--
-- SIEMPRE filtrar o mirar `interpolado` antes de presentar el dato como del
-- BLS. Es una estimación nuestra, no una publicación oficial.
--
-- NSA a propósito: para deflactar se usa el índice sin ajuste estacional; el
-- ajuste se hace después, sobre la serie ya deflactada.
-- =====================================================================

CREATE OR REPLACE VIEW public.uscpi_mensual AS
WITH publicado AS (
    SELECT make_date(year::int, substring(period FROM 2)::int, 1) AS fecha,
           value::numeric                                        AS indice
    FROM public."USCPI"
    WHERE series_id = 'CUUR0000SA0'
      AND period ~ '^M(0[1-9]|1[0-2])$'
),
calendario AS (
    -- ::timestamp (SIN zona) a propósito. Con `date` los límites se castean a
    -- timestamptz y generate_series itera acumulando, así que cada cambio de
    -- offset histórico de America/Argentina/Buenos_Aires le corre la hora de
    -- pared de forma permanente: 00:00:00 hasta abr-1920, 00:16:48 cuando el
    -- país estandariza a -04:00, y 01:16:48 desde dic-1930. Con ese arrastre el
    -- último candidato (2026-06-01 01:16:48-03) supera el tope (2026-06-01
    -- 00:00:00-03) y se descarta: la serie perdía el mes más nuevo EN SILENCIO.
    -- timestamp no tiene reglas de zona, así que la grilla es exacta.
    SELECT generate_series((SELECT min(fecha) FROM publicado)::timestamp,
                           (SELECT max(fecha) FROM publicado)::timestamp,
                           interval '1 month')::date AS fecha
),
juntado AS (
    SELECT c.fecha, p.indice
    FROM calendario c
    LEFT JOIN publicado p ON p.fecha = c.fecha
)
SELECT
    j.fecha,
    COALESCE(
        j.indice,
        -- camino geométrico de tasa constante entre el anterior y el siguiente
        ant.indice * power(
            sig.indice / ant.indice,
            ( (extract(year FROM j.fecha)   * 12 + extract(month FROM j.fecha))
            - (extract(year FROM ant.fecha) * 12 + extract(month FROM ant.fecha)) )
            / NULLIF(
                (extract(year FROM sig.fecha) * 12 + extract(month FROM sig.fecha))
              - (extract(year FROM ant.fecha) * 12 + extract(month FROM ant.fecha)), 0)
        )
    )                     AS indice,
    (j.indice IS NULL)    AS interpolado
FROM juntado j
LEFT JOIN LATERAL (
    SELECT p.fecha, p.indice FROM publicado p
    WHERE p.fecha < j.fecha ORDER BY p.fecha DESC LIMIT 1
) ant ON j.indice IS NULL
LEFT JOIN LATERAL (
    SELECT p.fecha, p.indice FROM publicado p
    WHERE p.fecha > j.fecha ORDER BY p.fecha LIMIT 1
) sig ON j.indice IS NULL;

COMMENT ON VIEW public.uscpi_mensual IS
'CPI-U de EEUU (BLS, serie CUUR0000SA0, all items, NOT seasonally adjusted) en formato mensual con fecha al primer dia del mes, para deflactar series en dolares joineando por fecha igual que ipc_largo. Descarta el period M13 de "USCPI", que es el promedio anual y no un mes. Rellena los meses faltantes por interpolacion geometrica de tasa constante entre el ultimo publicado y el siguiente, y los marca con interpolado = true: hoy el unico es octubre-2025, que el BLS no publico por el shutdown. FILTRAR O MIRAR interpolado antes de presentar un valor como dato del BLS.';


-- =====================================================================
-- Checks de validación
-- =====================================================================

-- 1. Sin huecos: filas = meses entre el primero y el último.
SELECT count(*)                        AS filas,
       count(DISTINCT fecha)           AS fechas_unicas,
       min(fecha)                      AS desde,
       max(fecha)                      AS hasta,
       extract(year  FROM age(max(fecha), min(fecha))) * 12
     + extract(month FROM age(max(fecha), min(fecha))) + 1 AS meses_esperados,
       count(*) FILTER (WHERE interpolado) AS interpolados
FROM public.uscpi_mensual;

-- 2. Qué meses están interpolados y con qué valor. Esperado hoy: sólo 2025-10.
SELECT fecha, round(indice, 3) AS indice
FROM public.uscpi_mensual
WHERE interpolado
ORDER BY fecha;

-- 3. Ningún índice nulo ni <= 0 (rompería la deflactación).
SELECT count(*) AS indices_invalidos
FROM public.uscpi_mensual
WHERE indice IS NULL OR indice <= 0;

-- 4. Los no interpolados tienen que coincidir exactos con "USCPI" (0 filas = OK).
SELECT u.fecha, u.indice, o.value
FROM public.uscpi_mensual u
JOIN public."USCPI" o
  ON o.series_id = 'CUUR0000SA0'
 AND make_date(o.year::int, substring(o.period FROM 2)::int, 1) = u.fecha
 AND o.period ~ '^M(0[1-9]|1[0-2])$'
WHERE NOT u.interpolado AND u.indice <> o.value::numeric;

-- 5. El interpolado tiene que caer entre sus vecinos (0 filas = OK).
SELECT fecha, indice
FROM (
    SELECT fecha, indice, interpolado,
           lag(indice)  OVER (ORDER BY fecha) AS ant,
           lead(indice) OVER (ORDER BY fecha) AS sig
    FROM public.uscpi_mensual
) t
WHERE interpolado
  AND (indice < least(ant, sig) OR indice > greatest(ant, sig));
