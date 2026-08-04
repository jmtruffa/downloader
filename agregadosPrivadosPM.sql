-- =====================================================================
-- Agregados monetarios del sector privado, PROMEDIO MENSUAL (tipoSerie = PM).
--
-- Hermana mensual de public."agregadosPrivados" (que es la serie diaria,
-- tipoSerie = D). Mismas definiciones de BM, Circulante, M1, M2 y M3; lo único
-- que cambia es la serie de origen y, con ella, las variaciones disponibles.
--
-- POR QUÉ SEPARADA Y NO UNA MATVIEW CON COLUMNA tipoSerie
-- Las variaciones no significan lo mismo en cada serie: en la mensual varMens
-- es la variación contra el mes anterior, y en la diaria no hay equivalente
-- útil. Objetos separados dejan cada semántica explícita y evitan tocar la
-- diaria, ya validada.
--
-- POR QUÉ MATCH EXACTO Y NO LATERAL
-- Al revés que la diaria, la serie PM no tiene huecos: 283 meses entre 2003-01
-- y 2026-07, todos al día 1, sin faltantes (verificado 2026-08-04). Con una
-- serie mensual completa el match exacto es la semántica correcta, y además es
-- gap-safe: si el BCRA revisa la serie y aparece un hueco, la variación sale
-- NULL en lugar de comparar contra el mes equivocado, que es lo que haría un
-- lag() posicional.
--
-- FECHAS AL DÍA 1, IGUAL QUE public.ipc_largo, así que se puede deflactar con
-- un JOIN directo por fecha. A propósito NO se hace acá: los agregados quedan
-- nominales. La deflactación se define una sola vez, junto con la de prestamos.
-- Ojo al deflactar: el IPC va un mes atrás, así que el último mes de esta
-- serie no tiene deflactor (hoy 2026-07).
--
-- REFRESH: lo hace serieseDownloaderPostgres.py al final de cada corrida
-- (cron 25 18 * * 1-5), después de cargar depositos y bmBCRA.
--
-- Este archivo es re-ejecutable entero.
-- =====================================================================

DROP MATERIALIZED VIEW IF EXISTS public."agregadosPrivadosPM";

CREATE MATERIALIZED VIEW public."agregadosPrivadosPM" AS
WITH base AS (
    SELECT
        a.date,
        b."sdVBMTotal"            AS "BM",
        b."sdBMCMBilletesPublico" AS "Circulante",

        -- M1: circulante en poder del público + depósitos a la vista
        COALESCE(b."sdBMCMBilletesPublico", 0)
      + COALESCE(a."pSPPesosCtaCte", 0)          AS "M1",

        -- M2: M1 + caja de ahorro
        COALESCE(b."sdBMCMBilletesPublico", 0)
      + COALESCE(a."pSPPesosCtaCte", 0)
      + COALESCE(a."pSPPesosCA", 0)              AS "M2",

        -- M3: M2 + plazo fijo (ajustado y no ajustado) + otros
        COALESCE(b."sdBMCMBilletesPublico", 0)
      + COALESCE(a."pSPPesosCtaCte", 0)
      + COALESCE(a."pSPPesosCA", 0)
      + COALESCE(a."pSPPesosPFNoAjust", 0)
      + COALESCE(a."pSPPesosPFAjustCERUVA", 0)
      + COALESCE(a."pSPPesosOtros", 0)           AS "M3"
    FROM public.depositos a
    JOIN public."bmBCRA"  b ON b.date = a.date
    WHERE a."tipoSerie" = 'PM'
      AND b."tipoSerie" = 'PM'
)
SELECT
    b.date,
    b."BM",
    b."Circulante",
    b."M1",
    b."M2",
    b."M3",
    -- NULLIF evita la división por cero si alguna serie arranca en 0.
    b."BM"         / NULLIF(m."BM", 0)         - 1 AS "varMensBM",
    b."Circulante" / NULLIF(m."Circulante", 0) - 1 AS "varMensCirculante",
    b."M1"         / NULLIF(m."M1", 0)         - 1 AS "varMensM1",
    b."M2"         / NULLIF(m."M2", 0)         - 1 AS "varMensM2",
    b."M3"         / NULLIF(m."M3", 0)         - 1 AS "varMensM3",
    b."BM"         / NULLIF(y."BM", 0)         - 1 AS "varAnualBM",
    b."Circulante" / NULLIF(y."Circulante", 0) - 1 AS "varAnualCirculante",
    b."M1"         / NULLIF(y."M1", 0)         - 1 AS "varAnualM1",
    b."M2"         / NULLIF(y."M2", 0)         - 1 AS "varAnualM2",
    b."M3"         / NULLIF(y."M3", 0)         - 1 AS "varAnualM3"
FROM base b
LEFT JOIN base m ON m.date = b.date - interval '1 month'
LEFT JOIN base y ON y.date = b.date - interval '1 year'
WITH DATA;

-- UNIQUE es obligatorio para poder refrescar CONCURRENTLY.
CREATE UNIQUE INDEX IF NOT EXISTS "agregadosPrivadosPM_date_uidx"
    ON public."agregadosPrivadosPM" (date);

COMMENT ON MATERIALIZED VIEW public."agregadosPrivadosPM" IS
'Agregados monetarios del sector privado, PROMEDIO MENSUAL (tipoSerie = PM), derivada de depositos y bmBCRA. Hermana mensual de agregadosPrivados (que es diaria, tipoSerie = D), con las mismas definiciones: BM = sdVBMTotal, Circulante = sdBMCMBilletesPublico, M1 = circulante + cuenta corriente, M2 = M1 + caja de ahorro, M3 = M2 + plazo fijo no ajustado + plazo fijo CER/UVA + otros. Todo sector privado (columnas pSPPesos*), por eso NO coincide con el M2 publicado por el BCRA, que es total. varMens* y varAnual* en tanto por uno, por match exacto de fecha contra el mes anterior y contra el mismo mes del anio anterior; si falta el mes de referencia da NULL en lugar de comparar contra el mes equivocado. Serie NOMINAL: no esta deflactada. Las fechas van al dia 1 igual que ipc_largo, asi que se deflacta con un JOIN por fecha, teniendo en cuenta que el IPC va un mes atras y el ultimo mes no tiene deflactor. REFRESH: lo hace serieseDownloaderPostgres.py al final de cada corrida con REFRESH MATERIALIZED VIEW CONCURRENTLY.';


-- =====================================================================
-- Checks de validación
-- =====================================================================

-- 1. Cobertura y huecos. Esperado: 283 / 283 / 2003-01-01 / 2026-07-01 / 283,
--    y 0 fechas que no caigan el día 1.
SELECT count(*)                             AS filas,
       count(DISTINCT date)                 AS fechas_unicas,
       min(date)                            AS desde,
       max(date)                            AS hasta,
       extract(year  FROM age(max(date), min(date))) * 12
     + extract(month FROM age(max(date), min(date))) + 1 AS meses_esperados,
       count(*) FILTER (WHERE extract(day FROM date) <> 1) AS no_dia_1
FROM public."agregadosPrivadosPM";

-- 2. Variaciones presentes. Esperado: 282 con varMens (falta el primer mes) y
--    271 con varAnual (faltan los primeros 12 meses).
SELECT count("varMensM3")  AS con_var_mens,
       count("varAnualM3") AS con_var_anual
FROM public."agregadosPrivadosPM";

-- 3. Definiciones contra las fuentes (0 filas = OK).
SELECT a.date
FROM public."agregadosPrivadosPM" a
JOIN public.depositos d ON d.date = a.date AND d."tipoSerie" = 'PM'
JOIN public."bmBCRA"  b ON b.date = a.date AND b."tipoSerie" = 'PM'
WHERE abs(a."M1" - (COALESCE(b."sdBMCMBilletesPublico",0)
                  + COALESCE(d."pSPPesosCtaCte",0))) > 1e-6
   OR abs(a."M2" - (COALESCE(b."sdBMCMBilletesPublico",0)
                  + COALESCE(d."pSPPesosCtaCte",0)
                  + COALESCE(d."pSPPesosCA",0))) > 1e-6;

-- 4. Jerarquía: M1 <= M2 <= M3 siempre (0 filas = OK).
SELECT date, "M1", "M2", "M3"
FROM public."agregadosPrivadosPM"
WHERE "M1" > "M2" OR "M2" > "M3";

-- 5. Coherencia con la serie diaria: el PM de un mes tiene que caer entre el
--    mínimo y el máximo diario de ese mes (0 filas = OK).
SELECT pm.date, pm."M3" AS m3_pm, d.m3_min, d.m3_max
FROM public."agregadosPrivadosPM" pm
JOIN (
    SELECT date_trunc('month', date)::date AS mes,
           min("M3") AS m3_min, max("M3") AS m3_max
    FROM public."agregadosPrivados"
    GROUP BY 1
) d ON d.mes = pm.date
WHERE pm."M3" < d.m3_min OR pm."M3" > d.m3_max;

-- 6. Últimas filas, para mirar magnitudes.
SELECT date,
       round("M2"::numeric, 0)          AS "M2",
       round("M3"::numeric, 0)          AS "M3",
       round("varMensM3"::numeric, 4)   AS "varMensM3",
       round("varAnualM3"::numeric, 4)  AS "varAnualM3"
FROM public."agregadosPrivadosPM"
ORDER BY date DESC
LIMIT 6;
