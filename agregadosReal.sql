-- =====================================================================
-- Agregados monetarios del sector privado a valores reales, promedio mensual.
--
-- Paso 1 de dos, gemelo de prestamosReal.sql. Acá queda la serie deflactada; el
-- ajuste estacional va en agregadosDesest.sql + el módulo de Python, porque X-13
-- es un binario externo y no se puede calcular en SQL.
--
-- FUENTE: public."agregadosPrivadosPM" (promedio mensual, tipoSerie = PM), 283
-- meses desde 2003-01, fecha al primer día del mes, sin huecos. NO la serie
-- diaria: X-13 es mensual y el IPC también.
--
-- UN SOLO DEFLACTOR. A diferencia de préstamos, acá no hay pata en dólares: los
-- cinco agregados son en pesos (circulante, cuenta corriente, caja de ahorro,
-- plazo fijo), así que todos van contra 'ipc_largo' de public.deflactores, que
-- empalma el IPC publicado con las proyecciones cargadas.
--
-- BASE: último mes PUBLICADO del deflactor, calculada dinámicamente. Publicado y
-- no proyectado a propósito: si la base fuera un mes proyectado, corregir la
-- proyección movería TODA la serie real, y no sólo los meses proyectados.
-- Cuando entra un mes nuevo de IPC, la serie real entera se reescala: los niveles
-- cambian, las variaciones no, y X-13 es invariante a escala.
--
-- HASTA DÓNDE LLEGA: hasta el último mes que tenga deflactor, publicado o
-- proyectado. Con las proyecciones cargadas hoy llega a 2026-07, o sea la misma
-- cobertura que la serie nominal. Sin proyecciones cortaría en 2026-06. El JOIN es
-- INNER a propósito: una fila sin deflactor sería un valor real nulo, y X-13
-- rechaza la serie entera si tiene huecos o nulos.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. Nominal + real, formato ancho
-- ---------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS public.agregados_pm_real CASCADE;

CREATE MATERIALIZED VIEW public.agregados_pm_real AS
WITH base AS (
    SELECT indice AS base FROM public.deflactores
    WHERE deflactor = 'ipc_largo' AND origen <> 'proyectado'
    ORDER BY fecha DESC LIMIT 1
)
SELECT
    a.date,
    a."BM"                          AS bm_nominal,
    a."Circulante"                  AS circulante_nominal,
    a."M1"                          AS m1_nominal,
    a."M2"                          AS m2_nominal,
    a."M3"                          AS m3_nominal,
    a."BM"         * b.base / d.indice AS bm_real,
    a."Circulante" * b.base / d.indice AS circulante_real,
    a."M1"         * b.base / d.indice AS m1_real,
    a."M2"         * b.base / d.indice AS m2_real,
    a."M3"         * b.base / d.indice AS m3_real,
    d.indice                        AS ipc,
    d.origen                        AS ipc_origen
FROM public."agregadosPrivadosPM" a
JOIN public.deflactores d ON d.fecha = a.date AND d.deflactor = 'ipc_largo'
CROSS JOIN base b
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS agregados_pm_real_date_uidx
    ON public.agregados_pm_real (date);

COMMENT ON MATERIALIZED VIEW public.agregados_pm_real IS
'Agregados monetarios del sector privado (BM, Circulante, M1, M2, M3), promedio mensual, nominal y a valores reales. Deriva de agregadosPrivadosPM y deflacta todo por el deflactor ipc_largo de public.deflactores (todos los agregados son en pesos: no hay pata en dolares como en prestamos). Base = ultimo mes PUBLICADO del deflactor, calculada dinamicamente: cuando entra un mes nuevo de IPC la serie real se reescala entera (los niveles cambian, las variaciones no). ipc_origen dice si el deflactor de cada mes es publicado o proyectado; los proyectados se revisan cuando INDEC publica. Escala: millones de pesos. REFRESH: lo hace serieseDownloaderPostgres.py al final de cada corrida, despues de agregadosPrivadosPM.';


-- ---------------------------------------------------------------------
-- 2. La misma serie en formato long: es la entrada de X-13
-- ---------------------------------------------------------------------
-- El núcleo de desestacionalización lee `select date, valor[, origen] from <vista>
-- where serie = %s order by date`, así que los nombres de columna (serie, date,
-- valor, origen) son parte del contrato y no se cambian.
CREATE OR REPLACE VIEW public.agregados_pm_series AS
SELECT 'bmReal'::text         AS serie, date, bm_real         AS valor, ipc_origen AS origen FROM public.agregados_pm_real
UNION ALL
SELECT 'circulanteReal'::text, date, circulante_real, ipc_origen FROM public.agregados_pm_real
UNION ALL
SELECT 'm1Real'::text,         date, m1_real,         ipc_origen FROM public.agregados_pm_real
UNION ALL
SELECT 'm2Real'::text,         date, m2_real,         ipc_origen FROM public.agregados_pm_real
UNION ALL
SELECT 'm3Real'::text,         date, m3_real,         ipc_origen FROM public.agregados_pm_real;

COMMENT ON VIEW public.agregados_pm_series IS
'agregados_pm_real en formato long (serie, date, valor, origen), entrada del desestacionalizador X-13. Series: bmReal, circulanteReal, m1Real, m2Real, m3Real. Los nombres de columna son el contrato que espera el nucleo de desest, no cambiarlos.';


-- =====================================================================
-- Checks de validación
-- =====================================================================

-- 1. Cobertura y contigüidad. Esperado con las proyecciones cargadas: 283 filas,
--    2003-01-01 -> 2026-07-01, 283 meses esperados, 0 nulos.
SELECT count(*)              AS filas,
       count(DISTINCT date)  AS fechas_unicas,
       min(date)             AS desde,
       max(date)             AS hasta,
       extract(year  FROM age(max(date), min(date))) * 12
     + extract(month FROM age(max(date), min(date))) + 1 AS meses_esperados,
       count(*) FILTER (WHERE bm_real IS NULL OR m3_real IS NULL) AS nulos
FROM public.agregados_pm_real;

-- 2. X-13 multiplicativo exige > 0 (0 en todas = OK).
SELECT count(*) FILTER (WHERE bm_real         <= 0) AS bm,
       count(*) FILTER (WHERE circulante_real <= 0) AS circulante,
       count(*) FILTER (WHERE m1_real         <= 0) AS m1,
       count(*) FILTER (WHERE m2_real         <= 0) AS m2,
       count(*) FILTER (WHERE m3_real         <= 0) AS m3
FROM public.agregados_pm_real;

-- 3. En el mes base, real == nominal (0 filas = OK).
SELECT r.date, r.m2_nominal, r.m2_real
FROM public.agregados_pm_real r
WHERE r.date = (SELECT max(fecha) FROM public.deflactores
                WHERE deflactor = 'ipc_largo' AND origen <> 'proyectado')
  AND abs(r.m2_real - r.m2_nominal) > 1e-6;

-- 4. El nominal tiene que coincidir con la matview de origen (0 filas = OK).
SELECT r.date
FROM public.agregados_pm_real r
JOIN public."agregadosPrivadosPM" a ON a.date = r.date
WHERE abs(r.bm_nominal - a."BM") > 1e-6
   OR abs(r.m3_nominal - a."M3") > 1e-6;

-- 5. La jerarquía se tiene que mantener después de deflactar (0 filas = OK):
--    deflactar es multiplicar por un escalar positivo, así que M1 <= M2 <= M3.
SELECT date, m1_real, m2_real, m3_real
FROM public.agregados_pm_real
WHERE m1_real > m2_real OR m2_real > m3_real;

-- 6. Procedencia del deflactor por mes.
SELECT ipc_origen, count(*) AS meses, min(date) AS desde, max(date) AS hasta
FROM public.agregados_pm_real
GROUP BY ipc_origen ORDER BY 3;

-- 7. La vista long: 5 series, 283 filas cada una, sin nulos.
SELECT serie, count(*) AS filas, min(date) AS desde, max(date) AS hasta,
       count(*) FILTER (WHERE valor IS NULL) AS nulos
FROM public.agregados_pm_series
GROUP BY serie ORDER BY serie;

-- 8. Nominal vs real, últimos meses de M2 y M3.
SELECT date,
       round(m2_nominal::numeric, 0) AS m2_nom,
       round(m2_real::numeric, 0)    AS m2_real,
       round(m3_nominal::numeric, 0) AS m3_nom,
       round(m3_real::numeric, 0)    AS m3_real,
       ipc_origen
FROM public.agregados_pm_real
ORDER BY date DESC LIMIT 6;
