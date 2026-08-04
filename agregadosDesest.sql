-- =====================================================================
-- Agregados monetarios reales desestacionalizados (Census X-13ARIMA-SEATS).
--
-- Paso 2 de dos, gemelo de prestamosDesest.sql. agregadosReal.sql deja la serie
-- deflactada; acá va el destino de la serie ajustada.
--
-- POR QUÉ ESTO ES UNA TABLA Y NO UNA MATVIEW
-- X-13 es un binario externo (x13as): SQL no desestacionaliza. El ajuste lo
-- calcula seasonalDesest.py y lo escribe acá por UPSERT. Sobre esta tabla sí hay
-- vistas para consumir.
--
-- FORMATO LONG, siguiendo el patrón del monorepo de ETLs
-- (/home/jmt/dev/downloader, etl/core/seasonal.py + etl/datasets/*/schema.sql):
-- una fila por (serie, mes) con estado, fuente y los parámetros de la corrida.
-- `parametros` guarda modo, trading-day, filtro estacional, el modelo ARIMA que
-- eligió automdl y el conteo por procedencia del deflactor, para poder auditar
-- después por qué un valor cambió.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. Destino del ajuste
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.agregados_desest (
    id          bigint generated always as identity primary key,
    serie       text   not null,                 -- bmReal / circulanteReal / m1Real / m2Real / m3Real
    date        date   not null,                 -- primer día del mes
    valor       double precision,                -- millones de pesos, misma escala que el origen
    estado      text,                            -- 'desestacionalizado'
    fuente      text,                            -- 'census x13'
    parametros  jsonb,                           -- parámetros de la corrida X-13
    ingested_at timestamptz not null default now()
);

CREATE INDEX IF NOT EXISTS agregados_desest_serie_date_estado_idx
    ON public.agregados_desest (serie, date, estado, ingested_at DESC);

-- Una sola fila desestacionalizada por (serie, mes): es el target del UPSERT.
CREATE UNIQUE INDEX IF NOT EXISTS agregados_desest_desest_uq
    ON public.agregados_desest (serie, date)
    WHERE estado = 'desestacionalizado';

COMMENT ON TABLE public.agregados_desest IS
'Agregados monetarios del sector privado a valores reales y desestacionalizados con Census X-13ARIMA-SEATS (tabla d11 del X-11). Formato long: una fila por (serie, mes), series bmReal, circulanteReal, m1Real, m2Real y m3Real. Lo escribe seasonalDesest.py por UPSERT al final del ETL; la serie observada de entrada es la matview agregados_pm_real. La columna parametros guarda modo, trading-day, filtro estacional, el modelo ARIMA elegido por automdl y el conteo por procedencia del deflactor. ATENCION: los parametros de X-13 son un default razonado, NO estan calibrados contra una referencia externa como los del monorepo de ETLs.';


-- ---------------------------------------------------------------------
-- 2. Un valor por (serie, mes)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW public.agregados_desest_actual AS
SELECT DISTINCT ON (serie, date)
       serie, date, valor, fuente, parametros, ingested_at
FROM public.agregados_desest
WHERE estado = 'desestacionalizado'
ORDER BY serie, date, ingested_at DESC;


-- ---------------------------------------------------------------------
-- 3. La vista para consumir: nominal, real y real desestacionalizado
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW public.agregados_pm_completo AS
SELECT
    r.date,
    r.bm_nominal,         r.bm_real,         dbm.valor  AS bm_real_desest,
    r.circulante_nominal, r.circulante_real, dc.valor   AS circulante_real_desest,
    r.m1_nominal,         r.m1_real,         d1.valor   AS m1_real_desest,
    r.m2_nominal,         r.m2_real,         d2.valor   AS m2_real_desest,
    r.m3_nominal,         r.m3_real,         d3.valor   AS m3_real_desest,
    r.ipc,
    r.ipc_origen
FROM public.agregados_pm_real r
LEFT JOIN public.agregados_desest_actual dbm ON dbm.date = r.date AND dbm.serie = 'bmReal'
LEFT JOIN public.agregados_desest_actual dc  ON dc.date  = r.date AND dc.serie  = 'circulanteReal'
LEFT JOIN public.agregados_desest_actual d1  ON d1.date  = r.date AND d1.serie  = 'm1Real'
LEFT JOIN public.agregados_desest_actual d2  ON d2.date  = r.date AND d2.serie  = 'm2Real'
LEFT JOIN public.agregados_desest_actual d3  ON d3.date  = r.date AND d3.serie  = 'm3Real';

COMMENT ON VIEW public.agregados_pm_completo IS
'Vista de consumo de los agregados monetarios privados, promedio mensual: nominal, real y real desestacionalizado, para BM, Circulante, M1, M2 y M3. LEFT JOIN a proposito: si X-13 todavia no corrio, o salteo una serie, las columnas *_desest vienen en NULL y el resto sigue sirviendo. ipc_origen dice si el deflactor del mes es publicado o proyectado.';


-- =====================================================================
-- Checks de validación (correr DESPUÉS de una corrida del ETL)
-- =====================================================================

-- 1. Qué series se ajustaron, con qué cobertura. Esperado: las 5 con 283 meses.
SELECT serie, count(*) AS meses, min(date) AS desde, max(date) AS hasta,
       max(ingested_at) AS ultima_corrida
FROM public.agregados_desest_actual
GROUP BY serie ORDER BY serie;

-- 2. Parámetros de la última corrida de cada serie (auditoría).
SELECT DISTINCT ON (serie) serie,
       parametros->>'arima'          AS arima,
       parametros->>'modo'           AS modo,
       parametros->'origen_conteo'   AS origen_conteo
FROM public.agregados_desest_actual
ORDER BY serie, ingested_at DESC;

-- 3. Ninguna fila desest sin su observada (0 filas = OK).
SELECT d.serie, d.date
FROM public.agregados_desest_actual d
LEFT JOIN public.agregados_pm_real r ON r.date = d.date
WHERE r.date IS NULL;

-- 4. Cordura: la desestacionalizada tiene que seguir de cerca a la observada.
--    Un desvío grande es señal de que el ajuste salió mal.
SELECT serie,
       round((avg(abs(ratio - 1)) * 100)::numeric, 3) AS desvio_medio_pct,
       round((max(abs(ratio - 1)) * 100)::numeric, 3) AS desvio_max_pct
FROM (
    SELECT d.serie, d.valor / NULLIF(
        CASE d.serie WHEN 'bmReal'         THEN r.bm_real
                     WHEN 'circulanteReal' THEN r.circulante_real
                     WHEN 'm1Real'         THEN r.m1_real
                     WHEN 'm2Real'         THEN r.m2_real
                     WHEN 'm3Real'         THEN r.m3_real END, 0) AS ratio
    FROM public.agregados_desest_actual d
    JOIN public.agregados_pm_real r ON r.date = d.date
) t
GROUP BY serie ORDER BY serie;

-- 5. El ajuste no debe cambiar el nivel promedio: el promedio anual de la
--    desestacionalizada tiene que ser muy parecido al de la observada. El año en
--    curso puede desviarse porque es parcial (meses estacionalmente sesgados).
SELECT extract(year FROM r.date)::int AS anio,
       round(avg(r.m2_real)::numeric, 0) AS m2_real_prom,
       round(avg(d.valor)::numeric, 0)   AS m2_desest_prom,
       round(((avg(d.valor) / avg(r.m2_real) - 1) * 100)::numeric, 3) AS dif_pct
FROM public.agregados_pm_real r
JOIN public.agregados_desest_actual d ON d.date = r.date AND d.serie = 'm2Real'
GROUP BY 1 ORDER BY 1 DESC LIMIT 5;

-- 6. La jerarquía tiene que sobrevivir al ajuste. OJO: acá SÍ puede fallar y no
--    seria un bug: X-13 ajusta cada serie por separado, con su propio modelo
--    ARIMA y sus propios factores, asi que nada garantiza M1 <= M2 <= M3 en la
--    serie ajustada. Si aparecen violaciones, conviene mirar su magnitud: unas
--    pocas y chicas son ruido del ajuste; muchas o grandes indican que alguna
--    serie se ajusto mal.
SELECT count(*) AS violaciones_jerarquia,
       round((max(greatest(m1 - m2, m2 - m3)) )::numeric, 0) AS peor_exceso_millones
FROM (
    SELECT date,
           max(valor) FILTER (WHERE serie = 'm1Real') AS m1,
           max(valor) FILTER (WHERE serie = 'm2Real') AS m2,
           max(valor) FILTER (WHERE serie = 'm3Real') AS m3
    FROM public.agregados_desest_actual GROUP BY date
) t
WHERE m1 > m2 OR m2 > m3;

-- 7. Últimas filas de la vista de consumo.
SELECT date,
       round(m2_real::numeric, 0)        AS m2_real,
       round(m2_real_desest::numeric, 0) AS m2_desest,
       round(m3_real::numeric, 0)        AS m3_real,
       round(m3_real_desest::numeric, 0) AS m3_desest,
       ipc_origen
FROM public.agregados_pm_completo
ORDER BY date DESC LIMIT 6;
