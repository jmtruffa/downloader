-- =====================================================================
-- Préstamos reales desestacionalizados (Census X-13ARIMA-SEATS).
--
-- Paso 2 de dos. prestamosReal.sql deja la serie deflactada; acá va el destino
-- de la serie ajustada.
--
-- POR QUÉ ESTO ES UNA TABLA Y NO UNA MATVIEW
-- X-13 es un binario externo (x13as): SQL no desestacionaliza. El ajuste lo
-- calcula seasonalDesest.py y lo escribe acá por UPSERT. Sobre esta tabla sí
-- hay vistas para consumir.
--
-- FORMATO LONG, siguiendo el patrón del monorepo de ETLs
-- (/home/jmt/dev/downloader, etl/core/seasonal.py + etl/datasets/*/schema.sql):
-- una fila por (serie, mes) con estado, fuente y los parámetros de la corrida.
-- `parametros` guarda el modo, el trading-day, el filtro estacional y el modelo
-- ARIMA que eligió automdl, para poder auditar después por qué un valor cambió.
--
-- Hoy la tabla sólo contiene filas con estado = 'desestacionalizado' (la serie
-- observada vive en la matview prestamos_pm_real). Se mantiene igual la columna
-- `estado` y el índice único PARCIAL para que el UPSERT sea el mismo del patrón
-- y para poder sumar otros estados más adelante sin migrar nada.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. Destino del ajuste
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.prestamos_desest (
    id          bigint generated always as identity primary key,
    serie       text   not null,                 -- pesosReal / dolaresReal
    date        date   not null,                 -- primer día del mes
    valor       double precision,                -- millones, misma escala que el origen
    estado      text,                            -- 'desestacionalizado'
    fuente      text,                            -- 'census x13'
    parametros  jsonb,                           -- parámetros de la corrida X-13
    ingested_at timestamptz not null default now()
);

-- Búsqueda del último snapshot de un (serie, date, estado).
CREATE INDEX IF NOT EXISTS prestamos_desest_serie_date_estado_idx
    ON public.prestamos_desest (serie, date, estado, ingested_at DESC);

-- Una sola fila desestacionalizada por (serie, mes): es el target del UPSERT.
CREATE UNIQUE INDEX IF NOT EXISTS prestamos_desest_desest_uq
    ON public.prestamos_desest (serie, date)
    WHERE estado = 'desestacionalizado';

COMMENT ON TABLE public.prestamos_desest IS
'Prestamos al sector privado a valores reales y desestacionalizados con Census X-13ARIMA-SEATS (tabla d11 del X-11). Formato long: una fila por (serie, mes), series pesosReal y dolaresReal. Lo escribe seasonalDesest.py por UPSERT al final del ETL; la serie observada de entrada es la matview prestamos_pm_real. La columna parametros guarda modo, trading-day, filtro estacional y el modelo ARIMA elegido por automdl. ATENCION: los parametros de X-13 son un default razonado, NO estan calibrados contra una referencia externa como los del monorepo de ETLs.';


-- ---------------------------------------------------------------------
-- 2. Un valor por (serie, mes)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW public.prestamos_desest_actual AS
SELECT DISTINCT ON (serie, date)
       serie, date, valor, fuente, parametros, ingested_at
FROM public.prestamos_desest
WHERE estado = 'desestacionalizado'
ORDER BY serie, date, ingested_at DESC;


-- ---------------------------------------------------------------------
-- 3. La vista para consumir: nominal, real y real desestacionalizado
-- ---------------------------------------------------------------------
-- pm_hasta / pm_definitivo: hasta qué día del mes está calculado el promedio
-- mensual que publica el BCRA. Verificado: el PM es el promedio de DÍAS CORRIDOS
-- desde el 1 hasta el último día presente en la serie diaria, arrastrando el
-- último saldo en fines de semana y feriados. Para marzo a junio de 2026 el
-- cálculo reproduce el PM publicado con 0,00000% de diferencia.
--
-- Va POR FILA y no como un valor global: para un mes cerrado el corte es su
-- propio último día y el PM es definitivo; sólo el último mes puede estar
-- incompleto. Poner el corte global en todas las filas diría que marzo es
-- provisorio, que es falso.
--
-- Importa porque el último punto de las series reales y desestacionalizadas se
-- mueve por DOS motivos independientes: la inflación proyectada (ipc_origen /
-- uscpi_origen) y el PM provisorio (esto). Hoy julio-2026 tiene los dos.
CREATE OR REPLACE VIEW public.prestamos_pm_completo AS
WITH corte AS (
    SELECT max(date) AS hasta FROM public.prestamos WHERE "tipoSerie" = 'D'
)
SELECT
    r.date,
    r.pesos_nominal,
    r.pesos_real,
    dp.valor                AS pesos_real_desest,
    r.dolares_nominal,
    r.dolares_real,
    dd.valor                AS dolares_real_desest,
    r.ipc,
    r.ipc_origen,
    r.uscpi,
    r.uscpi_origen,
    least((date_trunc('month', r.date) + interval '1 month -1 day')::date,
          c.hasta)                                                   AS pm_hasta,
    ((date_trunc('month', r.date) + interval '1 month -1 day')::date
        <= c.hasta)                                                  AS pm_definitivo
FROM public.prestamos_pm_real r
LEFT JOIN public.prestamos_desest_actual dp
       ON dp.date = r.date AND dp.serie = 'pesosReal'
LEFT JOIN public.prestamos_desest_actual dd
       ON dd.date = r.date AND dd.serie = 'dolaresReal'
CROSS JOIN corte c;

COMMENT ON VIEW public.prestamos_pm_completo IS
'Vista de consumo de prestamos al sector privado, promedio mensual: nominal, real y real desestacionalizado, en pesos y en dolares. LEFT JOIN a proposito: si X-13 todavia no corrio, o salteo una serie, las columnas *_desest vienen en NULL y el resto sigue sirviendo. Termina un mes antes que public.prestamos porque los deflactores van a mes vencido.';


-- =====================================================================
-- Checks de validación (correr DESPUÉS de una corrida del ETL)
-- =====================================================================

-- 1. Qué series se ajustaron, con qué cobertura.
SELECT serie, count(*) AS meses, min(date) AS desde, max(date) AS hasta,
       max(ingested_at) AS ultima_corrida
FROM public.prestamos_desest_actual
GROUP BY serie ORDER BY serie;

-- 2. Parámetros de la última corrida de cada serie (auditoría).
SELECT DISTINCT ON (serie) serie, parametros, ingested_at
FROM public.prestamos_desest_actual
ORDER BY serie, ingested_at DESC;

-- 3. Ninguna fila desest sin su observada (0 filas = OK).
SELECT d.serie, d.date
FROM public.prestamos_desest_actual d
LEFT JOIN public.prestamos_pm_real r ON r.date = d.date
WHERE r.date IS NULL;

-- 4. Cordura del ajuste: la desestacionalizada tiene que seguir a la observada
--    de cerca. Un desvío grande es señal de que el ajuste salió mal.
SELECT serie,
       round((avg(abs(ratio - 1)) * 100)::numeric, 3) AS desvio_medio_pct,
       round((max(abs(ratio - 1)) * 100)::numeric, 3) AS desvio_max_pct
FROM (
    SELECT d.serie, d.valor / NULLIF(
               CASE d.serie WHEN 'pesosReal' THEN r.pesos_real
                            WHEN 'dolaresReal' THEN r.dolares_real END, 0) AS ratio
    FROM public.prestamos_desest_actual d
    JOIN public.prestamos_pm_real r ON r.date = d.date
) t
GROUP BY serie ORDER BY serie;

-- 5. El ajuste no debe cambiar el nivel promedio de la serie: el promedio anual
--    de la desestacionalizada tiene que ser muy parecido al de la observada.
SELECT extract(year FROM r.date)::int AS anio,
       round(avg(r.pesos_real)::numeric, 0)  AS real_prom,
       round(avg(d.valor)::numeric, 0)       AS desest_prom,
       round(((avg(d.valor) / avg(r.pesos_real) - 1) * 100)::numeric, 3) AS dif_pct
FROM public.prestamos_pm_real r
JOIN public.prestamos_desest_actual d ON d.date = r.date AND d.serie = 'pesosReal'
GROUP BY 1 ORDER BY 1 DESC LIMIT 5;

-- 6. Últimas filas de la vista de consumo.
SELECT date,
       round(pesos_real::numeric, 0)        AS pesos_real,
       round(pesos_real_desest::numeric, 0) AS pesos_desest,
       round(dolares_real::numeric, 0)      AS usd_real,
       round(dolares_real_desest::numeric, 0) AS usd_desest
FROM public.prestamos_pm_completo
ORDER BY date DESC LIMIT 6;
