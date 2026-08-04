-- =====================================================================
-- Índice de precios empalmado, base diciembre 2016 = 100.
--
--   * Desde dic-2016 en adelante: IPCIndec."nacionalNivelGeneral" sin
--     tocar, exacto.
--   * Antes de dic-2016: inflaempalmada.indice reescalado por el factor
--     que iguala ambas series en el mes de anclaje (dic-2016).
--
-- Cobertura: ene-1990 -> último mes de IPCIndec.
-- Mensual, `fecha` = día 1 del mes. Sin gaps ni duplicados.
--
-- El anclaje se calcula dinámicamente, no está hardcodeado: se extiende
-- solo cuando se carga un mes nuevo en IPCIndec y se reajusta si alguna
-- vez se corrige inflaempalmada.
--
-- Se crean DOS objetos:
--   1. public.v_ipc_largo  -> vista, tiene la lógica del empalme.
--   2. public.ipc_largo    -> matview, es un SELECT * de la anterior.
-- Así la lógica vive en un solo lugar y el REFRESH es trivial. Consumir
-- siempre `ipc_largo`; tocar `v_ipc_largo` sólo para cambiar el empalme.
--
-- Este archivo es idempotente: se puede volver a ejecutar entero sin
-- destruir ni duplicar nada.
--
-- IMPORTANTE para quien toque el ETL: la existencia de v_ipc_largo hace
-- que un DROP TABLE "IPCIndec" sin CASCADE falle. Por eso IPCDownload.py
-- graba con TRUNCATE + append y NO con to_sql(if_exists='replace').
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. La lógica
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_ipc_largo AS
WITH anclaje AS (
    -- Primer mes presente en ambas series (dic-2016).
    SELECT
        e.date                              AS fecha_ancla,
        e.indice::numeric                   AS infla_ancla,
        i."nacionalNivelGeneral"::numeric   AS ipc_ancla
    FROM public.inflaempalmada e
    JOIN public."IPCIndec"     i ON i.date = e.date
    ORDER BY e.date
    LIMIT 1
),
serie AS (
    -- Tramo largo reescalado (todo lo anterior al anclaje).
    SELECT
        e.date                                          AS fecha,
        e.indice::numeric * a.ipc_ancla / a.infla_ancla AS indice,
        'inflaempalmada'::text                          AS fuente
    FROM public.inflaempalmada e
    CROSS JOIN anclaje a
    WHERE e.date < a.fecha_ancla

    UNION ALL

    -- Tramo INDEC, exacto.
    SELECT
        i.date,
        i."nacionalNivelGeneral"::numeric,
        'IPCIndec'::text
    FROM public."IPCIndec" i
)
SELECT
    fecha,
    indice,
    indice / lag(indice)     OVER (ORDER BY fecha) - 1 AS var_mens,
    indice / lag(indice, 12) OVER (ORDER BY fecha) - 1 AS var_anual,
    fuente
FROM serie;


-- ---------------------------------------------------------------------
-- 2. El snapshot que consume todo el mundo
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS public.ipc_largo AS
SELECT * FROM public.v_ipc_largo
WITH DATA;

-- UNIQUE es obligatorio para poder refrescar CONCURRENTLY.
CREATE UNIQUE INDEX IF NOT EXISTS ipc_largo_fecha_uidx ON public.ipc_largo (fecha);

COMMENT ON MATERIALIZED VIEW public.ipc_largo IS
'IPC empalmado base dic-2016=100, mensual, ene-1990 en adelante. Desde dic-2016 es IPCIndec.nacionalNivelGeneral exacto; hacia atras es inflaempalmada reescalada por el nivel de dic-2016. var_mens y var_anual en tanto por uno, recalculadas desde el indice empalmado. REFRESH: lo hace automaticamente IPCDownload.py (refreshIPCLargo) al final de cada corrida del ETL del IPC, con REFRESH MATERIALIZED VIEW CONCURRENTLY. Solo hace falta refrescar a mano si se corrige inflaempalmada por fuera del ETL. PRECISION: inflaempalmada.indice es float4 y esta redondeado a entero hasta sep-2023, por lo que el tramo previo a dic-2016 arrastra error de redondeo: ~0,02% en 2016, ~0,24% en 2002, ~6% en 1990. No usar el tramo pre-1995 para inferir inflacion mensual.';


-- ---------------------------------------------------------------------
-- 3. Refresh inicial
-- ---------------------------------------------------------------------
-- El refresh de cada corrida lo hace el ETL (IPCDownload.py). Esta linea
-- solo sirve para dejar el matview al dia si se corrigio inflaempalmada
-- a mano. CONCURRENTLY no bloquea lectores (usa el indice unico de arriba).
REFRESH MATERIALIZED VIEW CONCURRENTLY public.ipc_largo;


-- =====================================================================
-- Checks de validación (SELECTs, no modifican nada)
-- =====================================================================

-- 1. Sin gaps ni duplicados: filas = fechas_unicas = meses_esperados.
SELECT count(*)              AS filas,
       count(DISTINCT fecha) AS fechas_unicas,
       min(fecha)            AS desde,
       max(fecha)            AS hasta,
       extract(year  FROM age(max(fecha), min(fecha))) * 12
     + extract(month FROM age(max(fecha), min(fecha))) + 1 AS meses_esperados
FROM public.ipc_largo;
-- Verificado 2026-08-04: 438 / 438 / 1990-01-01 / 2026-06-01 / 438

-- 2. El matview no quedó desactualizado contra la lógica (0 filas = OK).
SELECT 'falta en matview' AS problema, v.fecha, v.indice
FROM public.v_ipc_largo v
LEFT JOIN public.ipc_largo m ON m.fecha = v.fecha
WHERE m.fecha IS NULL OR m.indice <> v.indice;

-- 3. El tramo INDEC tiene que ser idéntico al original (0 filas = OK).
SELECT l.fecha, l.indice, i."nacionalNivelGeneral"
FROM public.ipc_largo l
JOIN public."IPCIndec" i ON i.date = l.fecha
WHERE l.indice <> i."nacionalNivelGeneral"::numeric;

-- 4. La juntura no debe inventar variación: var_mens de dic-2016 tiene que
--    coincidir con la varMens original de inflaempalmada (~0,011939).
SELECT l.fecha, l.var_mens, e."varMens" AS var_original, l.fuente
FROM public.ipc_largo l
JOIN public.inflaempalmada e ON e.date = l.fecha
WHERE l.fecha BETWEEN '2016-11-01' AND '2017-02-01'
ORDER BY l.fecha;

-- 5. Meses con variación mensual negativa.
--    OJO: este check NO da cero y no tiene por qué darlo. Devuelve 19 meses
--    entre 1995 y 2001, todos de ~-0,65%. Son dos cosas legítimas mezcladas:
--    (a) bajo convertibilidad hubo deflación mensual real, y (b) el redondeo
--    a entero de inflaempalmada.indice genera saltos de +-1/indice (~0,65%
--    con indice ~154). No es un defecto del empalme. Sirve como control de
--    magnitud: si aparece un mes negativo fuerte o posterior a dic-2016,
--    ahi si hay algo para mirar.
SELECT fecha, var_mens FROM public.ipc_largo WHERE var_mens < 0 ORDER BY fecha;


-- =====================================================================
-- Uso: deflactar préstamos en pesos, base = último mes con IPC
-- =====================================================================
WITH base AS (
    SELECT indice AS ipc_base
    FROM public.ipc_largo
    ORDER BY fecha DESC
    LIMIT 1
)
SELECT
    p.date,
    p."prestamosSPPesosTotal"                         AS pesos_nominal,
    p."prestamosSPPesosTotal" * b.ipc_base / l.indice AS pesos_real,
    l.fuente                                          AS fuente_deflactor
FROM public.prestamos p
LEFT JOIN public.ipc_largo l ON l.fecha = p.date
CROSS JOIN base b
WHERE p."tipoSerie" = 'PM'
ORDER BY p.date;
