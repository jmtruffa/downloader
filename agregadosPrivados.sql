-- =====================================================================
-- Agregados monetarios del sector privado, base diaria.
--
-- Reemplaza tres objetos por uno solo:
--   * procedure public.agregadosprivados()  -> borrado
--   * tabla     public."agregadosPrivados"  -> pasa a ser matview
--   * tablas    public."varAgregados" y public."varAnualAgregadosPrivados"
--               -> borradas, sus variaciones viven acá adentro
--
-- POR QUÉ MATVIEW Y NO TABLA
-- No es por el índice: una tabla común también acepta índices. El problema era
-- que el procedure hacía `drop table` + `create table as` en cada corrida, y
-- eso se lleva cualquier índice y deja a los lectores con "relation does not
-- exist" mientras corre. REFRESH MATERIALIZED VIEW CONCURRENTLY conserva los
-- índices y no bloquea lectores. Y la serie es 100% derivada de depositos y
-- bmBCRA, que es exactamente lo que una matview modela.
--
-- AJUSTE EN M1 (cambio de definición, no un refactor)
-- El procedure calculaba M1 = circulante + caja de ahorro, y metía cuenta
-- corriente en M2. Queda invertido respecto de la convención: M1 son los
-- medios de pago a la vista, o sea circulante + cuenta corriente, y la caja de
-- ahorro suma en M2. M2 y M3 no cambian de valor (son la misma suma); el único
-- que cambia es M1.
--
-- VARIACIONES ANUALES
-- Se toma el último día disponible en o antes de un año atrás, no la fecha
-- exacta. El cálculo anterior (en pandas) exigía la fecha exacta y perdía la
-- fila entera cuando un año atrás caía domingo o feriado: se iban 1728 de 5777
-- fechas, el 30% de la serie. Con el LATERAL quedan 5528 con variación; las 249
-- restantes son el primer año, que correctamente no tiene referencia.
--
-- REFRESH: lo hace serieseDownloaderPostgres.py al final de cada corrida
-- (cron 25 18 * * 1-5), después de cargar depositos y bmBCRA.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. Fuera lo viejo
-- ---------------------------------------------------------------------
-- Las tres son 100% derivadas de depositos + bmBCRA: no se pierde nada que la
-- corrida siguiente no reconstruya. Verificado el 2026-08-04 con pg_depend:
-- ninguna vista ni matview cuelga de ellas.
DROP TABLE     IF EXISTS public."varAgregados";
DROP TABLE     IF EXISTS public."varAnualAgregadosPrivados";
DROP TABLE     IF EXISTS public."agregadosPrivados";
DROP PROCEDURE IF EXISTS public.agregadosprivados();


-- ---------------------------------------------------------------------
-- 2. El objeto nuevo
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW public."agregadosPrivados" AS
WITH base AS MATERIALIZED (
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
    WHERE a."tipoSerie" = 'D'
      AND b."tipoSerie" = 'D'
)
SELECT
    b.date,
    b."BM",
    b."Circulante",
    b."M1",
    b."M2",
    b."M3",
    -- NULLIF evita la división por cero si alguna serie arranca en 0.
    b."BM"         / NULLIF(prev."BM", 0)         - 1 AS "varAnualBM",
    b."Circulante" / NULLIF(prev."Circulante", 0) - 1 AS "varAnualCirculante",
    b."M1"         / NULLIF(prev."M1", 0)         - 1 AS "varAnualM1",
    b."M2"         / NULLIF(prev."M2", 0)         - 1 AS "varAnualM2",
    b."M3"         / NULLIF(prev."M3", 0)         - 1 AS "varAnualM3",
    prev.date                                        AS "fechaBaseVarAnual"
FROM base b
LEFT JOIN LATERAL (
    SELECT p.date, p."BM", p."Circulante", p."M1", p."M2", p."M3"
    FROM base p
    WHERE p.date <= b.date - interval '1 year'
    ORDER BY p.date DESC
    LIMIT 1
) prev ON true
WITH DATA;

-- UNIQUE es obligatorio para poder refrescar CONCURRENTLY.
CREATE UNIQUE INDEX IF NOT EXISTS "agregadosPrivados_date_uidx"
    ON public."agregadosPrivados" (date);

COMMENT ON MATERIALIZED VIEW public."agregadosPrivados" IS
'Agregados monetarios del sector privado, serie diaria, derivada de depositos y bmBCRA (tipoSerie = D). BM = sdVBMTotal. Circulante = sdBMCMBilletesPublico. M1 = circulante + cuenta corriente. M2 = M1 + caja de ahorro. M3 = M2 + plazo fijo no ajustado + plazo fijo CER/UVA + otros. Todo sector privado (columnas pSPPesos*), por eso NO coincide con el M2 publicado por el BCRA, que es total. varAnual* en tanto por uno contra el ultimo dia disponible en o antes de un anio atras; fechaBaseVarAnual dice cual se uso. Las primeras 249 fechas de la serie no tienen varAnual porque no hay referencia. REFRESH: lo hace serieseDownloaderPostgres.py al final de cada corrida con REFRESH MATERIALIZED VIEW CONCURRENTLY. HISTORIA: reemplaza al procedure agregadosprivados() y a las tablas varAgregados / varAnualAgregadosPrivados (identicas entre si), y corrige M1, que antes era circulante + caja de ahorro.';


-- =====================================================================
-- Checks de validación
-- =====================================================================

-- 1. Cobertura y variaciones. Esperado: 5777 filas, 5777 fechas unicas,
--    5528 con varAnualM3, 249 sin.
SELECT count(*)                            AS filas,
       count(DISTINCT date)                AS fechas_unicas,
       count("varAnualM3")                 AS con_var_anual,
       count(*) - count("varAnualM3")      AS sin_var_anual,
       min(date)                           AS desde,
       max(date)                           AS hasta
FROM public."agregadosPrivados";

-- 2. M2 y M3 no debieron cambiar de valor respecto de la definición anterior
--    (el ajuste fue sólo en M1). Se recalcula desde las fuentes (0 filas = OK).
SELECT a.date
FROM public."agregadosPrivados" a
JOIN public.depositos d ON d.date = a.date AND d."tipoSerie" = 'D'
JOIN public."bmBCRA"  b ON b.date = a.date AND b."tipoSerie" = 'D'
WHERE abs(a."M2" - (COALESCE(b."sdBMCMBilletesPublico",0) + COALESCE(d."pSPPesosCA",0)
                  + COALESCE(d."pSPPesosCtaCte",0))) > 1e-6;

-- 3. M1 ahora tiene que ser circulante + cuenta corriente (0 filas = OK).
SELECT a.date, a."M1"
FROM public."agregadosPrivados" a
JOIN public.depositos d ON d.date = a.date AND d."tipoSerie" = 'D'
JOIN public."bmBCRA"  b ON b.date = a.date AND b."tipoSerie" = 'D'
WHERE abs(a."M1" - (COALESCE(b."sdBMCMBilletesPublico",0)
                  + COALESCE(d."pSPPesosCtaCte",0))) > 1e-6;

-- 4. Jerarquía: M1 <= M2 <= M3 siempre (0 filas = OK).
SELECT date, "M1", "M2", "M3"
FROM public."agregadosPrivados"
WHERE "M1" > "M2" OR "M2" > "M3";

-- 5. La fecha base nunca puede ser posterior a un año atrás (0 filas = OK).
SELECT date, "fechaBaseVarAnual"
FROM public."agregadosPrivados"
WHERE "fechaBaseVarAnual" > date - interval '1 year';

-- 6. Últimas filas, para mirar magnitudes.
SELECT date,
       round("M1"::numeric, 0)          AS "M1",
       round("M2"::numeric, 0)          AS "M2",
       round("M3"::numeric, 0)          AS "M3",
       round("varAnualM2"::numeric, 4)  AS "varAnualM2",
       round("varAnualM3"::numeric, 4)  AS "varAnualM3",
       "fechaBaseVarAnual"
FROM public."agregadosPrivados"
ORDER BY date DESC
LIMIT 5;
