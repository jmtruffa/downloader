-- =====================================================================
-- Préstamos al sector privado a valores reales, promedio mensual.
--
-- Paso 1 de dos. Este archivo deja la serie deflactada; el ajuste estacional
-- (Census X-13) lo hace prestamosDesest.sql + el módulo de Python, porque X-13
-- es un binario externo y no se puede calcular en SQL.
--
-- FUENTE: public.prestamos con tipoSerie = 'PM' (promedio mensual), 283 meses
-- desde 2003-01, fecha al primer día del mes, sin huecos.
--
-- DEFLACTORES, uno por moneda:
--   * pesos   -> public.ipc_largo (IPC empalmado base dic-2016 = 100). NO se usa
--     "IPCIndec" directo: arranca en dic-2016 y dejaría la serie en 115 meses
--     contra 282. De dic-2016 en adelante ipc_largo ES IPCIndec exacto, así que
--     no se pierde nada; hacia atrás arrastra el redondeo de inflaempalmada
--     (~0,2% por 2002), que está documentado en el COMMENT de ipc_largo.
--   * dólares -> public.uscpi_mensual (CPI-U del BLS, NSA, con oct-2025
--     interpolado y marcado). Deflactar dólares con el IPC argentino no
--     significa nada: son dos monedas y dos inflaciones distintas.
--
-- BASE: último mes disponible de CADA deflactor, calculada dinámicamente, igual
-- que el ejemplo de uso de ipc_largo.sql. O sea, pesos y dólares de <último mes>.
-- CONSECUENCIA A TENER PRESENTE: cuando entra un mes nuevo de IPC o de CPI, la
-- serie real entera se reescala. Los niveles cambian, las variaciones no. X-13
-- es invariante a escala, así que los factores estacionales tampoco cambian.
--
-- EL ÚLTIMO MES QUEDA AFUERA. Préstamos llega a 2026-07 y los dos deflactores a
-- 2026-06, así que la serie real corta un mes antes que la nominal. Es inevitable
-- (el IPC se publica a mes vencido) y por eso el JOIN es INNER: una fila con
-- deflactor nulo sería una fila con valor real nulo, y X-13 rechaza la serie
-- entera si tiene huecos.
--
-- ESCALAS de public.prestamos, que no son homogéneas:
--   prestamosSPPesosTotal            millones de pesos
--   prestamosSPDolaresTotal          millones de dólares
--   prestamosSPMillonesPesosDolares  los dólares convertidos a pesos al TC
--   prestamosSPPesosMasDolares       suma de pesos + dólares expresados en pesos
-- Se deflacta cada moneda con su propio índice. Las dos columnas mezcladas
-- (MillonesPesosDolares y PesosMasDolares) NO se deflactan acá: mezclan una
-- conversión por tipo de cambio con inflación local y merecen su propia
-- decisión, que todavía no tomamos.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. Nominal + real, formato ancho (para mirar y graficar)
-- ---------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS public.prestamos_pm_real CASCADE;

CREATE MATERIALIZED VIEW public.prestamos_pm_real AS
WITH base_pesos AS (
    SELECT indice AS base FROM public.ipc_largo ORDER BY fecha DESC LIMIT 1
),
base_dolares AS (
    SELECT indice AS base FROM public.uscpi_mensual ORDER BY fecha DESC LIMIT 1
)
SELECT
    p.date,
    p."prestamosSPPesosTotal"                          AS pesos_nominal,
    p."prestamosSPDolaresTotal"                        AS dolares_nominal,
    p."prestamosSPPesosTotal"   * bp.base / l.indice   AS pesos_real,
    p."prestamosSPDolaresTotal" * bd.base / u.indice   AS dolares_real,
    l.indice                                           AS ipc,
    u.indice                                           AS uscpi,
    u.interpolado                                      AS uscpi_interpolado,
    l.fuente                                           AS ipc_fuente
FROM public.prestamos p
JOIN public.ipc_largo     l  ON l.fecha = p.date
JOIN public.uscpi_mensual u  ON u.fecha = p.date
CROSS JOIN base_pesos   bp
CROSS JOIN base_dolares bd
WHERE p."tipoSerie" = 'PM'
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS prestamos_pm_real_date_uidx
    ON public.prestamos_pm_real (date);

COMMENT ON MATERIALIZED VIEW public.prestamos_pm_real IS
'Prestamos al sector privado, promedio mensual (prestamos.tipoSerie = PM), nominal y a valores reales. pesos_real deflactado por ipc_largo; dolares_real por uscpi_mensual (CPI-U del BLS, NSA). Base = ultimo mes disponible de cada deflactor, calculada dinamicamente: cuando entra un mes nuevo de IPC o CPI la serie real se reescala entera (los niveles cambian, las variaciones no). Escalas: pesos en millones de pesos, dolares en millones de dolares. Termina un mes antes que prestamos porque los deflactores van a mes vencido (JOIN inner a proposito: un hueco haria que X-13 saltee la serie). uscpi_interpolado marca los meses cuyo CPI estimamos nosotros (hoy solo oct-2025, que el BLS no publico). REFRESH: lo hace serieseDownloaderPostgres.py al final de cada corrida.';


-- ---------------------------------------------------------------------
-- 2. La misma serie en formato long: es la entrada de X-13
-- ---------------------------------------------------------------------
-- El núcleo de desestacionalización lee exactamente `select date, valor from
-- <vista> where serie = %s order by date`, así que los nombres de columna
-- (date, valor, serie) son parte del contrato y no se cambian.
CREATE OR REPLACE VIEW public.prestamos_pm_series AS
SELECT 'pesosReal'::text   AS serie, date, pesos_real   AS valor FROM public.prestamos_pm_real
UNION ALL
SELECT 'dolaresReal'::text AS serie, date, dolares_real AS valor FROM public.prestamos_pm_real;

COMMENT ON VIEW public.prestamos_pm_series IS
'prestamos_pm_real en formato long (serie, date, valor), entrada del desestacionalizador X-13. Series: pesosReal y dolaresReal. Los nombres de columna son el contrato que espera el nucleo de desest (select date, valor from <vista> where serie = %s), no cambiarlos.';


-- =====================================================================
-- Checks de validación
-- =====================================================================

-- 1. Cobertura y contigüidad. Esperado: 282 filas, 2003-01-01 -> 2026-06-01,
--    282 meses esperados, 0 nulos. Un mes menos que prestamos PM (283).
SELECT count(*)                    AS filas,
       count(DISTINCT date)        AS fechas_unicas,
       min(date)                   AS desde,
       max(date)                   AS hasta,
       extract(year  FROM age(max(date), min(date))) * 12
     + extract(month FROM age(max(date), min(date))) + 1 AS meses_esperados,
       count(*) FILTER (WHERE pesos_real IS NULL OR dolares_real IS NULL) AS nulos
FROM public.prestamos_pm_real;

-- 2. X-13 exige > 0 y al menos 36 meses (0 no positivos = OK).
SELECT count(*) FILTER (WHERE pesos_real   <= 0) AS pesos_no_positivos,
       count(*) FILTER (WHERE dolares_real <= 0) AS dolares_no_positivos
FROM public.prestamos_pm_real;

-- 3. En el mes base, real tiene que ser igual a nominal (0 filas = OK).
SELECT r.date, r.pesos_nominal, r.pesos_real, r.dolares_nominal, r.dolares_real
FROM public.prestamos_pm_real r
WHERE r.date = (SELECT max(fecha) FROM public.ipc_largo)
  AND (abs(r.pesos_real   - r.pesos_nominal)   > 1e-6
    OR abs(r.dolares_real - r.dolares_nominal) > 1e-6);

-- 4. El nominal tiene que coincidir con la tabla origen (0 filas = OK).
SELECT r.date
FROM public.prestamos_pm_real r
JOIN public.prestamos p ON p.date = r.date AND p."tipoSerie" = 'PM'
WHERE abs(r.pesos_nominal   - p."prestamosSPPesosTotal")   > 1e-6
   OR abs(r.dolares_nominal - p."prestamosSPDolaresTotal") > 1e-6;

-- 5. La vista long tiene que tener 2 series y el doble de filas.
SELECT serie, count(*) AS filas, min(date) AS desde, max(date) AS hasta,
       count(*) FILTER (WHERE valor IS NULL) AS nulos
FROM public.prestamos_pm_series
GROUP BY serie ORDER BY serie;

-- 6. Cuántos meses arrastran el CPI interpolado.
SELECT count(*) AS meses_con_uscpi_interpolado
FROM public.prestamos_pm_real WHERE uscpi_interpolado;

-- 7. Nominal vs real, últimas filas: en pesos el real tiene que crecer mucho
--    menos que el nominal (la inflación se fue), en dólares casi lo mismo.
SELECT date,
       round(pesos_nominal::numeric, 0)   AS pesos_nom,
       round(pesos_real::numeric, 0)      AS pesos_real,
       round(dolares_nominal::numeric, 0) AS usd_nom,
       round(dolares_real::numeric, 0)    AS usd_real
FROM public.prestamos_pm_real
ORDER BY date DESC LIMIT 6;
