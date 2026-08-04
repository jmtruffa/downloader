-- =====================================================================
-- Proyecciones de inflación, para poder deflactar meses que todavía no se
-- publicaron.
--
-- EL PROBLEMA
-- Préstamos (y cualquier serie del BCRA) llega hasta el mes en curso, pero el
-- IPC del INDEC y el CPI del BLS salen a mes vencido. Sin proyección, el último
-- mes no se puede deflactar, y como X-13 rechaza series con huecos, tampoco se
-- desestacionaliza. Hoy la serie real corta un mes antes que la nominal.
--
-- POR QUÉ VARIACIÓN Y NO NIVEL
-- Se guarda la variación mensual, no el índice proyectado. Con el nivel, cada
-- publicación real obligaría a recalcular a mano todos los niveles proyectados
-- que quedan. Con la variación, el nivel se deriva encadenando sobre el último
-- dato publicado y se reancla solo. Además es el formato en que vienen las
-- proyecciones reales: el REM del BCRA publica inflación mensual esperada, no
-- niveles (y ya hay un rem_downloader en el cron para automatizar esto).
--
-- APPEND-ONLY
-- Corregir una proyección es INSERTAR una fila nueva, no updatear. La vista
-- inflacion_proyectada_actual se queda con la más reciente por (deflactor, mes).
-- Así queda registrado qué se proyectaba para un mes y desde cuándo, que es lo
-- que después explica por qué un valor real cambió. Mismo patrón que
-- prestamos_desest y que el monorepo de ETLs.
--
-- LAS SERIES PUBLICADAS NO SE TOCAN
-- ipc_largo y uscpi_mensual siguen conteniendo sólo lo publicado. El empalme con
-- las proyecciones vive en la vista `deflactores`, que es la que consumen las
-- series deflactadas. Nada que lea ipc_largo directo cambia de comportamiento.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. La tabla
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.inflacion_proyectada (
    id          bigint generated always as identity primary key,
    -- Nombra la serie publicada que esta proyección extiende. Autodocumentado:
    -- se lee la fila y se sabe a qué se engancha.
    deflactor   text not null
                check (deflactor in ('ipc_largo', 'uscpi_mensual')),
    fecha       date not null
                check (extract(day from fecha) = 1),   -- primer día del mes
    -- Tanto por uno: 0.019 = 1,9% mensual. > -1 porque el encadenado usa
    -- ln(1 + var_mens) y un índice no puede llegar a cero.
    var_mens    double precision not null
                check (var_mens > -1),
    fuente      text,                                   -- 'REM BCRA 2026-07', 'estimacion propia', ...
    -- Quién cargó el valor. NOT NULL y SIN DEFAULT a propósito: todas las
    -- conexiones a esta base usan el único rol `postgres`, así que un
    -- `default current_user` pondría 'postgres' en todas las filas y daría una
    -- falsa sensación de trazabilidad. Postgres no puede ver el usuario del
    -- sistema operativo; el que lo sabe es la terminal. Obligar a declararlo es
    -- lo único que hace que el campo sirva. Complementa a `fuente`: fuente dice
    -- de dónde salió el número, usuario dice quién lo cargó.
    usuario     text not null check (usuario <> ''),
    nota        text,
    ingested_at timestamptz not null default now()
);

-- Upgrade para bases que ya tengan la tabla sin esta columna. Si la tabla tuviera
-- filas, este ALTER falla por el NOT NULL: en ese caso hay que agregarla nullable,
-- backfillear y después poner el NOT NULL.
ALTER TABLE public.inflacion_proyectada
    ADD COLUMN IF NOT EXISTS usuario text not null check (usuario <> '');

CREATE INDEX IF NOT EXISTS inflacion_proyectada_lookup_idx
    ON public.inflacion_proyectada (deflactor, fecha, ingested_at DESC);

COMMENT ON TABLE public.inflacion_proyectada IS
'Proyecciones de inflacion mensual para deflactar meses todavia no publicados. Se guarda la VARIACION mensual (var_mens, tanto por uno) y no el nivel del indice: el nivel se deriva encadenando sobre el ultimo mes publicado de la serie que indica `deflactor`, asi se reancla solo cuando INDEC o el BLS publican. APPEND-ONLY: para corregir una proyeccion se inserta una fila nueva, nunca se updatea; la vista inflacion_proyectada_actual se queda con la mas reciente por (deflactor, mes). `usuario` es obligatorio y sin default: todas las conexiones usan el rol postgres, asi que un default current_user pondria postgres en todo y no serviria de nada; pasarlo desde la shell con psql -v quien="$(id -un)" y despues :''quien'' en el INSERT ($(id -un) y no $USER, que es una variable de entorno y se puede exportar a cualquier valor). El empalme publicado + proyectado vive en la vista public.deflactores; ipc_largo y uscpi_mensual siguen conteniendo solo datos publicados.';


-- ---------------------------------------------------------------------
-- 2. La proyección vigente de cada mes
-- ---------------------------------------------------------------------
-- El desempate por `id DESC` NO es decorativo: now() en Postgres es hora de
-- TRANSACCIÓN, así que dos filas del mismo (deflactor, fecha) insertadas en la
-- misma transacción —una carga en batch que incluye una corrección, por ejemplo—
-- quedan con el mismo ingested_at y el DISTINCT ON elegiría una arbitrariamente.
-- `id` es identity y siempre crece, así que gana la última insertada, que es la
-- definición de "vigente" en una tabla append-only.
-- `usuario` va AL FINAL, no al lado de `fuente` donde quedaría más prolijo:
-- CREATE OR REPLACE VIEW sólo permite AGREGAR columnas al final, y meterla en el
-- medio se interpreta como renombrar una existente. Reordenar obligaría a dropear
-- la vista, y de ella cuelgan deflactores y, más abajo, la matview
-- prestamos_pm_real: un CASCADE se llevaría media cadena. No "acomodar" el orden.
CREATE OR REPLACE VIEW public.inflacion_proyectada_actual AS
SELECT DISTINCT ON (deflactor, fecha)
       deflactor, fecha, var_mens, fuente, nota, ingested_at, id, usuario
FROM public.inflacion_proyectada
ORDER BY deflactor, fecha, ingested_at DESC, id DESC;


-- ---------------------------------------------------------------------
-- 3. El deflactor efectivo: publicado + proyectado encadenado
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW public.deflactores AS
-- `indice` es double precision en toda la vista, a propósito y explícito en cada
-- rama. Motivo: el nivel proyectado sale de exp(sum(ln(...))), que devuelve
-- double, y CREATE OR REPLACE VIEW no puede cambiarle el tipo a una columna
-- existente. Si se forzara numeric habría que dropear la vista en cada cambio, y
-- eso falla en cuanto prestamos_pm_real dependa de ella. Además el valor
-- proyectado es aproximado por construcción, así que double no miente.
-- Para redondear en queries ad-hoc: round(indice::numeric, 4).
WITH publicado AS (
    SELECT 'ipc_largo'::text AS deflactor, fecha,
           indice::double precision AS indice,
           'publicado'::text AS origen
    FROM public.ipc_largo
    UNION ALL
    SELECT 'uscpi_mensual'::text, fecha, indice::double precision,
           CASE WHEN interpolado THEN 'interpolado' ELSE 'publicado' END
    FROM public.uscpi_mensual
),
-- Último mes publicado de cada deflactor: el ancla del encadenado.
ancla AS (
    SELECT DISTINCT ON (deflactor) deflactor, fecha, indice
    FROM publicado
    ORDER BY deflactor, fecha DESC
),
-- Sólo la corrida CONTIGUA de proyecciones que arranca justo después del ancla.
-- Si falta un mes en el medio, el producto acumulado compondría la variación de
-- un mes sobre un índice que no le corresponde. Exigir que el número de fila
-- coincida con la distancia en meses al ancla corta la serie en el hueco, así se
-- ignora el resto en lugar de devolver un número mal.
contiguo AS (
    SELECT p.deflactor, p.fecha, p.var_mens, a.indice AS indice_ancla,
           row_number() OVER (PARTITION BY p.deflactor ORDER BY p.fecha) AS fila,
           ( (extract(year FROM p.fecha) * 12 + extract(month FROM p.fecha))
           - (extract(year FROM a.fecha) * 12 + extract(month FROM a.fecha)) ) AS meses_desde_ancla
    FROM public.inflacion_proyectada_actual p
    JOIN ancla a ON a.deflactor = p.deflactor
    WHERE p.fecha > a.fecha
),
proyectado AS (
    SELECT deflactor, fecha,
           (indice_ancla * exp(sum(ln(1 + var_mens))
               OVER (PARTITION BY deflactor ORDER BY fecha)))::double precision AS indice,
           'proyectado'::text AS origen
    FROM contiguo
    WHERE fila = meses_desde_ancla
)
SELECT deflactor, fecha, indice, origen FROM publicado
UNION ALL
SELECT deflactor, fecha, indice, origen FROM proyectado;

COMMENT ON VIEW public.deflactores IS
'Deflactor efectivo por moneda: lo publicado mas las proyecciones encadenadas. deflactor = ipc_largo (pesos) o uscpi_mensual (dolares). origen dice la procedencia de cada fila: publicado (dato oficial), interpolado (mes que el BLS no publico y estimamos, hoy solo oct-2025) o proyectado (de inflacion_proyectada). El nivel proyectado es indice_del_ultimo_publicado * producto acumulado de (1 + var_mens). Solo entra la corrida CONTIGUA de proyecciones que arranca justo despues del ultimo publicado: si falta un mes, la serie se corta ahi en lugar de encadenar mal. SIEMPRE mirar origen antes de presentar un valor como oficial.';


-- =====================================================================
-- Checks de validación
-- =====================================================================

-- 1. Con la tabla vacía, deflactores tiene que ser exactamente lo publicado.
SELECT deflactor, origen, count(*) AS filas, min(fecha) AS desde, max(fecha) AS hasta
FROM public.deflactores
GROUP BY deflactor, origen
ORDER BY deflactor, origen;

-- 2. Ninguna fecha duplicada por deflactor (rompería los joins).
SELECT deflactor, fecha, count(*)
FROM public.deflactores
GROUP BY deflactor, fecha HAVING count(*) > 1;

-- 3. Ningún índice nulo o <= 0.
SELECT count(*) AS indices_invalidos
FROM public.deflactores WHERE indice IS NULL OR indice <= 0;

-- 4. Los publicados tienen que seguir siendo idénticos a su serie de origen
--    (0 filas = OK). Es la garantía de que la vista no contamina lo oficial.
SELECT d.fecha, d.indice, l.indice AS en_ipc_largo
FROM public.deflactores d
JOIN public.ipc_largo l ON l.fecha = d.fecha
WHERE d.deflactor = 'ipc_largo' AND d.origen = 'publicado'
  AND d.indice <> l.indice::numeric;

-- 5. Proyecciones cargadas que NO entraron al encadenado (por hueco o por ser
--    anteriores al último publicado). Idealmente vacío; si aparece algo, hay una
--    proyección que se está ignorando y conviene saberlo.
SELECT p.deflactor, p.fecha, p.var_mens, p.fuente
FROM public.inflacion_proyectada_actual p
LEFT JOIN public.deflactores d
       ON d.deflactor = p.deflactor AND d.fecha = p.fecha AND d.origen = 'proyectado'
WHERE d.fecha IS NULL
ORDER BY p.deflactor, p.fecha;


-- =====================================================================
-- Cómo cargar una proyección
-- =====================================================================
-- `usuario` es obligatorio. La forma de no tipearlo a mano cada vez es pasarle el
-- usuario del sistema operativo desde la shell, que es el unico que sabe quien
-- sos, y usarlo como variable de psql.
--
-- Se usa $(id -un) y NO $USER: $USER es una variable de entorno y alcanza un
-- `export USER=otro` para que la shell mande cualquier cosa. `id -un` lee el UID
-- real del proceso, asi que devuelve el usuario verdadero siempre. Comprobado.
--
--   psql -h 10.0.16.3 -U postgres -d data -v quien="$(id -un)" <<'SQL'
--   insert into public.inflacion_proyectada (deflactor, fecha, var_mens, fuente, usuario)
--   values ('ipc_largo',     '2026-07-01', 0.019, 'REM BCRA 2026-07', :'quien'),
--          ('uscpi_mensual', '2026-07-01', 0.002, 'proyeccion propia', :'quien');
--   SQL
--
-- `quien` es solo el nombre de la variable de psql, elegido por nosotros: lo unico
-- que importa es que coincida entre `-v quien=` y `:'quien'`.
--
-- Ojo con las comillas: va :'quien' CON comilla simple, que es como psql interpola
-- una variable como literal de texto ('jmt'). Sin las comillas, :quien, la deja
-- pelada (jmt) y SQL la lee como nombre de columna: falla.
--
-- Para extender la serie real un mes hacen falta las DOS proyecciones, pesos y
-- dolares: el JOIN de prestamos_pm_real es inner y con una sola el mes no entra.
--
-- Varios meses de una: tienen que ser CONTIGUOS desde el ultimo mes publicado, o
-- la corrida se corta en el hueco (ver el check 5).
--
-- Corregir: insertar de nuevo el mismo (deflactor, fecha) con el valor nuevo. La
-- vista _actual se queda con la fila mas reciente y la vieja queda como historia,
-- con su propio `usuario`: asi se ve quien cargo el valor original y quien lo
-- corrigio.
