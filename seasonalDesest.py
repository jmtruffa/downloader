"""Desestacionalización Census X-13ARIMA-SEATS llamando al binario directo.

Adaptado de /home/jmt/dev/downloader/etl/core/seasonal.py (el monorepo de ETLs),
que es donde vive la versión original y calibrada. Mismo método y mismo
vocabulario de parámetros para que la calibración hecha allá sirva acá. Dos
diferencias, a propósito:

  * usa SQLAlchemy en lugar de psycopg2, porque es la única capa de base que
    tiene este repo y no vale la pena mezclar dos;
  * es una copia y no un import cruzado: importar del monorepo ataría este ETL a
    sus internos (`from . import report`, el paquete `etl` entero en el sys.path)
    y un refactor allá lo rompería en silencio acá.

No depende de statsmodels: arma el .spc, ejecuta x13as y lee la tabla d11 (serie
desestacionalizada por X-11). Funciona con el binario "html" (x13ashtml), que es
el que se consigue precompilado para Linux.

Requiere `X13PATH` apuntando al binario o a su carpeta. Si falta, NO rompe:
devuelve status 'skipped' y el ETL sigue.
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
from datetime import date

import sqlalchemy
from sqlalchemy import text

MIN_MESES = 36           # X-13 necesita varios años de historia
MAX_LINEA = 120          # X-13 corta las líneas de input a ~132 chars; dejamos margen
TIMEOUT_X13 = 200        # segundos; el peor caso medido en el monorepo fue 34s


def _x13Binary():
    """Ruta al binario x13as a partir de X13PATH (carpeta o archivo), o None."""
    x13path = os.environ.get("X13PATH")
    if not x13path:
        return None
    if os.path.isfile(x13path):
        return x13path
    for name in ("x13as", "x13as.exe", "x13ashtml", "x13as_html"):
        cand = os.path.join(x13path, name)
        if os.path.isfile(cand):
            return cand
    return None


def _esContigua(dates):
    """True si la lista de fechas (primer día de mes) es mensual sin huecos."""
    for a, b in zip(dates, dates[1:]):
        esperadoMes = a.month % 12 + 1
        esperadoAnio = a.year + (1 if a.month == 12 else 0)
        if (b.year, b.month) != (esperadoAnio, esperadoMes):
            return False
    return True


def _writeSpc(path, dates, values, mode="mult", td="none", seasonalma="s3x5"):
    """Escribe el .spc de X-13: regARIMA (ARIMA automático + outliers) + X-11.

    `mode`:
      - 'add'   aditivo    -> transform=none (admite ceros)
      - 'mult'  multipl.   -> transform=log  (requiere serie estrictamente positiva)
      - 'auto'  X-13 elige -> transform=auto (decide por AIC)
    `td`: 'td1coef' (1 coef) / 'td' (6 coef) / 'none' (sin ajuste por días hábiles).
    `seasonalma`: filtro estacional del X-11; s3x5 es el estándar.
    """
    y, m = dates[0].year, dates[0].month
    nums = [f"{v:.3f}" for v in values]
    # Envolver por ancho de línea y no por conteo fijo: series de valores grandes
    # desbordan el límite de ~132 chars de X-13 y le parten un número al medio.
    # X-13 lee formato libre, así que el agrupado no cambia su salida.
    bloques, cur = [], "  "
    for n in nums:
        add = n if cur == "  " else " " + n
        if cur != "  " and len(cur) + len(add) > MAX_LINEA:
            bloques.append(cur)
            cur = "  " + n
        else:
            cur += add
    if cur.strip():
        bloques.append(cur)
    data = "\n".join(bloques)

    transform = {"add": "none", "mult": "log", "auto": "auto"}[mode]
    # d10=factores estacionales, d11=serie desest, d12=tendencia, d13=irregular.
    saves = "save=(d10 d11 d12 d13)"
    sma = f"seasonalma={seasonalma} "
    x11Opts = (f"mode=add {sma}" if mode == "add" else sma) + saves
    if td == "none":
        reg = ""
    elif mode == "auto":
        reg = f"regression{{ aictest=({td}) }}\n"
    else:
        reg = f"regression{{ variables=({td}) }}\n"

    spc = (
        f'series{{ title="serie" start={y}.{m:02d} period=12\n'
        f' data=(\n{data}\n ) }}\n'
        f'transform{{ function={transform} }}\n'
        f'{reg}'
        f'automdl{{ }}\n'
        f'outlier{{ }}\n'
        f'x11{{ {x11Opts} }}\n'
    )
    with open(path, "w") as f:
        f.write(spc)


def _parseD11(path):
    """Lee la tabla d11 -> lista de (date primer-día-de-mes, valor)."""
    out = []
    with open(path) as f:
        for ln in f:
            parts = ln.split()
            if len(parts) != 2:
                continue
            ym, val = parts
            if not (len(ym) == 6 and ym.isdigit()):
                continue  # saltea header y separador
            out.append((date(int(ym[:4]), int(ym[4:6]), 1), round(float(val), 3)))
    return out


_TAG_RE = re.compile(r"<[^>]*>")
# Modelo ARIMA: dos triples entre paréntesis, ej. (1 1 1)(0 1 1).
_MODEL_RE = re.compile(r"\(\s*\d+\s+\d+\s+\d+\s*\)\s*\(\s*\d+\s+\d+\s+\d+\s*\)")


def _arimaModel(workdir, base):
    """Modelo ARIMA elegido por automdl, leído del serie.html (la build HTML no
    genera .udg). Ancla en el modelo FINAL, no en el preliminar."""
    path = os.path.join(workdir, base + ".html")
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            contenido = _TAG_RE.sub("", f.read())
    except OSError:
        return None
    for label in ("Final automatic model choice", "ARIMA Model:"):
        idx = contenido.find(label)
        if idx != -1:
            m = _MODEL_RE.search(contenido, idx)
            if m:
                return re.sub(r"\s+", " ", m.group(0))
    return None


def x13Available():
    """True si hay binario x13as resoluble (X13PATH seteado y existe)."""
    return _x13Binary() is not None


def _result(serie, status, *, n=0, mode=None, reason="", outdir=None):
    return {"serie": serie, "status": status, "n": n,
            "mode": mode or "mult", "reason": reason, "outdir": outdir}


def deseasonalize(engine, *, serie, sourceView, table,
                  outEstado="desestacionalizado", fuente="census x13",
                  mode="mult", td="none", seasonalma="s3x5", start=None,
                  keepDir=None):
    """Corre X-13 sobre una serie observada y hace UPSERT de la desestacionalizada.

    - `sourceView`  vista con (serie, date, valor); se filtra por `serie`.
    - `table`       tabla long destino, con índice único parcial sobre
                    (serie, date) WHERE estado = outEstado.
    - `keepDir`     si se pasa, conserva la salida completa de x13as (serie.html
                    con modelo, factores y diagnósticos, más d10/d11/d12/d13 y el
                    .spc) en keepDir/<serie>/ para poder inspeccionarla.

    No imprime: devuelve un dict que el caller reporta de forma uniforme.
    """
    x13bin = _x13Binary()
    if not x13bin:
        return _result(serie, "skipped", reason="x13as no encontrado (setear X13PATH)")

    # 1. Serie observada desde la vista.
    with engine.connect() as con:
        rows = con.execute(
            text(f"SELECT date, valor FROM {sourceView} WHERE serie = :serie ORDER BY date"),
            {"serie": serie},
        ).fetchall()

    if start:
        rows = [r for r in rows if r[0] >= start]
    if len(rows) < MIN_MESES:
        return _result(serie, "skipped",
                       reason=f"serie corta ({len(rows)} meses, min {MIN_MESES})")

    dates = [r[0] for r in rows]
    values = [float(r[1]) for r in rows]
    if any(v is None for v in values):
        return _result(serie, "skipped", reason="la serie tiene valores nulos")
    if not _esContigua(dates):
        return _result(serie, "skipped", reason="la serie tiene huecos mensuales")

    # 2. Correr x13as en un directorio temporal.
    # El X-11 multiplicativo/log no admite valores <= 0: si aparece alguno,
    # forzamos aditivo en lugar de fallar.
    forcedAdd = mode != "add" and any(v <= 0 for v in values)
    if forcedAdd:
        mode = "add"

    if keepDir:
        workdir = os.path.join(keepDir, serie)
        os.makedirs(workdir, exist_ok=True)
    else:
        workdir = tempfile.mkdtemp(prefix="x13_")
    base = "serie"
    _writeSpc(os.path.join(workdir, base + ".spc"), dates, values,
              mode=mode, td=td, seasonalma=seasonalma)
    try:
        subprocess.run([x13bin, base], cwd=workdir, capture_output=True,
                       text=True, timeout=TIMEOUT_X13)
    except Exception as e:
        return _result(serie, "error", mode=mode,
                       reason=f"x13as no se pudo ejecutar: {e}")

    d11 = os.path.join(workdir, base + ".d11")
    if not os.path.isfile(d11):
        return _result(serie, "error", mode=mode,
                       reason=f"sin d11 (ver {workdir}/{base}_err.html)",
                       outdir=workdir if keepDir else None)
    ajustada = _parseD11(d11)

    # Parámetros de la corrida, para auditar por qué un valor puede cambiar.
    params = {
        "metodo": "x11",
        "modo": {"add": "aditivo", "mult": "multiplicativo", "auto": "auto"}[mode],
        "transform": {"add": "none", "mult": "log", "auto": "auto"}[mode],
        "regarima": True,
        "automdl": True,
        "outliers": "auto",
        "trading_day": td,
        "seasonalma": seasonalma,
        "tabla": "d11",
        "n_meses": len(rows),
        "desde": dates[0].isoformat(),
        "hasta": dates[-1].isoformat(),
    }
    arima = _arimaModel(workdir, base)
    if arima:
        params["arima"] = arima
    if forcedAdd:
        params["modo_motivo"] = "serie con algun valor <= 0 (el X-11 multiplicativo no lo admite)"

    # 3. UPSERT: 1 fila por (serie, mes), se actualiza en cada corrida.
    sql = text(
        f"INSERT INTO {table} (serie, date, valor, estado, fuente, parametros) "
        f"VALUES (:serie, :fecha, :valor, :estado, :fuente, CAST(:params AS jsonb)) "
        f"ON CONFLICT (serie, date) WHERE estado = '{outEstado}' "
        f"DO UPDATE SET valor = excluded.valor, ingested_at = now(), "
        f"parametros = excluded.parametros"
    )
    paramsJson = json.dumps(params)
    with engine.begin() as con:
        for d, val in ajustada:
            con.execute(sql, {"serie": serie, "fecha": d, "valor": val,
                              "estado": outEstado, "fuente": fuente,
                              "params": paramsJson})

    if not keepDir:
        shutil.rmtree(workdir, ignore_errors=True)
    return _result(serie, "ok", n=len(ajustada), mode=mode,
                   outdir=workdir if keepDir else None)


def runDesest(engine, dataset, jobs):
    """Corre la desest de una o más series y reporta un bloque por dataset.

    `jobs` = lista de dicts de kwargs para deseasonalize. X-13 nunca tumba el
    ETL: cualquier excepción se reporta como status=error.
    """
    print(f"[{dataset} / desest]")
    if not x13Available():
        print("  x13as no encontrado (setear X13PATH): se saltea la desestacionalización")
        return

    for kwargs in jobs:
        serie = kwargs.get("serie", "?")
        try:
            res = deseasonalize(engine, **kwargs)
        except Exception as e:
            res = _result(serie, "error", reason=str(e))

        if res["status"] == "ok":
            extra = f" [{res['mode']}]"
            print(f"  {res['serie']}: ok, {res['n']} meses{extra}")
        else:
            print(f"  {res['serie']}: {res['status']} — {res['reason']}")
