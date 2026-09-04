"""ETL for the BCRA A3500 wholesale reference exchange rate (Com. "A" 3500).

Two independent sources feed the same table:

  1. com3500.xls, the spreadsheet the BCRA publishes. Primary source.
  2. The BCRA statistics API, variable id 5 ("Tipo de cambio mayorista de
     referencia"). Used as a fallback when the spreadsheet brings no new date.

Both were compared over the 162 overlapping business days of 2026 and agree
exactly (max absolute difference 0.0), so a row is worth the same whichever
path produced it. The fallback exists because the spreadsheet lags: on
2026-09-03 the 17:00 run downloaded the file and found nothing, while the API
already served that day's value.

Both paths write through the same upsert, so any run is idempotent: re-running
rewrites identical rows instead of duplicating or aborting.
"""

import io
import os
import sys
from datetime import date, datetime, timedelta

import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import text
from urllib3.exceptions import InsecureRequestWarning

from dataBaseConn2 import DatabaseConnection

XLS_URL = "https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/com3500.xls"
API_URL = "https://api.bcra.gob.ar/estadisticas/v4.0/Monetarias"
API_VARIABLE_ID = 5  # "Tipo de cambio mayorista de referencia"
API_MAX_LIMIT = 3000  # hard cap: limit=3001 answers HTTP 400
TABLE = "A3500"

# Days re-read before the last stored date. Keeps every run idempotent and
# absorbs a late correction without re-reading the whole series.
OVERLAP_DAYS = 5

HTTP_TIMEOUT = 60


def log(message):
    """Write to stdout, which the cron wrapper appends to the log file."""
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} | {message}", flush=True)


def fail(message):
    """Write to stderr, which the cron wrapper leaves free for the MAILTO."""
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} | {message}", file=sys.stderr, flush=True)


def fetch_xls():
    """Download com3500.xls and return a [date, A3500] frame, or None on failure.

    verify=False is deliberate: the www.bcra.gob.ar certificate chain does not
    validate here, while the API host does and is fetched normally.
    """
    try:
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        response = requests.get(XLS_URL, verify=False, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        fail(f"XLS download failed: {exc}")
        return None

    try:
        # Columns C:D of the first sheet, three header rows skipped. Positional,
        # so a layout change on the BCRA side surfaces as a parse error here
        # rather than as silently wrong data downstream.
        data = pd.read_excel(io.BytesIO(response.content), sheet_name=0,
                             usecols="C:D", skiprows=3)
    except Exception as exc:
        fail(f"XLS parse failed: {exc}")
        return None

    if data.shape[1] != 2:
        fail(f"XLS layout changed: expected 2 columns in C:D, got {data.shape[1]}")
        return None

    data.columns = ["date", "A3500"]
    try:
        data["date"] = pd.to_datetime(data["date"], format="%d/%m/%Y")
    except (ValueError, TypeError) as exc:
        fail(f"XLS date column did not parse as %d/%m/%Y: {exc}")
        return None

    data["A3500"] = pd.to_numeric(data["A3500"], errors="coerce")
    data = data.dropna(subset=["date", "A3500"])
    log(f"XLS: {len(data)} rows, last date {data['date'].max().date()}")
    return normalize(data)


def fetch_api(desde, hasta):
    """Read variable id 5 from the BCRA API and return a [date, A3500] frame.

    Returns None when the API cannot be reached or answers an error. Paginates
    by offset because limit is capped at API_MAX_LIMIT; the empty-page guard
    protects against a server that reports a count it does not deliver.
    """
    rows = []
    offset = 0
    while True:
        params = {
            "desde": desde.isoformat(),
            "hasta": hasta.isoformat(),
            "limit": API_MAX_LIMIT,
            "offset": offset,
        }
        try:
            response = requests.get(f"{API_URL}/{API_VARIABLE_ID}", params=params,
                                    timeout=HTTP_TIMEOUT)
        except requests.exceptions.RequestException as exc:
            fail(f"API request failed: {exc}")
            return None

        if response.status_code != 200:
            # Errors always arrive as a non-200 with errorMessages; there is no
            # 200 carrying an error payload.
            try:
                detail = "; ".join(response.json().get("errorMessages", []))
            except ValueError:
                detail = response.text[:200]
            fail(f"API returned HTTP {response.status_code}: {detail}")
            return None

        try:
            payload = response.json()
            results = payload.get("results") or []
            page = results[0].get("detalle", []) if results else []
            total = payload["metadata"]["resultset"]["count"]
        except (ValueError, KeyError, IndexError, AttributeError) as exc:
            fail(f"API response was not in the expected shape: {exc}")
            return None

        rows.extend(page)
        offset += API_MAX_LIMIT
        if offset >= total or len(page) == 0:
            break

    if not rows:
        log(f"API: no rows between {desde} and {hasta}")
        return normalize(pd.DataFrame(columns=["date", "A3500"]))

    data = pd.DataFrame(rows).rename(columns={"fecha": "date", "valor": "A3500"})
    data["date"] = pd.to_datetime(data["date"])
    data["A3500"] = pd.to_numeric(data["A3500"], errors="coerce")
    data = data.dropna(subset=["date", "A3500"])
    log(f"API: {len(data)} rows, last date {data['date'].max().date()}")
    return normalize(data)


def normalize(data):
    """Sort ascending and drop duplicate dates.

    The API answers newest first. The spreadsheet occasionally repeats a date,
    and date carries a UNIQUE constraint, so a duplicate inside one statement
    would abort the whole write.
    """
    data = data[["date", "A3500"]].sort_values("date")
    return data.drop_duplicates(subset="date", keep="last").reset_index(drop=True)


def ensure_table(engine):
    with engine.begin() as conn:
        conn.execute(text(
            f'CREATE TABLE IF NOT EXISTS "{TABLE}" ('
            ' date date NOT NULL,'
            f' "{TABLE}" double precision,'
            f' CONSTRAINT "{TABLE}_date_unique" UNIQUE (date))'
        ))


def last_stored_date(engine):
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT MAX(date) FROM "{TABLE}"')).scalar()
    return result


def upsert(engine, data):
    """Write rows, overwriting any date already present.

    ON CONFLICT is what makes both paths idempotent: the same run repeated, or
    the two sources overlapping, converges on identical rows.
    """
    if data.empty:
        return 0
    statement = text(
        f'INSERT INTO "{TABLE}" (date, "{TABLE}") VALUES (:date, :value)'
        f' ON CONFLICT (date) DO UPDATE SET "{TABLE}" = EXCLUDED."{TABLE}"'
    )
    records = [{"date": row.date.date(), "value": float(row.A3500)}
               for row in data.itertuples()]
    with engine.begin() as conn:
        conn.execute(statement, records)
    return len(records)


def window_start(last_date):
    """First date worth writing: the overlap tail behind what is already stored."""
    if last_date is None:
        return date(1900, 1, 1)
    return last_date - timedelta(days=OVERLAP_DAYS)


def downloadA3500():
    db = DatabaseConnection(db_type="postgresql", db_name=os.environ.get("POSTGRES_DB"))
    engine = db.engine
    try:
        ensure_table(engine)
        last_date = last_stored_date(engine)
        log(f"Last stored date: {last_date}")
        start = window_start(last_date)

        source = "XLS"
        data = fetch_xls()

        fresh = 0
        if data is not None and not data.empty:
            fresh = len(data[data["date"].dt.date > last_date]) if last_date else len(data)

        if data is None or fresh == 0:
            reason = "download failed" if data is None else "brought no new date"
            log(f"XLS {reason}; falling back to the BCRA API (id {API_VARIABLE_ID})")
            source = "API"
            data = fetch_api(start, date.today())
            if data is None:
                fail("Both the XLS and the API failed: A3500 was not updated")
                return False
            fresh = len(data[data["date"].dt.date > last_date]) if last_date else len(data)

        window = data[data["date"].dt.date >= start]
        written = upsert(engine, window)
        log(f"{source}: upserted {written} rows (>= {start}), {fresh} of them new")

        if fresh == 0:
            log("No new date available from either source yet")
    finally:
        engine.dispose()

    return True


if __name__ == "__main__":
    if not downloadA3500():
        sys.exit(1)
