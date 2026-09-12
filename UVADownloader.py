"""ETL for the BCRA UVA index (Unidad de Valor Adquisitivo, base 31.3.16=14.05).

Two independent sources feed the same table:

  1. The BCRA statistics API, variable id 31. Primary source.
  2. diar_uva.xls, the spreadsheet the BCRA publishes. Fallback, used ONLY
     when the API request itself fails. It is deliberately not used when the
     API merely brings no new date: that is the normal outcome of most runs
     here (see below), and there is no evidence the spreadsheet ever leads the
     API -- for A3500 the spreadsheet was measured lagging it.

Both were compared over the full series on 2026-09-12: 3851 dates each, the
same range, and a maximum absolute difference of 0.0. A row is worth the same
whichever path produced it.

Two things about this series drive the design and are easy to get wrong:

  * It is published FORWARD. The BCRA extends UVA about a month past the
    current day, so MAX(date) in the table is normally a future date. The API
    window therefore has to ask up to today + FORWARD_DAYS; asking up to
    today would return a range that ends before it starts once the stored
    date overtakes the calendar.
  * The extension only happens after INDEC publishes the CPI, because UVA
    tracks CER. That is why the cron runs in the CPI window (days 10-20)
    instead of every day.

Both paths write through the same upsert, so any run is idempotent:
re-running rewrites identical rows instead of duplicating or aborting.
"""

import io
import os
import sys
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from sqlalchemy import text
from urllib3.exceptions import InsecureRequestWarning

from dataBaseConn2 import DatabaseConnection

API_URL = "https://api.bcra.gob.ar/estadisticas/v4.0/Monetarias"
API_VARIABLE_ID = 31  # "Unidad de valor adquisitivo (base 31.3.16=14.05)"
API_MAX_LIMIT = 3000  # hard cap: limit=3001 answers HTTP 400
XLS_URL = "http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/diar_uva.xls"

# Table and value column are both upper case, so every reference below is
# quoted: unquoted "UVA" folds to "uva" in Postgres and would point at a
# relation that does not exist.
TABLE = "UVA"
COLUMN = "UVA"

# Pinned, NOT derived from TABLE. The index was created while the table was
# still named "uva" and kept that name through the rename (Postgres renames a
# table, not its indexes). Deriving it would look for "UVA_date_unique", miss,
# and CREATE IF NOT EXISTS would add a second redundant unique index on the
# same column.
INDEX = "uva_date_unique"

# Days re-read before the last stored date. Keeps every run idempotent and
# absorbs a late correction without re-reading the whole series.
OVERLAP_DAYS = 5

# How far past today the API `hasta` reaches. The series runs roughly one month
# ahead; 90 days leaves room for that plus a longer publication than usual.
# Only `hasta` may be in the future: the API rejects a future `desde`.
FORWARD_DAYS = 90

# First date of the series (31/03/2016). Used as the window start on an empty
# table so the first run backfills everything.
SERIES_START = date(2016, 1, 1)

HTTP_TIMEOUT = 60


def log(message):
    """Write to stdout, which the cron wrapper appends to the log file."""
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} | {message}", flush=True)


def fail(message):
    """Write to stderr, which the cron wrapper leaves free for the MAILTO."""
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} | {message}", file=sys.stderr, flush=True)


def fetch_api(desde, hasta):
    """Read variable id 31 from the BCRA API and return a [date, UVA] frame.

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
        return normalize(pd.DataFrame(columns=["date", COLUMN]))

    data = pd.DataFrame(rows).rename(columns={"fecha": "date", "valor": COLUMN})
    data["date"] = pd.to_datetime(data["date"])
    data[COLUMN] = pd.to_numeric(data[COLUMN], errors="coerce")
    data = data.dropna(subset=["date", COLUMN])
    log(f"API: {len(data)} rows, last date {data['date'].max().date()}")
    return normalize(data)


def fetch_xls():
    """Download diar_uva.xls and return a [date, UVA] frame, or None on failure.

    verify=False is deliberate: the www.bcra.gov.ar certificate chain does not
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
        # Columns A:B of the first sheet. The sheet carries 27 preamble rows
        # (the last two being the "fecha | uva1" header and a "cd_serie" row),
        # so the series starts at 27 with no header of its own. Positional, so
        # a layout change on the BCRA side surfaces as a parse error here
        # rather than as silently wrong data downstream.
        data = pd.read_excel(io.BytesIO(response.content), sheet_name=0,
                             usecols="A:B", skiprows=27, header=None)
    except Exception as exc:
        fail(f"XLS parse failed: {exc}")
        return None

    if data.shape[1] != 2:
        fail(f"XLS layout changed: expected 2 columns in A:B, got {data.shape[1]}")
        return None

    data.columns = ["date", COLUMN]
    try:
        data["date"] = pd.to_datetime(data["date"], format="%d/%m/%Y")
    except (ValueError, TypeError) as exc:
        fail(f"XLS date column did not parse as %d/%m/%Y: {exc}")
        return None

    data[COLUMN] = pd.to_numeric(data[COLUMN], errors="coerce")
    data = data.dropna(subset=["date", COLUMN])
    log(f"XLS: {len(data)} rows, last date {data['date'].max().date()}")
    return normalize(data)


def normalize(data):
    """Sort ascending and drop duplicate dates.

    The API answers newest first. A duplicate date inside one statement would
    make the upsert fail ("cannot affect row a second time"), so they are
    collapsed before the write rather than after.
    """
    data = data[["date", COLUMN]].sort_values("date")
    return data.drop_duplicates(subset="date", keep="last").reset_index(drop=True)


def ensure_table(engine):
    """Create the table when missing and guarantee the unique date index.

    The table already existed with no index of any kind, so the index is
    created separately instead of as an inline constraint: that covers both a
    fresh database and the one in production. ON CONFLICT needs it — without a
    unique index the upsert below fails outright.
    """
    with engine.begin() as conn:
        conn.execute(text(
            f'CREATE TABLE IF NOT EXISTS "{TABLE}" ('
            ' date date,'
            f' "{COLUMN}" double precision)'
        ))
        conn.execute(text(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{INDEX}"'
            f' ON "{TABLE}" (date)'
        ))


def last_stored_date(engine):
    with engine.connect() as conn:
        return conn.execute(text(f'SELECT MAX(date) FROM "{TABLE}"')).scalar()


def upsert(engine, data):
    """Write rows, overwriting any date already present.

    ON CONFLICT is what makes both paths idempotent: the same run repeated, or
    the two sources overlapping, converges on identical rows.
    """
    if data.empty:
        return 0
    statement = text(
        f'INSERT INTO "{TABLE}" (date, "{COLUMN}") VALUES (:date, :value)'
        f' ON CONFLICT (date) DO UPDATE SET "{COLUMN}" = EXCLUDED."{COLUMN}"'
    )
    records = [{"date": row.date.date(), "value": float(getattr(row, COLUMN))}
               for row in data.itertuples()]
    with engine.begin() as conn:
        conn.execute(statement, records)
    return len(records)


def window_start(last_date):
    """First date worth writing: the overlap tail behind what is already stored."""
    if last_date is None:
        return SERIES_START
    return last_date - timedelta(days=OVERLAP_DAYS)


def count_fresh(data, last_date):
    """Rows the sources carry beyond what is already stored."""
    if data is None or data.empty:
        return 0
    if last_date is None:
        return len(data)
    return len(data[data["date"].dt.date > last_date])


def downloadUVA():
    db = DatabaseConnection(db_type="postgresql", db_name=os.environ.get("POSTGRES_DB"))
    engine = db.engine
    try:
        ensure_table(engine)
        last_date = last_stored_date(engine)
        log(f"Last stored date: {last_date}")
        start = window_start(last_date)
        # Reaches past today because the series is published forward; see the
        # module docstring.
        horizon = date.today() + timedelta(days=FORWARD_DAYS)

        # The API refuses a `desde` past today ("La fecha desde no puede mayor a
        # la actual"), and `start` sits in the future whenever the stored series
        # already runs ahead of the calendar — which is the normal state here.
        # Clamping only narrows what is ASKED; `start` still decides what is
        # written, so the overlap window is unaffected.
        source = "API"
        data = fetch_api(min(start, date.today()), horizon)

        # The fallback fires only when the API actually FAILED, never merely
        # because it brought no new date. A3500 falls back on "no new date"
        # because there its primary is the spreadsheet, which was measured
        # lagging behind the API. Here the primary IS the API and nothing
        # suggests the spreadsheet ever leads it, so "no new date" from the API
        # means the value does not exist yet anywhere -- which, for a series
        # that only extends once the CPI is out, is the normal outcome of most
        # runs. Falling back on it would download the spreadsheet on every
        # healthy run, report the XLS as the source that did the work, and turn
        # any BCRA layout change into a false "both sources failed" alert with
        # a perfectly good API frame already in hand.
        if data is None:
            log("API request failed; falling back to diar_uva.xls")
            source = "XLS"
            data = fetch_xls()
            if data is None:
                fail("Both the API and the XLS failed: UVA was not updated")
                return False

        if data.empty:
            fail(f"{source} returned no rows at all: UVA was not updated")
            return False

        fresh = count_fresh(data, last_date)
        window = data[data["date"].dt.date >= start]
        written = upsert(engine, window)
        log(f"{source}: upserted {written} rows (>= {start}), {fresh} of them new")

        if fresh == 0:
            # Expected on most runs: the series only extends after INDEC
            # publishes the CPI. Not a warning.
            log("No new date yet; the series has not been extended since the last run")
    finally:
        engine.dispose()

    return True


if __name__ == "__main__":
    if not downloadUVA():
        sys.exit(1)
