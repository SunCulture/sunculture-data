"""
Standard sections shared by the analysis reports (analysis/<nn_topic>/analysis*/generator.py).
Reports open with the title and go straight into Key Highlights; these sections close the report:

    methodology()     ->  '## Methodology': scope, definitions, data source
    report_details()  ->  'Prepared by / Date / Period Covered', the last lines of the report

Both return content blocks in the format the report generators already render, so a generator just
adds them to the end of its block list:

    import sys, os
    sys.path.insert(0, os.path.join(<path to analysis/>, "_templates"))
    import report_sections

    blocks += report_sections.methodology([("Scope", "…"), ("Units", "…"), …])
    blocks += report_sections.report_details(date=meta["generated_at"], period=(start, end))
"""
from datetime import date as _date

PREPARED_BY = "Data Team"


def _fields(rows):
    """A label / detail list, rendered as bold-label lines (a header-less two-column table)."""
    return {"type": "table", "columns": ["Item", "Detail"], "rows": [[str(a), str(b)] for a, b in rows],
            "note": None, "header": False, "text_cols": 2, "title": None}


def format_date(d):
    """e.g. 'Oct. 27, 2026'."""
    return f"{d:%b}. {d.day}, {d:%Y}"


def format_period(start, end):
    """e.g. 'February 23 – March 8, 2026', or 'September 28, 2017 – October 27, 2026' across years."""
    if start.year == end.year:
        return f"{start:%B} {start.day} – {end:%B} {end.day}, {end:%Y}"
    return f"{start:%B} {start.day}, {start:%Y} – {end:%B} {end.day}, {end:%Y}"


def report_details(date=None, period=None, prepared_by=PREPARED_BY):
    """Prepared by / Date / Period Covered. `date` defaults to today; `period` is a (start, end) pair of
    dates/datetimes or a ready-made string, and is left out when not given."""
    d = date or _date.today()
    rows = [("Prepared by", prepared_by), ("Date", format_date(d))]
    if period:
        rows.append(("Period Covered", period if isinstance(period, str) else format_period(*period)))
    return [_fields(rows)]


def methodology(items, heading="Methodology"):
    """'## Methodology' followed by one bold-label line per (label, detail) item."""
    return [{"type": "h2", "text": heading}, _fields(items)]
