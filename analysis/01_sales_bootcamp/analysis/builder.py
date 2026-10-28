"""
May 2026 Sales Bootcamp — 4-month post-training review.

builder.py loads the sales data (Google Sheet, or a local CSV), computes every
table used in the report, then hands them to generator.py which writes the report
into ../output/ with a YYYYMMDDHHMM suffix.

Workflow: iterate on the Markdown draft (default) until the content is agreed,
then produce the Word version with --format docx. Both come from the same content.

Usage (from the repo root):
    python analysis_and_insights/01_sales_bootcamp/analysis/builder.py                       # .md draft
    python analysis_and_insights/01_sales_bootcamp/analysis/builder.py --source cache        # reuse last pull
    python analysis_and_insights/01_sales_bootcamp/analysis/builder.py --source cache --format docx
    python analysis_and_insights/01_sales_bootcamp/analysis/builder.py --source csv --csv path/to/sales.csv
"""
import argparse
import logging
import os
import re
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

import generator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ANALYSIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(ANALYSIS_DIR)
REPO_ROOT = os.path.abspath(os.path.join(PROJECT_DIR, "..", ".."))
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")
DATA_DIR = os.path.join(PROJECT_DIR, "data")  # local cache, git-ignored (contains customer PII)

load_dotenv(os.path.join(REPO_ROOT, ".env"))

# ---------------------------------------------------------------------------
# Analysis configuration
# ---------------------------------------------------------------------------
BOOTCAMP_START = pd.Timestamp("2026-05-10")
BOOTCAMP_END = pd.Timestamp("2026-05-13")
BOOTCAMP_PARTICIPANTS = 155

# Full calendar months only. May 2026 (bootcamp month) is a hold-out: shown in trends,
# excluded from both comparison windows.
PRE_MONTHS = pd.period_range("2026-01", "2026-04", freq="M")    # 4 full months before
POST_MONTHS = pd.period_range("2026-06", "2026-09", freq="M")   # 4 full months after
TREND_MONTHS = pd.period_range("2026-01", "2026-09", freq="M")  # monthly trend window
LY_PRE_MONTHS = pd.period_range("2025-01", "2025-04", freq="M")  # same windows last year
LY_POST_MONTHS = pd.period_range("2025-06", "2025-09", freq="M")

# Sheet tab names
SALES_TAB_GID = int(os.getenv("BOOTCAMP_SHEET_GID", "2139346053"))
ATTENDEES_TAB = "Copy of Data > May 2026 Attendeees"
TENURE_TAB = "Analysis 2"  # AMT Employee Id -> MIN of Sale Month (used as joining date)

# Employee tenure = months from first sale month to the bootcamp month. Fixed at the bootcamp
# so each agent stays in one band across both comparison windows.
TENURE_AS_OF = pd.Period("2026-05", freq="M")
JOINED_AFTER = "Joined after bootcamp"
TENURE_BANDS = [(-999, -1, JOINED_AFTER), (0, 5, "0–5 months"), (6, 11, "6–11 months"),
                (12, 23, "1–2 years"), (24, 35, "2–3 years"), (36, 9999, "3+ years")]
TENURE_ORDER = [b[2] for b in TENURE_BANDS]

COHORT_COL = "Employee - Bootcamp Cohort"
BOOTCAMP = "Bootcamp"
NON_BOOTCAMP = "Non-Bootcamp"

# Role -> segment (mirrors the 'Analysis 1' pivots). Other roles are excluded.
SEGMENT_BY_ROLE = {
    "FieldSales Agent": "Field Sales",
    "POP Agent": "Field Sales",
    "Sales Team Lead": "Field Sales",
    "TeleSales Agent": "Telesales",
    "Telesales Team Lead": "Telesales",
    "Service Center Associate": "SSC",
    "SSCSales Agent": "SSC",
}
SEGMENTS = ["Field Sales", "Telesales", "SSC"]
FIELD_AGENT_ROLES = ["FieldSales Agent", "POP Agent"]  # used for the STL cascade analysis

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def _sheet_client():
    import gspread

    key_file = os.path.join(REPO_ROOT, os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"])
    gc = gspread.service_account(filename=key_file)
    return gc.open_by_key(os.environ["BOOTCAMP_SHEET_ID"])


def _values_to_df(values):
    # Some tabs have duplicate / blank headers; keep the first occurrence of each name
    header, rows = values[0], values[1:]
    df = pd.DataFrame(rows, columns=range(len(header)))
    keep = {}
    for i, h in enumerate(header):
        if h and h not in keep:
            keep[h] = i
    return df[list(keep.values())].set_axis(list(keep.keys()), axis=1)


def load_from_sheet():
    sh = _sheet_client()
    logger.info("Connected to spreadsheet: %s", sh.title)
    sales = _values_to_df(sh.get_worksheet_by_id(SALES_TAB_GID).get_all_values())
    attendees = _values_to_df(sh.worksheet(ATTENDEES_TAB).get_all_values())
    tenure = _values_to_df(sh.worksheet(TENURE_TAB).get_all_values())
    os.makedirs(DATA_DIR, exist_ok=True)
    sales.to_csv(os.path.join(DATA_DIR, "sales_cache.csv"), index=False)
    attendees.to_csv(os.path.join(DATA_DIR, "attendees_cache.csv"), index=False)
    tenure.to_csv(os.path.join(DATA_DIR, "tenure_cache.csv"), index=False)
    return sales, attendees, tenure, sh.title


def _read_optional(path, columns):
    if path and os.path.exists(path):
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    return pd.DataFrame(columns=columns)


def load_from_csv(sales_csv, attendees_csv=None, tenure_csv=None):
    sales = pd.read_csv(sales_csv, dtype=str, keep_default_na=False)
    attendees = _read_optional(attendees_csv, ["Name"])
    tenure = _read_optional(tenure_csv, ["AMT Employee Id", "MIN of Sale Month"])
    return sales, attendees, tenure, os.path.basename(sales_csv)


def _tenure_band(months):
    if pd.isna(months):
        return None
    return next(lbl for lo, hi, lbl in TENURE_BANDS if lo <= months <= hi)


def prepare(sales, tenure=None):
    df = sales.copy()
    df = df[df["Category"] == "Unit"].copy()
    df["units"] = pd.to_numeric(df["Units"].str.replace(",", ""), errors="coerce").fillna(0)
    df["sale_date"] = pd.to_datetime(df["Sale Date"], errors="coerce")
    df = df[df["sale_date"].notna()].copy()
    df["month"] = df["sale_date"].dt.to_period("M")
    df["cohort"] = df[COHORT_COL].map({"May Cohort": BOOTCAMP}).fillna(NON_BOOTCAMP)
    df["segment"] = df["Employee Primary Role"].map(SEGMENT_BY_ROLE)
    df["agent"] = df["AMT Employee Id"].where(df["AMT Employee Id"] != "", df["Employee Names"])
    df["region"] = df["Customer Region"].replace("", "Not yet assigned (pending installation)")

    # Joining date = first sale month (Analysis 2 tab); fall back to first unit sale in this data
    first_sale = df.groupby("agent")["month"].min()
    if tenure is not None and len(tenure):
        sheet_first = pd.to_datetime(tenure.set_index("AMT Employee Id")["MIN of Sale Month"],
                                     errors="coerce").dt.to_period("M")
        df["first_month"] = df["AMT Employee Id"].map(sheet_first).fillna(df["agent"].map(first_sale))
    else:
        df["first_month"] = df["agent"].map(first_sale)
    df["tenure_months"] = [(TENURE_AS_OF - m).n if pd.notna(m) else None for m in df["first_month"]]
    df["tenure_band"] = df["tenure_months"].map(_tenure_band)

    df = df[df["segment"].notna()].copy()
    return df


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------
def productivity_by(df, keys):
    """Units, distinct selling agents and units/agent for each group."""
    g = df.groupby(keys).agg(units=("units", "sum"), agents=("agent", "nunique")).reset_index()
    g["productivity"] = g["units"] / g["agents"]
    return g


def window_productivity(df, months):
    """Productivity over several months = total units / total agent-months."""
    m = productivity_by(df[df["month"].isin(months)], ["month"])
    agent_months = m["agents"].sum()
    return {
        "units": m["units"].sum(),
        "avg_monthly_units": m["units"].sum() / len(months),
        "avg_monthly_agents": agent_months / len(months),
        "productivity": m["units"].sum() / agent_months if agent_months else float("nan"),
    }


def pct_change(new, old):
    return (new - old) / old if old else float("nan")


def _tokens(name):
    return set(t for t in re.sub(r"[^a-z ]", " ", str(name).lower()).split() if len(t) > 1)


def build_attendee_matcher(names):
    token_sets = [_tokens(n) for n in names if str(n).strip()]

    def attended(name):
        t = _tokens(name)
        return bool(t) and any(len(t & a) >= 2 or (t and t == a) for a in token_sets)
    return attended


# ---------------------------------------------------------------------------
# Analysis tables
# ---------------------------------------------------------------------------
def build_tables(df, attendees):
    t = {}
    boot = df[df["cohort"] == BOOTCAMP]
    field = df[df["segment"] == "Field Sales"]

    # 1. Pre vs post summary, by cohort x segment
    rows = []
    for cohort in [BOOTCAMP, NON_BOOTCAMP]:
        for seg in ["All segments"] + SEGMENTS:
            sub = df[df["cohort"] == cohort]
            if seg != "All segments":
                sub = sub[sub["segment"] == seg]
            pre, post = window_productivity(sub, PRE_MONTHS), window_productivity(sub, POST_MONTHS)
            if pre["avg_monthly_agents"] < 3 and post["avg_monthly_agents"] < 3:
                continue  # too few agents to say anything (e.g. non-bootcamp SSC)
            rows.append({
                "Cohort": cohort, "Segment": seg,
                "Agents/mo (pre)": pre["avg_monthly_agents"], "Agents/mo (post)": post["avg_monthly_agents"],
                "Units/mo (pre)": pre["avg_monthly_units"], "Units/mo (post)": post["avg_monthly_units"],
                "Productivity (pre)": pre["productivity"], "Productivity (post)": post["productivity"],
                "Change": pct_change(post["productivity"], pre["productivity"]),
            })
    t["summary"] = pd.DataFrame(rows)

    # 2. Monthly trend — bootcamp cohort overall + by segment
    m_all = productivity_by(boot[boot["month"].isin(TREND_MONTHS)], ["month"])
    m_seg = productivity_by(boot[boot["month"].isin(TREND_MONTHS)], ["month", "segment"])
    seg_prod = m_seg.pivot(index="month", columns="segment", values="productivity")[SEGMENTS]
    trend = m_all.set_index("month")[["agents", "units", "productivity"]].join(seg_prod)
    trend.insert(0, "Phase", [_phase(m) for m in trend.index])
    t["monthly_bootcamp"] = trend.reset_index()

    # 3. Field sales: bootcamp vs non-bootcamp, monthly. Agents who joined after the bootcamp have no
    #    "before", so they are excluded from the comparison (reported separately in the tenure tables).
    field_cmp = field[field["tenure_band"] != JOINED_AFTER]
    fm = productivity_by(field_cmp[field_cmp["month"].isin(TREND_MONTHS)], ["month", "cohort"])
    fp = fm.pivot(index="month", columns="cohort", values=["productivity", "agents", "units"])
    fc = pd.DataFrame({
        "Phase": [_phase(m) for m in fp.index],
        "Bootcamp agents": fp[("agents", BOOTCAMP)],
        "Bootcamp productivity": fp[("productivity", BOOTCAMP)],
        "Non-Bootcamp agents": fp[("agents", NON_BOOTCAMP)],
        "Non-Bootcamp productivity": fp[("productivity", NON_BOOTCAMP)],
    }, index=fp.index)
    fc["Gap"] = fc["Bootcamp productivity"] - fc["Non-Bootcamp productivity"]
    fc["Ratio"] = fc["Bootcamp productivity"] / fc["Non-Bootcamp productivity"]
    fc["Bootcamp unit share"] = fp[("units", BOOTCAMP)] / (fp[("units", BOOTCAMP)] + fp[("units", NON_BOOTCAMP)])
    fc["Bootcamp agent share"] = fc["Bootcamp agents"] / (fc["Bootcamp agents"] + fc["Non-Bootcamp agents"])
    t["field_compare"] = fc.reset_index()

    # 4. Seasonality check — Jan-Apr -> Jun-Sep change in 2025 (no bootcamp) vs 2026.
    #    Telesales / SSC have no non-bootcamp comparison group, so last year is their control.
    rows = []
    groups = [(BOOTCAMP, seg) for seg in ["All segments"] + SEGMENTS] + [(NON_BOOTCAMP, "Field Sales")]
    for cohort, seg in groups:
        sub = df[df["cohort"] == cohort]
        if seg != "All segments":
            sub = sub[sub["segment"] == seg]
        row = {"Cohort": cohort, "Segment": seg}
        for year, pre_m, post_m in [("2025", LY_PRE_MONTHS, LY_POST_MONTHS), ("2026", PRE_MONTHS, POST_MONTHS)]:
            pre, post = window_productivity(sub, pre_m), window_productivity(sub, post_m)
            row[f"{year} Jan-Apr"] = pre["productivity"]
            row[f"{year} Jun-Sep"] = post["productivity"]
            row[f"{year} change"] = pct_change(post["productivity"], pre["productivity"])
        row["2026 vs 2025 (pp)"] = (row["2026 change"] - row["2025 change"]) * 100
        rows.append(row)
    t["seasonality"] = pd.DataFrame(rows)

    # 4b. Year-on-year — same months, 2026 vs 2025, before (Jan-Apr) and after (Jun-Sep) the bootcamp
    rows = []
    for cohort, seg in groups:
        sub = df[df["cohort"] == cohort]
        if seg != "All segments":
            sub = sub[sub["segment"] == seg]
        pre25, pre26 = window_productivity(sub, LY_PRE_MONTHS), window_productivity(sub, PRE_MONTHS)
        post25, post26 = window_productivity(sub, LY_POST_MONTHS), window_productivity(sub, POST_MONTHS)
        row = {"Cohort": cohort, "Segment": seg,
               "Jan-Apr YoY": pct_change(pre26["productivity"], pre25["productivity"]),
               "Jun-Sep YoY": pct_change(post26["productivity"], post25["productivity"]),
               "Agents/mo 2025": post25["avg_monthly_agents"], "Agents/mo 2026": post26["avg_monthly_agents"],
               "Units YoY": pct_change(post26["avg_monthly_units"], post25["avg_monthly_units"])}
        row["Shift (pp)"] = (row["Jun-Sep YoY"] - row["Jan-Apr YoY"]) * 100
        rows.append(row)
    t["yoy"] = pd.DataFrame(rows)

    # Month-by-month Jun-Sep, 2025 vs 2026, bootcamp cohort overall + by segment
    ly_months = list(LY_POST_MONTHS)
    yb = boot[boot["month"].isin(ly_months + list(POST_MONTHS))]
    overall = productivity_by(yb, ["month"]).assign(segment="Overall")
    by_seg = productivity_by(yb, ["month", "segment"])
    ym = pd.concat([overall, by_seg]).pivot(index="month", columns="segment", values="productivity")
    out = pd.DataFrame({"Month": [m.strftime("%b") for m in POST_MONTHS]})
    for seg in ["Overall"] + SEGMENTS:
        out[f"{seg} 2025"] = [ym.loc[m, seg] for m in ly_months]
        out[f"{seg} 2026"] = [ym.loc[m, seg] for m in POST_MONTHS]
    t["yoy_monthly"] = out
    # Late post-period (last 2 months) vs same months last year — is the lift still there?
    t["yoy_late"] = {"months": f"{POST_MONTHS[-2].strftime('%b')}–{POST_MONTHS[-1].strftime('%b')}",
                     "2025": window_productivity(boot, LY_POST_MONTHS[-2:])["productivity"],
                     "2026": window_productivity(boot, POST_MONTHS[-2:])["productivity"]}

    # 4c. Employee tenure
    t.update(_tenure_tables(df, boot, field))

    # 4d. Regional comparison (field, home region per agent)
    t["region_field"], t["region_other"] = _region_table(df, field[field["tenure_band"] != JOINED_AFTER])
    bf = field[field["cohort"] == BOOTCAMP]
    t["region_pending_share"] = {
        w: bf.loc[bf["month"].isin(m) & (bf["Customer Region"] == ""), "units"].sum() / bf.loc[bf["month"].isin(m), "units"].sum()
        for w, m in [("pre", PRE_MONTHS), ("post", POST_MONTHS)]}

    # 5. Growth drivers — share of the bootcamp cohort's monthly unit growth by segment
    rows = []
    for seg in SEGMENTS:
        sub = boot[boot["segment"] == seg]
        pre, post = window_productivity(sub, PRE_MONTHS), window_productivity(sub, POST_MONTHS)
        rows.append({"Segment": seg, "Units/mo (pre)": pre["avg_monthly_units"],
                     "Units/mo (post)": post["avg_monthly_units"],
                     "Added units/mo": post["avg_monthly_units"] - pre["avg_monthly_units"]})
    drivers = pd.DataFrame(rows)
    drivers["Share of growth"] = drivers["Added units/mo"] / drivers["Added units/mo"].sum()
    t["drivers"] = drivers

    # 7. STL cascade — field agents split by own attendance x their STL's attendance
    attendee_names = list(attendees.get("Name", pd.Series(dtype=str))) + \
        list(df.loc[df["cohort"] == BOOTCAMP, "Employee Names"].unique())
    attended = build_attendee_matcher(attendee_names)
    fa = field[field["Employee Primary Role"].isin(FIELD_AGENT_ROLES)].copy()
    sup_map = {s: attended(s) for s in fa["Supervisor"].unique()}
    fa["stl_attended"] = fa["Supervisor"].map(sup_map)
    fa = fa[fa["Supervisor"] != ""]
    rows = []
    for agent_att in [True, False]:
        for stl_att in [True, False]:
            sub = fa[((fa["cohort"] == BOOTCAMP) == agent_att) & (fa["stl_attended"] == stl_att)]
            pre, post = window_productivity(sub, PRE_MONTHS), window_productivity(sub, POST_MONTHS)
            rows.append({"Agent attended": "Yes" if agent_att else "No",
                         "STL attended": "Yes" if stl_att else "No",
                         "Agents/mo (post)": post["avg_monthly_agents"],
                         "Productivity (pre)": pre["productivity"], "Productivity (post)": post["productivity"],
                         "Change": pct_change(post["productivity"], pre["productivity"])})
    t["cascade"] = pd.DataFrame(rows)
    recent = fa[fa["month"].isin(POST_MONTHS)]
    t["cascade_coverage"] = {
        "supervisors": recent["Supervisor"].nunique(),
        "supervisors_attended": recent.loc[recent["stl_attended"], "Supervisor"].nunique(),
        "unit_share_attended_stl": recent.loc[recent["stl_attended"], "units"].sum() / recent["units"].sum(),
    }

    # 8. Mix shifts for the bootcamp cohort (share of units, pre vs post)
    for key, col, top in [("mix_product", "Product", 8), ("mix_source", "Lead Source", 8), ("mix_region", "region", 10)]:
        t[key] = _mix_shift(boot, col, top)

    # 9. Top improvers per segment (total units Jun-Sep vs Jan-Apr; equal 4-month windows)
    pre_u = boot[boot["month"].isin(PRE_MONTHS)].groupby(["segment", "agent"])["units"].sum()
    post_u = boot[boot["month"].isin(POST_MONTHS)].groupby(["segment", "agent"])["units"].sum()
    imp = pd.concat([pre_u.rename("pre"), post_u.rename("post")], axis=1).fillna(0).reset_index()
    imp["change"] = imp["post"] - imp["pre"]
    names = boot.groupby("agent")["Employee Names"].agg(lambda s: s.value_counts().index[0])
    imp["name"] = imp["agent"].map(names)
    t["top_improvers"] = {seg: imp[imp["segment"] == seg].nlargest(5, "change") for seg in SEGMENTS}

    # 10. Retention — how many bootcamp agents sold in each post month
    selling_pre = set(boot.loc[boot["month"].isin(PRE_MONTHS), "agent"])
    t["active_bootcamp_agents"] = {
        "ever_sold_2026": boot.loc[boot["month"].isin(PRE_MONTHS.append(POST_MONTHS)), "agent"].nunique(),
        "sold_pre": len(selling_pre),
        "sold_post": boot.loc[boot["month"].isin(POST_MONTHS), "agent"].nunique(),
        "sold_last_month": boot.loc[boot["month"] == POST_MONTHS[-1], "agent"].nunique(),
    }
    return t


def _tenure_tables(df, boot, field):
    out = {}
    windows = PRE_MONTHS.append(POST_MONTHS)

    # Bootcamp selling agents (sold in either window) by tenure band x segment, plus median tenure
    ag = boot[boot["month"].isin(windows)].drop_duplicates("agent")
    dist = pd.crosstab(ag["tenure_band"], ag["segment"]).reindex(index=TENURE_ORDER, columns=SEGMENTS, fill_value=0)
    dist = dist[dist.sum(axis=1) > 0]
    dist["All"] = dist.sum(axis=1)
    out["tenure_dist"] = dist.rename_axis("Tenure band").reset_index()
    out["tenure_median_years"] = {seg: ag.loc[ag["segment"] == seg, "tenure_months"].median() / 12 for seg in SEGMENTS}
    out["tenure_median_years"]["All"] = ag["tenure_months"].median() / 12

    # Field productivity by tenure band, cohort and window (post-bootcamp joiners excluded)
    rows = []
    for band in TENURE_ORDER[1:]:
        row = {"Tenure band": band}
        for cohort, key in [(BOOTCAMP, "B"), (NON_BOOTCAMP, "NB")]:
            x = field[(field["tenure_band"] == band) & (field["cohort"] == cohort)]
            pre, post = window_productivity(x, PRE_MONTHS), window_productivity(x, POST_MONTHS)
            row[f"{key} agents"] = post["avg_monthly_agents"]
            row[f"{key} pre"], row[f"{key} post"] = pre["productivity"], post["productivity"]
            row[f"{key} change"] = pct_change(post["productivity"], pre["productivity"])
            row[f"{key} agent_months_pre"] = pre["avg_monthly_agents"] * len(PRE_MONTHS)
            row[f"{key} agent_months_post"] = post["avg_monthly_agents"] * len(POST_MONTHS)
        rows.append(row)
    tf = pd.DataFrame(rows)
    out["tenure_field"] = tf

    # Tenure-matched: non-bootcamp band productivity weighted to the bootcamp tenure mix
    matched = {}
    for w in ["pre", "post"]:
        weights = tf[f"B agent_months_{w}"] / tf[f"B agent_months_{w}"].sum()
        nb = (weights * tf[f"NB {w}"].fillna(0)).sum()
        bt = (weights * tf[f"B {w}"].fillna(0)).sum()
        matched[w] = {"bootcamp": bt, "non_bootcamp": nb, "ratio": bt / nb if nb else float("nan")}
    out["tenure_matched"] = matched

    # Agents who joined after the bootcamp, within the non-bootcamp field base Jun-Sep
    nb_post = field[(field["cohort"] == NON_BOOTCAMP) & field["month"].isin(POST_MONTHS)]
    joiners = nb_post[nb_post["tenure_band"] == JOINED_AFTER]
    am_all = productivity_by(nb_post, ["month"])["agents"].sum()
    am_new = productivity_by(joiners, ["month"])["agents"].sum() if len(joiners) else 0
    out["new_joiners"] = {"share_agent_months": am_new / am_all if am_all else 0,
                          "productivity": window_productivity(joiners, POST_MONTHS)["productivity"],
                          "agents_per_month": am_new / len(POST_MONTHS)}
    return out


MIN_REGION_AGENTS = 1.5  # regions with fewer bootcamp selling agents/month are left out of the regional table
PENDING_REGION = "Pending installation (no region yet)"


def _region_table(df, field):
    """Field productivity by Customer Region: units sold into a region / distinct selling agents with a sale
    there that month. An agent selling into several regions counts in each, so regions do not add up to Overall."""
    field = field.assign(cust_region=field["Customer Region"].replace("", PENDING_REGION))
    full = field  # Overall row covers all field sales, including excluded small regions

    def stats(x):
        pre, post = window_productivity(x, PRE_MONTHS), window_productivity(x, POST_MONTHS)
        return pre["avg_monthly_agents"], post["avg_monthly_agents"], pre["productivity"], post["productivity"]

    boot = field[field["cohort"] == BOOTCAMP]
    small = [r for r in field["cust_region"].unique() if r != PENDING_REGION
             and max(stats(boot[boot["cust_region"] == r])[:2]) < MIN_REGION_AGENTS]
    field = field[~field["cust_region"].isin(small)]  # too few bootcamp sales to compare; noted in the report

    rows = []
    for region in list(field["cust_region"].unique()) + ["Overall"]:
        x = full if region == "Overall" else field[field["cust_region"] == region]
        row = {"Region": region}
        for cohort, key in [(BOOTCAMP, "B"), (NON_BOOTCAMP, "NB")]:
            _, ag_post, pre, post = stats(x[x["cohort"] == cohort])
            row[f"{key} agents"], row[f"{key} pre"], row[f"{key} post"] = ag_post, pre, post
            row[f"{key} change"] = pct_change(post, pre)
        rows.append(row)
    out = pd.DataFrame(rows)
    tail = [PENDING_REGION, "Overall"]
    body = out[~out["Region"].isin(tail)].sort_values("B agents", ascending=False)
    ordered = [body] + [out[out["Region"] == r] for r in tail]
    return pd.concat(ordered, ignore_index=True), small


def _phase(month):
    if month < pd.Period(BOOTCAMP_START, "M"):
        return "Pre"
    if month == pd.Period(BOOTCAMP_START, "M"):
        return "Bootcamp month"
    return "Post"


def _mix_shift(boot, col, top):
    pre = boot[boot["month"].isin(PRE_MONTHS)].groupby(col)["units"].sum()
    post = boot[boot["month"].isin(POST_MONTHS)].groupby(col)["units"].sum()
    mix = pd.concat([(pre / pre.sum()).rename("Share (pre)"), (post / post.sum()).rename("Share (post)")], axis=1).fillna(0)
    mix["Change (pp)"] = (mix["Share (post)"] - mix["Share (pre)"]) * 100
    mix = mix.sort_values("Share (post)", ascending=False)
    head = mix.head(top).copy()
    rest = mix.iloc[top:]
    if len(rest):
        head.loc["All others"] = [rest["Share (pre)"].sum(), rest["Share (post)"].sum(), rest["Change (pp)"].sum()]
    return head.rename_axis(col if col != "region" else "Region").reset_index()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", choices=["sheet", "cache", "csv"], default="sheet",
                        help="sheet: pull from Google Sheets (default); cache: reuse last pull in data/; csv: --csv file")
    parser.add_argument("--csv", help="Sales CSV export (with --source csv)")
    parser.add_argument("--attendees-csv", help="Optional attendees CSV (with --source csv)")
    parser.add_argument("--tenure-csv", help="Optional first-sale-month CSV (with --source csv); "
                                             "otherwise tenure is derived from the sales data")
    parser.add_argument("--format", choices=["md", "docx"], default="md",
                        help="md: draft for review (default); docx: final stakeholder version")
    args = parser.parse_args()

    if args.source == "sheet":
        sales, attendees, tenure, source_name = load_from_sheet()
    elif args.source == "cache":
        sales, attendees, tenure, _ = load_from_csv(os.path.join(DATA_DIR, "sales_cache.csv"),
                                                    os.path.join(DATA_DIR, "attendees_cache.csv"),
                                                    os.path.join(DATA_DIR, "tenure_cache.csv"))
        source_name = "Sales Bootcamp Google Sheet (cached pull)"
    else:
        if not args.csv:
            parser.error("--csv is required with --source csv")
        sales, attendees, tenure, source_name = load_from_csv(args.csv, args.attendees_csv, args.tenure_csv)

    logger.info("Loaded %s sales rows, %s attendee rows, %s tenure rows", len(sales), len(attendees), len(tenure))
    df = prepare(sales, tenure)
    tables = build_tables(df, attendees)

    meta = {
        "source": source_name,
        "data_through": df["sale_date"].max(),
        "generated_at": datetime.now(),
        "bootcamp_start": BOOTCAMP_START, "bootcamp_end": BOOTCAMP_END,
        "participants": BOOTCAMP_PARTICIPANTS,
        "pre_months": PRE_MONTHS, "post_months": POST_MONTHS,
        "ly_pre_months": LY_PRE_MONTHS, "ly_post_months": LY_POST_MONTHS,
        "tenure_as_of": TENURE_AS_OF,
    }
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stamp = meta["generated_at"].strftime("%Y%m%d%H%M")
    # Every run is a new file so drafts can be compared; never overwrite an earlier one
    out_path = os.path.join(OUTPUT_DIR, f"sales_bootcamp_4_month_review_{stamp}.{args.format}")
    n = 2
    while os.path.exists(out_path):
        out_path = os.path.join(OUTPUT_DIR, f"sales_bootcamp_4_month_review_{stamp}_{n}.{args.format}")
        n += 1
    generator.generate(tables, meta, out_path, fmt=args.format)
    logger.info("Report written to %s", out_path)


if __name__ == "__main__":
    main()
