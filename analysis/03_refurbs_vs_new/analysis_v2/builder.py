"""
Refurbished vs new units by product type — Battery vs Direct (gross unit sales: sold + refunded accounts).
One report per country; the product view (refurb vs new overall, by product) is ../analysis/.

Compares four segments: New Battery, Refurb Battery, New Direct and Refurb Direct.
    Battery = pump + controller + battery (Climate Smart Battery, CSB)
    Direct  = pump connected straight to the panels (Climate Smart Direct, CSD)
Product type comes from ../data/product_groups_v2.csv (product_id -> product_group), maintained by hand;
any product not listed there counts as Other. Product types other than Battery and Direct appear in the
overview only.

Reads the same snapshots as ../analysis/builder.py (../output/data/); --refresh pulls a new one.

Usage (from the repo root):
    python analysis/03_refurbs_vs_new/analysis_v2/builder.py                  # Kenya, latest snapshot
    python analysis/03_refurbs_vs_new/analysis_v2/builder.py --country uganda
    python analysis/03_refurbs_vs_new/analysis_v2/builder.py --docx           # also write a Word version
    python analysis/03_refurbs_vs_new/analysis_v2/builder.py --refresh        # pull fresh first (or --fresh)
    python analysis/03_refurbs_vs_new/analysis_v2/builder.py --snapshot 202610271522
    python analysis/03_refurbs_vs_new/analysis_v2/builder.py --snapshot sales_and_refunds_accounts_202610271522.csv
    python analysis/03_refurbs_vs_new/analysis_v2/builder.py --list-snapshots
"""
import argparse
import glob
import logging
import os
import re
from datetime import datetime

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from scipy.stats import binomtest, chi2_contingency

import generator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ANALYSIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(ANALYSIS_DIR)
REPO_ROOT = os.path.abspath(os.path.join(PROJECT_DIR, "..", ".."))
QUERY_DIR = os.path.join(PROJECT_DIR, "queries")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")
DATA_DIR = os.path.join(OUTPUT_DIR, "data")  # snapshots, git-ignored (contains customer PII)
PRODUCT_GROUPS_CSV = os.path.join(PROJECT_DIR, "data", "product_groups_v2.csv")  # hand-maintained, tracked in git

load_dotenv(os.path.join(REPO_ROOT, ".env"))

# ---------------------------------------------------------------------------
# Analysis configuration
# ---------------------------------------------------------------------------
# snapshot name -> query file (shared with ../analysis/builder.py)
DATASETS = {
    "sales_and_refunds_accounts": "sales_and_refunds_accounts.sql",
    "products_refurb": "products_refurb.sql",
}
DATE_COLS = ["dispatchDate", "createdAt", "sale_date", "RefundDate", "_generated_at"]

COUNTRIES = ["kenya", "uganda"]
REFURB, NEW = "Refurb", "New"
GROUPS = ["Battery", "Direct"]                                   # the product types compared
SEGMENTS = [f"{u} {g}" for g in GROUPS for u in (NEW, REFURB)]   # New Battery, Refurb Battery, New Direct, ...
OTHER = "Other"                                                  # products not in the mapping
REFUNDED = "Refunded"
NOT_RECORDED = "Not recorded"
ACCOUNT_TYPES = {"CASH": "Cash", "PAYG": "PAYG", "LEASE": "Lease"}

MIN_TREND_YEAR = 2017      # a handful of accounts carry placeholder creation dates (e.g. 2000)
MONTHS = 13                # monthly views: last 12 full months + the current month to date
TOP_N = 10
MAX_DISPATCH_DAYS = 365    # sale -> dispatch gaps outside 0..365 days are treated as data errors
DISPATCH_YEAR_FROM = 2021  # first year in the dispatch-days-by-year table (refurb sales are tiny before)
SALES_YEAR_FROM = 2020     # first year in the refurb units by account type table
MIN_GROUP_UNITS = 200      # an STL needs this many Battery (or Direct) units before that rate is tested
DISPATCH_PENDING = "Dispatch Pending"


# ---------------------------------------------------------------------------
# Data loading — ClickHouse pull or saved snapshot
# ---------------------------------------------------------------------------
def _ch_client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ["SC_CH_DB_HOST"], port=int(os.environ["SC_CH_DB_PORT"]),
        username=os.environ["SC_CH_DB_USER"], password=os.environ["SC_CH_DB_PASSWORD"], secure=True)


def pull_snapshot():
    """Run every query against ClickHouse and save the results as a new timestamped snapshot."""
    client = _ch_client()
    stamp = datetime.now().strftime("%Y%m%d%H%M")
    os.makedirs(DATA_DIR, exist_ok=True)
    for name, query_file in DATASETS.items():
        with open(os.path.join(QUERY_DIR, query_file), encoding="utf-8") as fh:
            df = client.query_df(fh.read())
        path = os.path.join(DATA_DIR, f"{name}_{stamp}.csv")
        df.to_csv(path, index=False)
        logger.info("Pulled %s rows from %s -> %s", len(df), query_file, path)
    return stamp


def list_snapshots():
    """Snapshot stamps for which every dataset exists, oldest first."""
    stamps = None
    for name in DATASETS:
        found = {re.search(r"_(\d{12})\.csv$", p).group(1)
                 for p in glob.glob(os.path.join(DATA_DIR, f"{name}_*.csv"))
                 if re.search(r"_(\d{12})\.csv$", p)}
        stamps = found if stamps is None else stamps & found
    return sorted(stamps or [])


def load_snapshot(stamp):
    data = {}
    for name in DATASETS:
        path = os.path.join(DATA_DIR, f"{name}_{stamp}.csv")
        df = pd.read_csv(path, low_memory=False)
        for col in DATE_COLS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        data[name] = df
    return data


# ---------------------------------------------------------------------------
# Product types — hand-maintained mapping
# ---------------------------------------------------------------------------
def load_product_groups():
    """product_id -> product type from the mapping CSV (blank types count as Other)."""
    groups = pd.read_csv(PRODUCT_GROUPS_CSV, usecols=["product_id", "product_group"], dtype={"product_group": str},
                         keep_default_na=False)
    groups["product_group"] = groups["product_group"].str.strip().replace("", OTHER)
    dupes = groups[groups["product_id"].duplicated(keep=False)]
    if len(dupes):
        logger.warning("product_id listed more than once in %s (first row used): %s", PRODUCT_GROUPS_CSV,
                       sorted(dupes["product_id"].unique()))
    return groups.drop_duplicates("product_id").set_index("product_id")["product_group"]


# ---------------------------------------------------------------------------
# Cleaning
# ---------------------------------------------------------------------------
def prepare(raw, products, groups, country):
    df = raw[raw["companyRegion"] == country].copy()
    df["is_refurb"] = df["productId"].map(products.drop_duplicates("id").set_index("id")["isRefurb"]).eq(1)
    df["unit_type"] = np.where(df["is_refurb"], REFURB, NEW)
    df["product_group"] = df["productId"].map(groups).fillna(OTHER)
    df["segment"] = np.where(df["product_group"].isin(GROUPS), df["unit_type"] + " " + df["product_group"], None)
    # Units sold = productQty (cash accounts can carry several units)
    df["units"] = df["productQty"].fillna(1).clip(lower=1).astype(int)
    df["refunded"] = df["status"] == REFUNDED
    df["refunded_units"] = df["units"].where(df["refunded"], 0)
    # Refunded accounts rarely have a sale_date: date them by RefundDate, else the account creation date
    df["gross_date"] = df["sale_date"].fillna(df["RefundDate"]).fillna(df["createdAt"])
    df["Sales_channel"] = df["Sales_channel"].fillna(NOT_RECORDED)
    for col in ["employee_name", "supervisor_name"]:
        df[col] = df[col].str.replace(r"\s+", " ", regex=True).str.strip().replace("", pd.NA)
    df["region"] = df["region"].replace("", pd.NA).fillna(NOT_RECORDED)
    df["account_type"] = df["accountType"].map(ACCOUNT_TYPES).fillna(NOT_RECORDED)
    return df


# ---------------------------------------------------------------------------
# Analysis tables
# ---------------------------------------------------------------------------
def _seg_split(df, key):
    """Units per segment and the refurb % within Battery and within Direct, per group of `key`."""
    g = df.pivot_table(index=key, columns="segment", values="units", aggfunc="sum", fill_value=0)
    g = g.reindex(columns=SEGMENTS, fill_value=0)
    out = pd.DataFrame(index=g.index)
    for grp in GROUPS:
        new, ref = g[f"{NEW} {grp}"], g[f"{REFURB} {grp}"]
        out[f"{NEW} {grp}"], out[f"{REFURB} {grp}"] = new, ref
        out[f"Refurb % of {grp}"] = ref / (new + ref).replace(0, np.nan)
    out["Total"] = g.sum(axis=1)
    return out


def _months(df, date_col, current):
    """Rows in the last MONTHS calendar months up to `current`, with a 'month' period column."""
    out = df[df[date_col].notna()].copy()
    out["month"] = out[date_col].dt.to_period("M")
    return out[out["month"].between(current - (MONTHS - 1), current)]


def _month_label(period, current):
    return period.strftime("%b %Y") + (" (to date)" if period == current else "")


def _mode(s):
    return s.mode().iat[0] if s.notna().any() else NOT_RECORDED


def _for_half(counts):
    """How many sellers (largest first) it takes to reach half of the units."""
    c = counts.sort_values(ascending=False).cumsum() / counts.sum()
    return int((c < 0.5).sum() + 1)


def _days_by(v, key):
    """Average and median days to dispatch, new vs refurb, per `key` (year or month)."""
    agg = v.pivot_table(index=key, columns="unit_type", values="days", aggfunc=["mean", "median"])
    return pd.DataFrame({"new_mean": agg.get(("mean", NEW)), "new_median": agg.get(("median", NEW)),
                         "refurb_mean": agg.get(("mean", REFURB)), "refurb_median": agg.get(("median", REFURB))},
                        index=agg.index)


def _refurb_cols():
    return [f"{REFURB} {g}" for g in GROUPS]


def build_tables(all_units, df, current):
    """all_units: every unit in the country (for the product-type overview); df: Battery and Direct only."""
    t = {}

    # 1. Proportion -----------------------------------------------------------
    og = all_units.pivot_table(index="product_group", columns="unit_type", values="units", aggfunc="sum",
                               fill_value=0).reindex(columns=[NEW, REFURB], fill_value=0)
    og = og.reindex(GROUPS).fillna(0)  # only Battery and Direct are reported
    og["Total"] = og.sum(axis=1)
    og["refurb_pct"] = og[REFURB] / og["Total"]
    # Splits across product types — each of these adds up to 100%
    og["share_new"] = og[NEW] / og[NEW].sum()
    og["share_refurb"] = og[REFURB] / og[REFURB].sum()
    t["product_types"] = og.rename_axis("Product type").reset_index()

    seg = df.groupby("segment").agg(accounts=("account_id", "size"), gross=("units", "sum"),
                                    refunded=("refunded_units", "sum")).reindex(SEGMENTS)
    seg["share"] = seg["gross"] / seg["gross"].sum()
    seg["refund_rate"] = seg["refunded"] / seg["gross"]
    priced = df[df["account_type"].isin(["Cash", "PAYG"]) & (df["Total_Price"] > 0)]
    price = (priced.assign(ppu=priced["Total_Price"] / priced["units"])
             .groupby(["segment", "account_type"])["ppu"].median().unstack())
    seg["price_cash"] = price.get("Cash")
    seg["price_payg"] = price.get("PAYG")
    t["segments"] = seg.rename_axis("Segment").reset_index()

    rp = df[df["is_refurb"]].groupby(["product_group", "product"])["units"].sum()
    rows = []
    for grp in GROUPS:
        s = rp.xs(grp).sort_values(ascending=False) if grp in rp.index.get_level_values(0) else pd.Series(dtype=int)
        for product, units in s.head(5).items():
            rows.append((product, grp, units, units / s.sum()))
        if len(s) > 5:
            rows.append((f"Other {grp} refurb products", grp, s.iloc[5:].sum(), s.iloc[5:].sum() / s.sum()))
    t["refurb_products"] = pd.DataFrame(rows, columns=["Product", "Product type", "Units", "share"])

    # 2. Trends ---------------------------------------------------------------
    trend = df[df["gross_date"].dt.year >= MIN_TREND_YEAR].assign(year=lambda x: x["gross_date"].dt.year)
    t["yearly"] = _seg_split(trend, "year").rename_axis("Year").reset_index()
    m = _seg_split(_months(df, "gross_date", current), "month")
    m.index = [_month_label(p, current) for p in m.index]
    t["monthly"] = m.rename_axis("Month").reset_index()

    # 3. Selling agents and STLs ----------------------------------------------
    names = df.groupby("salesAgents").agg(name=("employee_name", _mode), stl=("supervisor_name", _mode))
    agents = _seg_split(df, "salesAgents").join(names)
    agents["refurb"] = agents[_refurb_cols()].sum(axis=1)
    top = agents.sort_values(["refurb", "Total"], ascending=False).head(TOP_N)
    t["agents_top"] = top.reset_index()
    t["agent_stats"] = {"agents": len(agents), "top_stls": top["stl"].nunique(),
                        "top_stl": top["stl"].mode().iat[0], "top_stl_agents": int(top["stl"].value_counts().iat[0])}
    for grp in GROUPS:
        ref, new = agents[f"{REFURB} {grp}"], agents[f"{NEW} {grp}"]
        t["agent_stats"][grp] = {
            "with_refurb": int((ref > 0).sum()),
            "top_share": ref.nlargest(TOP_N).sum() / ref.sum() if ref.sum() else np.nan,
            "top_new_share": new.nlargest(TOP_N).sum() / new.sum() if new.sum() else np.nan,
            "half_refurb": _for_half(ref) if ref.sum() else 0,
            "half_new": _for_half(new) if new.sum() else 0,
        }

    stl = _seg_split(df, "supervisor_id").join(df.groupby("supervisor_id").agg(name=("supervisor_name", _mode)))
    stl["refurb"] = stl[_refurb_cols()].sum(axis=1)
    t["stl_stats"] = {"stls": len(stl)}
    for grp in GROUPS:
        ref, new = stl[f"{REFURB} {grp}"], stl[f"{NEW} {grp}"]
        size = ref + new
        p0 = ref.sum() / size.sum() if size.sum() else np.nan
        tested = size >= MIN_GROUP_UNITS
        n_tests = int(tested.sum())

        def _vs(i, ref=ref, size=size, p0=p0, tested=tested, n_tests=n_tests):
            if not tested[i]:
                return "Too few units"
            p = min(1.0, binomtest(int(ref[i]), int(size[i]), p0).pvalue * n_tests)
            if p >= 0.05:
                return "In line"
            return "Higher" if ref[i] / size[i] > p0 else "Lower"

        stl[f"vs avg {grp}"] = [_vs(i) for i in stl.index]
        rated = stl[tested].assign(rate=ref[tested] / size[tested]).sort_values("rate", ascending=False)
        t["stl_stats"][grp] = {
            "avg": p0, "tested": n_tests,
            "higher": int((stl[f"vs avg {grp}"] == "Higher").sum()),
            "lower": int((stl[f"vs avg {grp}"] == "Lower").sum()),
            "chi2_p": chi2_contingency(np.column_stack([ref[tested], new[tested]]))[1]
                      if n_tests > 1 and ref[tested].sum() else np.nan,
            "top_rate_name": rated["name"].iat[0] if len(rated) else None,
            "top_rate": rated["rate"].iat[0] if len(rated) else np.nan,
            "with_refurb": int((ref > 0).sum()),
        }
    t["stl_top"] = stl.sort_values(["refurb", "Total"], ascending=False).head(TOP_N).reset_index()
    t["channels"] = _seg_split(df, "Sales_channel").sort_values("Total", ascending=False) \
        .rename_axis("Sales channel").reset_index()

    # 4. Sale -> dispatch (per account: one dispatch per account) -------------
    s = df[df["sale_date"].notna()].copy()
    s["days"] = (s["dispatchDate"].dt.normalize() - s["sale_date"].dt.normalize()).dt.days
    v = s[s["days"].between(0, MAX_DISPATCH_DAYS)]
    d = pd.DataFrame({
        "sold": s.groupby("segment").size(),
        "used": v.groupby("segment").size(),
        "mean": v.groupby("segment")["days"].mean(),
        "median": v.groupby("segment")["days"].median(),
        "within_7": v.groupby("segment")["days"].apply(lambda x: (x <= 7).mean()),
        "within_30": v.groupby("segment")["days"].apply(lambda x: (x <= 30).mean()),
    }).reindex(SEGMENTS)
    t["dispatch"] = d.rename_axis("Segment").reset_index()
    # Refurb vs new (Battery + Direct together) by year of sale — has the gap changed?
    vy = v.assign(year=v["sale_date"].dt.year, w7=v["days"] <= 7)
    vy = vy[vy["year"] >= DISPATCH_YEAR_FROM]
    w7 = vy.pivot_table(index="year", columns="unit_type", values="w7", aggfunc="mean")
    dy = _days_by(vy, "year").assign(new_w7=w7.get(NEW), refurb_w7=w7.get(REFURB))
    dy["coverage"] = s.groupby(s["sale_date"].dt.year)["dispatchDate"].apply(lambda x: x.notna().mean()) \
        .reindex(dy.index)
    t["dispatch_by_year"] = dy.rename_axis("Year of sale").reset_index()
    t["dispatch_excluded"] = {"no_dispatch": int(s["dispatchDate"].isna().sum()),
                              "negative": int((s["days"] < 0).sum()),
                              "over_max": int((s["days"] > MAX_DISPATCH_DAYS).sum())}
    # Same view as dispatch_by_year, month by month: refurb vs new, average and median side by side
    vm = _months(v, "sale_date", current)
    dm = _days_by(vm, "month")
    dm["refurb_n"] = vm[vm["unit_type"] == REFURB].groupby("month").size().reindex(dm.index)
    dm["coverage"] = _months(s, "sale_date", current).groupby("month")["dispatchDate"] \
        .apply(lambda x: x.notna().mean())
    dm.index = [_month_label(p, current) for p in dm.index]
    t["dispatch_monthly"] = dm.rename_axis("Month of sale").reset_index()
    pend = pd.DataFrame({
        "sold": s.groupby("segment")["units"].sum(),
        "pending": s[s["funnel_status"] == DISPATCH_PENDING].groupby("segment")["units"].sum(),
    }).reindex(SEGMENTS).fillna(0)
    pend["pending_share"] = pend["pending"] / pend["sold"]
    t["pending_dispatch"] = pend.rename_axis("Segment").reset_index()

    # 5. Account type (Cash vs PAYG) ------------------------------------------
    at = _seg_split(df, "account_type").sort_values("Total", ascending=False)
    t["account_type"] = at.rename_axis("Account type").reset_index()
    t["account_type_test"] = {}
    for grp in GROUPS:
        cp = at.reindex(["Cash", "PAYG"])[[f"{NEW} {grp}", f"{REFURB} {grp}"]].fillna(0)
        t["account_type_test"][grp] = chi2_contingency(cp.values)[1] if cp.values.min() > 0 else np.nan
    cp_df = df[df["account_type"].isin(["Cash", "PAYG"])].assign(year=lambda x: x["gross_date"].dt.year)
    years = list(range(current.year, SALES_YEAR_FROM - 1, -1))
    ru = (cp_df[cp_df["is_refurb"]].pivot_table(index="year", columns=["product_group", "account_type"],
                                                 values="units", aggfunc="sum"))
    t["refurb_units_by_year"] = pd.DataFrame(
        {f"Refurb {g} ({a})": ru.get((g, a), pd.Series(dtype=float)).reindex(years).fillna(0)
         for g in GROUPS for a in ["Cash", "PAYG"]}, index=years).rename_axis("Sales Year").reset_index()
    ry = cp_df[cp_df["year"] >= MIN_TREND_YEAR]
    shares = {}
    for grp in GROUPS:
        sub = ry[ry["product_group"] == grp]
        tot = sub.pivot_table(index="year", columns="account_type", values="units", aggfunc="sum")
        ref = sub[sub["is_refurb"]].pivot_table(index="year", columns="account_type", values="units", aggfunc="sum")
        for a in ["Cash", "PAYG"]:
            shares[f"{grp} ({a})"] = ref.get(a) / tot.get(a)
    refurb_by_year = ry[ry["is_refurb"]].groupby("year")["units"].sum()
    by_year = pd.DataFrame(shares).loc[lambda x: x.index.isin(refurb_by_year[refurb_by_year >= 20].index)]
    t["account_type_by_year"] = by_year.rename_axis("Year").reset_index()

    # 6. Region ---------------------------------------------------------------
    rg = _seg_split(df, "region")
    rg = pd.concat([rg.drop(NOT_RECORDED, errors="ignore").sort_values("Total", ascending=False),
                    rg.loc[[NOT_RECORDED]] if NOT_RECORDED in rg.index else None])
    t["region"] = rg.rename_axis("Region").reset_index()

    # 7. Customers with more than one refurb unit -----------------------------
    rc = df[df["is_refurb"]].pivot_table(index="customerId", columns="product_group", values="units",
                                         aggfunc="sum", fill_value=0).reindex(columns=GROUPS, fill_value=0)
    bands = {}
    for grp in GROUPS:
        c = rc.loc[rc[grp] > 0, grp]
        bands[f"{grp} customers"] = pd.cut(c, [0, 1, 2, 3, np.inf], labels=["1", "2", "3", "4+"]) \
            .value_counts().reindex(["1", "2", "3", "4+"])
    t["refurb_customers"] = pd.DataFrame(bands).rename_axis("Refurb units per customer").reset_index()
    new_customers = set(df.loc[~df["is_refurb"], "customerId"])
    t["refurb_customer_stats"] = {
        **{grp: {"customers": int((rc[grp] > 0).sum()), "multi": int((rc[grp] > 1).sum())} for grp in GROUPS},
        "both": int(((rc[GROUPS] > 0).all(axis=1)).sum()),
        "customers": len(rc),
        "also_new": int(rc.index.isin(new_customers).sum()),
    }
    return t


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--refresh", "--fresh", action="store_true",
                        help="pull fresh data from ClickHouse (saved as a new snapshot) before analysing")
    parser.add_argument("--snapshot", help="snapshot to analyse: its stamp (YYYYMMDDHHMM) or one of its file names, "
                                           "e.g. sales_and_refunds_accounts_202610271841.csv; default: the latest")
    parser.add_argument("--list-snapshots", action="store_true", help="list saved snapshots and exit")
    parser.add_argument("--country", choices=COUNTRIES, default="kenya", help="country to report on (default: kenya)")
    parser.add_argument("--docx", action="store_true",
                        help="also write a Word version (same content) next to the Markdown report")
    args = parser.parse_args()

    if args.list_snapshots:
        for s in list_snapshots():
            print(s)
        return

    if args.refresh:
        stamp = pull_snapshot()
    elif args.snapshot:
        found = re.search(r"\d{12}", os.path.basename(args.snapshot))
        if not found or found.group() not in list_snapshots():
            parser.error(f"snapshot {args.snapshot} not found in {DATA_DIR} (see --list-snapshots)")
        stamp = found.group()
    else:
        snaps = list_snapshots()
        if snaps:
            stamp = snaps[-1]
        else:
            logger.info("No snapshot in %s yet — pulling from ClickHouse", DATA_DIR)
            stamp = pull_snapshot()

    data = load_snapshot(stamp)
    logger.info("Using snapshot %s: %s accounts", stamp, len(data["sales_and_refunds_accounts"]))
    snapshot_at = datetime.strptime(stamp, "%Y%m%d%H%M")
    groups = load_product_groups()
    all_units = prepare(data["sales_and_refunds_accounts"], data["products_refurb"], groups, args.country)
    df = all_units[all_units["segment"].notna()].copy()
    tables = build_tables(all_units, df, pd.Period(snapshot_at, "M"))

    meta = {
        "snapshot": stamp,
        "snapshot_at": snapshot_at,
        "generated_at": datetime.now(),
        "country": args.country.title(),
        # Period covered: first sale in the report up to the snapshot
        "period": (df.loc[df["gross_date"].dt.year >= MIN_TREND_YEAR, "gross_date"].min(), snapshot_at),
        "trend_from": MIN_TREND_YEAR, "max_dispatch_days": MAX_DISPATCH_DAYS, "top_n": TOP_N,
        "min_group_units": MIN_GROUP_UNITS,
    }
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_stamp = meta["generated_at"].strftime("%Y%m%d%H%M")
    # Every run is a new file so drafts can be compared; never overwrite an earlier one
    base_name = f"refurbs_vs_new_battery_vs_direct_{args.country}_{out_stamp}"
    out_path = os.path.join(OUTPUT_DIR, f"{base_name}.md")
    n = 2
    while os.path.exists(out_path):
        out_path = os.path.join(OUTPUT_DIR, f"{base_name}_{n}.md")
        n += 1
    generator.generate(tables, meta, out_path)
    logger.info("Report written to %s", out_path)
    if args.docx:
        docx_path = os.path.splitext(out_path)[0] + ".docx"
        generator.generate(tables, meta, docx_path, fmt="docx")
        logger.info("Word version written to %s", docx_path)


if __name__ == "__main__":
    main()
