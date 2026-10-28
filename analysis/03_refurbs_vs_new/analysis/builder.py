"""
Refurbished vs non-refurbished units review (gross unit sales: sold + refunded accounts).
One report per country.

builder.py pulls the account data from ClickHouse (or reuses a saved snapshot), computes
every table used in the report, then hands them to generator.py which writes the report
into ../output/ with a YYYYMMDDHHMM suffix.

Snapshots: each pull is saved to ../output/data/ with the pull timestamp, so an analysis
can be re-run on exactly the same data later, or on a fresh pull:
    sales_and_refunds_accounts_<stamp>.csv   one row per gross-sales unit account
    products_refurb_<stamp>.csv              amt.products id / product / isRefurb

Refurb units are those whose product has isRefurb = 1 in amt.products (joined on productId);
every other unit is New.

Usage (from the repo root):
    python analysis/03_refurbs_vs_new/analysis/builder.py                       # Kenya, latest snapshot
    python analysis/03_refurbs_vs_new/analysis/builder.py --country uganda
    python analysis/03_refurbs_vs_new/analysis/builder.py --refresh             # pull fresh first (or --fresh)
    python analysis/03_refurbs_vs_new/analysis/builder.py --snapshot 202610271231
    python analysis/03_refurbs_vs_new/analysis/builder.py --snapshot sales_and_refunds_accounts_202610271231.csv
    python analysis/03_refurbs_vs_new/analysis/builder.py --list-snapshots
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

load_dotenv(os.path.join(REPO_ROOT, ".env"))

# ---------------------------------------------------------------------------
# Analysis configuration
# ---------------------------------------------------------------------------
# snapshot name -> query file
DATASETS = {
    "sales_and_refunds_accounts": "sales_and_refunds_accounts.sql",
    "products_refurb": "products_refurb.sql",
}
DATE_COLS = ["Repossession_created_at", "dispatchDate", "boqdate", "boqcreatedAt", "fullDepositDate", "jsfDate",
             "customer_preferred_date", "jsf_date", "jsf_completed_date", "warranty_expiry_date", "createdAt",
             "installment_date", "end_date", "employee_createdAt", "Full_Deposit_Date", "sale_date",
             "first_payment_date", "last_payment_date", "RefundDate", "leadCreatedAt", "writeOffDate",
             "cds1_date", "cds2_date", "creditScoreDate", "_generated_at"]

COUNTRIES = ["kenya", "uganda"]
REFURB, NEW = "Refurb", "New"
UNIT_TYPES = [REFURB, NEW]
REFUNDED = "Refunded"
NOT_RECORDED = "Not recorded"

MIN_TREND_YEAR = 2017      # a handful of accounts carry placeholder creation dates (e.g. 2000)
MONTHS = 13                # monthly views: last 12 full months + the current month to date
TOP_N = 10
MAX_DISPATCH_DAYS = 365    # sale -> dispatch gaps outside 0..365 days are treated as data errors
BULK_QTY = 10              # cash accounts with this many units or more are called out as bulk orders
SALES_YEAR_FROM = 2020     # first year in the refurb units by account type table
MIN_STL_UNITS = 200        # STLs need this many units before their refurb share is tested against the country
# funnel_status stages: units waiting to go out, and units with no dispatch on record
DISPATCH_PENDING, DISPATCH_MISSING = "Dispatch Pending", "Dispatch Missing"
ACCOUNT_TYPES = {"CASH": "Cash", "PAYG": "PAYG", "LEASE": "Lease"}


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
# Cleaning
# ---------------------------------------------------------------------------
def prepare(raw, products, country):
    df = raw[raw["companyRegion"] == country].copy()
    flags = products.drop_duplicates("id").set_index("id")["isRefurb"]
    df["is_refurb"] = df["productId"].map(flags).eq(1)
    df["unit_type"] = np.where(df["is_refurb"], REFURB, NEW)
    # Units sold = productQty (cash accounts can carry several units); one account otherwise
    df["units"] = df["productQty"].fillna(1).clip(lower=1).astype(int)
    df["refurb_units"] = df["units"].where(df["is_refurb"], 0)
    df["refunded"] = df["status"] == REFUNDED
    df["refunded_units"] = df["units"].where(df["refunded"], 0)
    # Refunded accounts rarely have a sale_date: date them by RefundDate, or the account creation date where
    # RefundDate is empty (the two are usually a couple of weeks apart)
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
def _split(df, key):
    """Refurb / new units (sum of productQty) and refurb share per group."""
    g = df.pivot_table(index=key, columns="unit_type", values="units", aggfunc="sum", fill_value=0)
    g = g.reindex(columns=UNIT_TYPES, fill_value=0)
    g.columns.name = None
    g["Total"] = g.sum(axis=1)
    g["Refurb share"] = g[REFURB] / g["Total"]
    return g


def _months(df, date_col, current):
    """Rows in the last MONTHS calendar months up to `current`, with a 'month' period column."""
    out = df[df[date_col].notna()].copy()
    out["month"] = out[date_col].dt.to_period("M")
    return out[out["month"].between(current - (MONTHS - 1), current)]


def _month_label(period, current):
    return period.strftime("%b %Y") + (" (to date)" if period == current else "")


def _mode(s):
    return s.mode().iat[0] if s.notna().any() else NOT_RECORDED


def _agents_for_half(counts):
    """How many agents (largest first) it takes to reach half of the units."""
    c = counts.sort_values(ascending=False).cumsum() / counts.sum()
    return int((c < 0.5).sum() + 1)


def build_tables(df, current):
    t = {}

    # 1. Proportion -----------------------------------------------------------
    ov = df.groupby("unit_type").agg(accounts=("account_id", "size"), gross=("units", "sum"),
                                     refunded=("refunded_units", "sum")).reindex(UNIT_TYPES)
    ov.loc["Total"] = ov.sum()
    ov["share"] = ov["gross"] / ov.loc["Total", "gross"]
    ov["net"] = ov["gross"] - ov["refunded"]
    ov["refund_rate"] = ov["refunded"] / ov["gross"]
    t["overview"] = ov.rename_axis("Unit type").reset_index()
    bulk = df[df["units"] >= BULK_QTY]
    t["bulk"] = {"accounts": len(bulk), "units": int(bulk["units"].sum()), "max": int(df["units"].max()),
                 "refurb_units": int(bulk.loc[bulk["is_refurb"], "units"].sum()),
                 "multi_accounts": int((df["units"] > 1).sum())}
    rp = df[df["is_refurb"]].groupby("product")["units"].sum().sort_values(ascending=False)
    rp_tbl = pd.DataFrame({"Units": rp, "Share of refurb units": rp / rp.sum()}).head(TOP_N)
    if len(rp) > TOP_N:
        rp_tbl.loc["All others"] = [rp.iloc[TOP_N:].sum(), rp.iloc[TOP_N:].sum() / rp.sum()]
    t["refurb_products"] = rp_tbl.rename_axis("Product").reset_index()
    refunded = df[df["refunded"]]
    t["refund_dates"] = {"refunded": len(refunded), "with_refund_date": int(refunded["RefundDate"].notna().sum()),
                         "with_sale_date": int(refunded["sale_date"].notna().sum())}
    # Median list price per unit, refurb vs new, within Cash and PAYG (prices differ a lot between the two)
    priced = df[df["account_type"].isin(["Cash", "PAYG"]) & (df["Total_Price"] > 0)]
    t["price"] = (priced.assign(ppu=priced["Total_Price"] / priced["units"])
                  .groupby(["account_type", "unit_type"])["ppu"].median().unstack("unit_type")
                  .reindex(columns=UNIT_TYPES))

    # 2. Trends ---------------------------------------------------------------
    trend = df[df["gross_date"].dt.year >= MIN_TREND_YEAR].copy()
    trend["year"] = trend["gross_date"].dt.year
    t["yearly"] = _split(trend, "year").rename_axis("Year").reset_index()
    m = _split(_months(df, "gross_date", current), "month")
    m.index = [_month_label(p, current) for p in m.index]
    t["monthly"] = m.rename_axis("Month").reset_index()
    t["trend_excluded"] = int((df["gross_date"].dt.year < MIN_TREND_YEAR).sum())

    # 3. Selling agents -------------------------------------------------------
    agents = df.groupby("salesAgents").agg(name=("employee_name", _mode), role=("employeePrimaryRole", _mode),
                                           stl=("supervisor_name", _mode),
                                           refurb=("refurb_units", "sum"), total=("units", "sum"))
    agents["new"] = agents["total"] - agents["refurb"]
    agents["own_share"] = agents["refurb"] / agents["total"]
    R, NW, N = agents["refurb"].sum(), agents["new"].sum(), agents["total"].sum()
    agents["share_of_refurb"] = agents["refurb"] / R if R else np.nan
    top = agents.sort_values(["refurb", "total"], ascending=False).head(TOP_N)
    t["agents_top"] = top.reset_index()
    t["agent_stats"] = {
        "agents": len(agents),
        "with_refurb": int((agents["refurb"] > 0).sum()),
        "overall_share": R / N,
        "top_refurb_share": top["refurb"].sum() / R if R else np.nan,
        "top_total_share": top["total"].sum() / N,
        "top_new_sellers_share": agents["new"].nlargest(TOP_N).sum() / NW,
        "half_refurb": _agents_for_half(agents["refurb"]) if R else 0,
        "half_new": _agents_for_half(agents["new"]),
        # Do the top refurb sellers sit under the same STL?
        "top_stls": top["stl"].nunique(),
        "top_stl": top["stl"].mode().iat[0],
        "top_stl_agents": int(top["stl"].value_counts().iat[0]),
    }

    # 3b. Sales Team Leads (supervisor of the Selling Agent, as currently set in AMT) ---------------
    stl = df.groupby("supervisor_id").agg(name=("supervisor_name", _mode),
                                          refurb=("refurb_units", "sum"), total=("units", "sum"))
    stl["new"] = stl["total"] - stl["refurb"]
    stl["share"] = stl["refurb"] / stl["total"]
    stl["share_of_refurb"] = stl["refurb"] / R if R else np.nan
    # Largest single Selling Agent's part of the STL's refurb units — is one agent carrying the team?
    agent_refurb = df.groupby(["supervisor_id", "salesAgents"])["refurb_units"].sum()
    stl["top_agent_share"] = agent_refurb.groupby(level=0).max() / stl["refurb"].replace(0, np.nan)
    # Refurb share of units vs the country's, binomial test with a Bonferroni correction across tested STLs
    p0 = R / N
    tested = stl["total"] >= MIN_STL_UNITS
    n_tests = int(tested.sum())

    def _vs_country(row):
        if row["total"] < MIN_STL_UNITS:
            return "Too few units"
        p = min(1.0, binomtest(int(row["refurb"]), int(row["total"]), p0).pvalue * n_tests)
        if p >= 0.05:
            return "In line"
        return "Higher" if row["share"] > p0 else "Lower"

    stl["vs_country"] = stl.apply(_vs_country, axis=1)
    top_stl = stl.sort_values(["refurb", "total"], ascending=False).head(TOP_N)
    t["stl_top"] = top_stl.reset_index()
    # Rate view: teams large enough to test, ranked by refurb % of their own units
    t["stl_rate"] = stl[tested].sort_values(["share", "refurb"], ascending=False).head(TOP_N).reset_index()
    tst = stl[tested]
    t["stl_stats"] = {
        "stls": len(stl),
        "with_refurb": int((stl["refurb"] > 0).sum()),
        "top_refurb_share": top_stl["refurb"].sum() / R if R else np.nan,
        "top_total_share": top_stl["total"].sum() / N,
        "top_new_share": stl["new"].nlargest(TOP_N).sum() / NW,
        "half_refurb": _agents_for_half(stl["refurb"]) if R else 0,
        "half_new": _agents_for_half(stl["new"]),
        "tested": n_tests,
        "above": int((stl["vs_country"] == "Higher").sum()),
        "below": int((stl["vs_country"] == "Lower").sum()),
        "country_share": p0,
        "chi2_p": chi2_contingency(np.column_stack([tst["refurb"], tst["new"]]))[1] if n_tests > 1 else np.nan,
    }
    ch = _split(df, "Sales_channel").sort_values("Total", ascending=False)
    ch["Share of refurb units"] = ch[REFURB] / R if R else np.nan
    t["channels"] = ch.rename_axis("Sales channel").reset_index()

    # 5. Account type (Cash vs PAYG) ------------------------------------------
    at = _split(df, "account_type").sort_values("Total", ascending=False)
    at["Share of refurb units"] = at[REFURB] / R if R else np.nan
    t["account_type"] = at.rename_axis("Account type").reset_index()
    cp = at.reindex(["Cash", "PAYG"])[UNIT_TYPES].fillna(0)
    t["account_type_test"] = ({"p": chi2_contingency(cp.values)[1]}
                              if cp.values.min() > 0 else {"p": np.nan})
    ty = df[df["gross_date"].dt.year >= MIN_TREND_YEAR].assign(year=lambda x: x["gross_date"].dt.year)
    ty = ty[ty["account_type"].isin(["Cash", "PAYG"])]
    ay = _split(ty, ["year", "account_type"])["Refurb share"].unstack("account_type")
    ry = ty[ty["is_refurb"]].groupby("year")["units"].sum()
    ru = ty[ty["is_refurb"]].pivot_table(index="year", columns="account_type", values="units", aggfunc="sum")
    ru = ru.reindex(index=range(current.year, SALES_YEAR_FROM - 1, -1), columns=["Cash", "PAYG"]).fillna(0)
    t["refurb_units_by_year"] = ru.rename_axis("Sales Year").reset_index()
    t["refurb_units_by_year"].columns.name = None
    # Only years with enough refurb units for a share to mean something
    t["account_type_by_year"] = ay.loc[ry[ry >= 20].index].rename_axis("Year").reset_index()
    t["account_type_by_year"].columns.name = None

    # 6. Region ---------------------------------------------------------------
    rg = _split(df, "region")
    rg = pd.concat([rg.drop(NOT_RECORDED, errors="ignore").sort_values("Total", ascending=False),
                    rg.loc[[NOT_RECORDED]] if NOT_RECORDED in rg.index else None])
    rg["Share of refurb units"] = rg[REFURB] / R if R else np.nan
    t["region"] = rg.rename_axis("Region").reset_index()

    # 7. Customers with more than one refurb unit -----------------------------
    rc = df[df["is_refurb"]].groupby("customerId").agg(units=("units", "sum"), accounts=("account_id", "size"))
    new_customers = set(df.loc[~df["is_refurb"], "customerId"])
    bands = pd.cut(rc["units"], [0, 1, 2, 3, np.inf], labels=["1", "2", "3", "4+"])
    cb = rc.groupby(bands, observed=False).agg(Customers=("units", "size"), **{"Refurb units": ("units", "sum")})
    cb["Share of refurb customers"] = cb["Customers"] / len(rc)
    t["refurb_customers"] = cb.rename_axis("Refurb units per customer").reset_index()
    multi = rc[rc["units"] > 1]
    t["refurb_customer_stats"] = {
        "customers": len(rc),
        "multi": len(multi),
        "multi_units": int(multi["units"].sum()),
        "multi_accounts": int((multi["accounts"] > 1).sum()),  # bought on separate accounts
        "multi_qty": int((multi["accounts"] == 1).sum()),      # several units on one account
        "also_new": int(rc.index.isin(new_customers).sum()),
        "max_units": int(rc["units"].max()) if len(rc) else 0,
    }

    # 4. Sale -> dispatch (per account: one dispatch per account) -------------
    s = df[df["sale_date"].notna()].copy()
    s["days"] = (s["dispatchDate"].dt.normalize() - s["sale_date"].dt.normalize()).dt.days
    s["valid"] = s["days"].between(0, MAX_DISPATCH_DAYS)
    v = s[s["valid"]]
    d = pd.DataFrame({
        "sold": s.groupby("unit_type").size(),
        "with_dispatch": s.groupby("unit_type")["dispatchDate"].count(),
        "used": v.groupby("unit_type").size(),
        "mean": v.groupby("unit_type")["days"].mean(),
        "median": v.groupby("unit_type")["days"].median(),
        "within_7": v.groupby("unit_type")["days"].apply(lambda x: (x <= 7).mean()),
        "within_30": v.groupby("unit_type")["days"].apply(lambda x: (x <= 30).mean()),
    }).reindex(UNIT_TYPES)
    t["dispatch"] = d.rename_axis("Unit type").reset_index()
    pend = pd.DataFrame({
        "sold": s.groupby("unit_type")["units"].sum(),
        "pending": s[s["funnel_status"] == DISPATCH_PENDING].groupby("unit_type")["units"].sum(),
        "missing": s[s["funnel_status"] == DISPATCH_MISSING].groupby("unit_type")["units"].sum(),
    }).reindex(UNIT_TYPES).fillna(0)
    pend["pending_share"] = pend["pending"] / pend["sold"]
    pend["missing_share"] = pend["missing"] / pend["sold"]
    pend = pend[["sold", "pending", "pending_share", "missing", "missing_share"]]
    t["pending_dispatch"] = pend.rename_axis("Unit type").reset_index()
    t["dispatch_excluded"] = {"no_dispatch": int(s["dispatchDate"].isna().sum()),
                              "negative": int((s["days"] < 0).sum()),
                              "over_max": int((s["days"] > MAX_DISPATCH_DAYS).sum())}
    vm = _months(v, "sale_date", current)
    dm = vm.pivot_table(index="month", columns="unit_type", values="days", aggfunc=["mean", "size"])
    dm = pd.DataFrame({
        f"{REFURB} avg days": dm.get(("mean", REFURB)), f"{REFURB} accounts": dm.get(("size", REFURB)),
        f"{NEW} avg days": dm.get(("mean", NEW)), f"{NEW} accounts": dm.get(("size", NEW)),
    })
    sm = _months(s, "sale_date", current)
    dm["Dispatch date recorded"] = sm.groupby("month")["dispatchDate"].apply(lambda x: x.notna().mean())
    dm.index = [_month_label(p, current) for p in dm.index]
    t["dispatch_monthly"] = dm.rename_axis("Month of sale").reset_index()
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
    df = prepare(data["sales_and_refunds_accounts"], data["products_refurb"], args.country)
    tables = build_tables(df, pd.Period(snapshot_at, "M"))

    meta = {
        "snapshot": stamp,
        "snapshot_at": snapshot_at,
        "generated_at": datetime.now(),
        "country": args.country.title(),
        # Period covered: first sale in the report up to the snapshot
        "period": (df.loc[df["gross_date"].dt.year >= MIN_TREND_YEAR, "gross_date"].min(), snapshot_at),
        "trend_from": MIN_TREND_YEAR, "max_dispatch_days": MAX_DISPATCH_DAYS, "top_n": TOP_N, "bulk_qty": BULK_QTY,
        "min_stl_units": MIN_STL_UNITS,
    }
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_stamp = meta["generated_at"].strftime("%Y%m%d%H%M")
    # Every run is a new file so drafts can be compared; never overwrite an earlier one
    base_name = f"refurbs_vs_new_review_{args.country}_{out_stamp}"
    out_path = os.path.join(OUTPUT_DIR, f"{base_name}.md")
    n = 2
    while os.path.exists(out_path):
        out_path = os.path.join(OUTPUT_DIR, f"{base_name}_{n}.md")
        n += 1
    generator.generate(tables, meta, out_path)
    logger.info("Report written to %s", out_path)


if __name__ == "__main__":
    main()
