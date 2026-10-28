"""
Renders the tables computed by builder.py into a short review (one per country).

The report content (headings, bullets, tables, wording) is assembled once by
build_report(); render_markdown() only lays it out, so a Word renderer can be
added later (as in 01_sales_bootcamp) once the content is agreed.

Narrative sentences are generated from the numbers, so re-running on a newer
snapshot updates the text as well as the tables. Review the wording before sharing.
"""
import os
import sys

import pandas as pd

# Shared report sections (header, methodology) — analysis/_templates/report_sections.py
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "_templates"))
import report_sections  # noqa: E402

F0 ="{:,.0f}".format
F1 = "{:,.1f}".format
PCT = "{:.0%}".format
PCT1 = "{:.1%}".format

REFURB, NEW = "Refurb", "New"


# ---------------------------------------------------------------------------
# Content blocks
# ---------------------------------------------------------------------------
def _fmt(value, fmt):
    if value is None or (isinstance(value, float) and pd.isna(value)) or value is pd.NA:
        return "–"
    return fmt(value) if fmt else str(value)


def title(text):
    return {"type": "title", "text": text}


def h2(text):
    return {"type": "h2", "text": text}


def h3(text):
    return {"type": "h3", "text": text}


def bullet(lead, text):
    return {"type": "bullet", "lead": lead, "text": text}


def para(lead, text):
    return {"type": "para", "lead": lead, "text": text}


def text(body):
    return {"type": "text", "text": body}


def note(text):
    return {"type": "note", "text": text}


def table(df, formats=None, note=None, header=True, text_cols=1, title=None):
    """A table with every value already formatted as text; the first `text_cols` columns are left-aligned.
    Titled tables are numbered 'Table 1: …', 'Table 2: …' in the order they appear in the report."""
    formats = formats or {}
    cells = [[_fmt(row[c], formats.get(c)) for c in df.columns] for _, row in df.iterrows()]
    return {"type": "table", "columns": [str(c) for c in df.columns], "rows": cells,
            "note": note, "header": header, "text_cols": text_cols, "title": title}


# ---------------------------------------------------------------------------
# Column naming — every share says what it is a share of
# ---------------------------------------------------------------------------
def _pct_of(group):
    """Refurb units as a % of the group's own units, e.g. 'Refurb % of region's units'."""
    return f"Refurb % of {group} units"


def _of_all(country):
    """The group's part of all refurb units in the country."""
    return f"% of all {country} refurb units"


def _split_table(df, group, country, **kw):
    """A refurb / new / total table with plainly named share columns."""
    out = df.rename(columns={"Refurb share": _pct_of(group), "Share of refurb units": _of_all(country)})
    fmts = {REFURB: F0, NEW: F0, "Total": F0, _pct_of(group): PCT1, _of_all(country): PCT1}
    return table(out, formats=fmts, **kw)


def _compare(a, b, unit, lower="lower", higher="higher"):
    """'x lower/higher than' wording for refurb (a) vs new (b)."""
    if pd.isna(a) or pd.isna(b) or round(a, 1) == round(b, 1):
        return "about the same as new units"
    return f"{F1(abs(a - b))} {unit} {lower if a < b else higher} than new units"


def _pending_compare(r, n):
    """Small backlogs can differ a lot in proportion, so compare the ratio, not the gap in points."""
    if n == 0 or abs(r / n - 1) < 0.5:
        return "a similar, small backlog for both."
    times = r / n
    return (f"small for both, but proportionally {'higher' if times > 1 else 'lower'} for refurb units "
            f"({F1(times if times > 1 else 1 / times)}x), which may be worth following up with operations.")


def _p(p):
    return "p < 0.001" if p < 0.001 else f"p = {p:.3f}"


def _refund_takeaway(r, n, price):
    out = f"{PCT1(r['refund_rate'])} of refurb units are refunded, vs {PCT1(n['refund_rate'])} of new units. "
    cheaper = [a for a in ["Cash", "PAYG"] if a in price.index and price.loc[a, REFURB] < price.loc[a, NEW]]
    if not cheaper:
        return out
    out += ("Refurb units are priced lower than new units (median price per unit: " +
            "; ".join(f"{a} {F0(price.loc[a, REFURB])} vs {F0(price.loc[a, NEW])}" for a in cheaper) + "). ")
    if r["refund_rate"] > n["refund_rate"]:
        out += ("If price were the main reason customers ask for a refund, the cheaper refurb units would be "
                "expected to be refunded less often, not more — which may suggest other factors (e.g. product "
                "condition or customer expectations) play a part. Refund reasons would help confirm this.")
    return out


def _account_type_takeaway(at, test, by_year, group):
    if not {"Cash", "PAYG"} <= set(at.index):
        return "Only one account type has unit sales, so there is no Cash vs PAYG comparison."
    c, p = at.loc["Cash"], at.loc["PAYG"]
    hi, lo = ("Cash", "PAYG") if c[group] > p[group] else ("PAYG", "Cash")
    out = f"Refurb units make up {PCT1(c[group])} of Cash unit sales vs {PCT1(p[group])} of PAYG unit sales. "
    if pd.notna(test["p"]):
        out += (f"The difference is statistically significant (chi-square, {_p(test['p'])}). " if test["p"] < 0.05
                else f"The difference is not statistically significant (chi-square, {_p(test['p'])}). ")
    if len(by_year):
        exceptions = by_year.index[by_year[hi] <= by_year[lo]].astype(int).tolist()
        years = " and ".join([", ".join(map(str, exceptions[:-1])), str(exceptions[-1])] if len(exceptions) > 1
                             else map(str, exceptions))
        out += (f"Year by year, {hi} has the higher refurb % in {len(by_year) - len(exceptions)} of the "
                f"{len(by_year)} years with refurb sales" +
                (", so the gap holds within years and does not only come from when refurb stock was sold. "
                 if not exceptions else
                 f" — the exception{'s are' if len(exceptions) > 1 else ' is'} {years}, when {lo} had the higher "
                 "refurb %. "))
    return out + ("This may point to refurb units being offered or priced differently for Cash and PAYG buyers — "
                  "worth checking with the commercial team.")


def _stl_blocks(t, meta, country):
    """Section 3b — refurb sales by Sales Team Lead (the Selling Agent's supervisor)."""
    s = t["stl_stats"]
    team_pct, top_agent, vs = _pct_of("team's"), "Top agent's % of team's refurb units", f"vs {country} average"
    cols = {"name": "STL", "refurb": REFURB, "new": NEW, "share": team_pct, "share_of_refurb": _of_all(country),
            "top_agent_share": top_agent, "vs_country": vs}
    fmts = {REFURB: F0, NEW: F0, team_pct: PCT1, _of_all(country): PCT1, top_agent: PCT}
    top = t["stl_top"].rename(columns=cols)[list(cols.values())]
    rate = t["stl_rate"].rename(columns=cols)[list(cols.values())]
    concentrated = s["top_refurb_share"] > 1.5 * s["top_new_share"]
    led = top.iloc[0]
    out = [
        h3("Section 3b: By Sales Team Lead (STL)"),
        text("Refurb sales concentrated among a few Selling Agents could still come from one team, so this view "
             "groups Selling Agents under their STL. Table {volume} ranks teams by how many refurb units they sell "
             f"(volume); Table {{rate}} ranks teams by how much of their own sales are refurb (rate). A large team "
             "can lead on volume with an average rate, and a small team can sell few refurb units but a high "
             "proportion."),
        table(top, formats=fmts, text_cols=1, title=f"Top {meta['top_n']} STLs by refurb units sold (volume)",
              note=f"Refurb / New = units sold by the team. {team_pct} = refurb units ÷ all the team's units. "
                   f"{_of_all(country)} = the team's part of every refurb unit sold in {country}. {top_agent} = how "
                   f"much of the team's refurb units come from its single biggest refurb seller. {vs} = whether the "
                   f"team's refurb % ({country} average: {PCT1(s['country_share'])}) is higher or lower by more than "
                   f"chance would explain (binomial test, 5% level, Bonferroni-corrected across the {s['tested']} "
                   f"teams with {meta['min_stl_units']}+ units)."),
        para("Takeaway:", f"{F0(s['with_refurb'])} of {F0(s['stls'])} STLs have at least one refurb sale in their "
                          f"team. The top {meta['top_n']} STLs account for {PCT(s['top_refurb_share'])} of refurb "
                          f"units and {PCT(s['top_total_share'])} of all gross unit sales; the top {meta['top_n']} by new "
                          f"units account for {PCT(s['top_new_share'])} of new units. Half of refurb units come from "
                          f"{F0(s['half_refurb'])} STLs' teams, vs {F0(s['half_new'])} for new units. "
                          + ("This may suggest refurb sales are more concentrated in specific teams than new sales. "
                             if concentrated else
                             "By this measure, refurb sales are about as spread across teams as new sales. ")
                          + f"{led['STL']}'s team sells the most refurb units: {F0(led[REFURB])}, "
                            f"{PCT1(led[team_pct])} of the team's units."),
    ]
    sig = (f"Refurb % does differ between teams (chi-square across the {s['tested']} STLs with "
           f"{meta['min_stl_units']}+ units, {_p(s['chi2_p'])}). " if pd.notna(s["chi2_p"]) and s["chi2_p"] < 0.05
           else "Refurb % does not differ significantly between teams. ")
    sig += (f"{s['above']} teams sell a significantly higher refurb % than the {country} average "
            f"({PCT1(s['country_share'])}) and {s['below']} a significantly lower one; the rest are in line. "
            f"The highest rate is {rate.iloc[0]['STL']}'s team at {PCT1(rate.iloc[0][team_pct])}.")
    out += [table(rate, formats=fmts, text_cols=1,
                  title=f"Top {meta['top_n']} STLs by refurb % of team's units (rate)",
                  note=f"Teams with {meta['min_stl_units']}+ units, ranked by {team_pct}. Columns as in Table "
                       "{volume}."),
            para("Takeaway:", sig + " A higher refurb % may reflect the team's region, channel or access to refurb "
                                    "stock as much as how the team sells — worth checking before drawing conclusions "
                                    "about individual STLs.")]
    return out


# ---------------------------------------------------------------------------
# Report content — edit wording / structure here
# ---------------------------------------------------------------------------
def build_report(t, meta):
    country = meta["country"]
    ov = t["overview"].set_index("Unit type")
    r, n, tot = ov.loc[REFURB], ov.loc[NEW], ov.loc["Total"]
    yearly = t["yearly"]
    big_years = yearly[yearly["Total"] >= 1000]
    peak = big_years.loc[big_years["Refurb share"].idxmax()]
    monthly = t["monthly"]
    last12 = monthly.iloc[:-1]  # full months only (the last row is the current month to date)
    last12_share = last12[REFURB].sum() / last12["Total"].sum()
    ag = t["agent_stats"]
    d = t["dispatch"].set_index("Unit type")
    dr, dn = d.loc[REFURB], d.loc[NEW]
    pdisp = t["pending_dispatch"].set_index("Unit type")
    pd_r, pd_n = pdisp.loc[REFURB], pdisp.loc[NEW]
    at_ix = t["account_type"].set_index("Account type")
    cs = t["refurb_customer_stats"]
    bk = t["bulk"]
    rd = t["refund_dates"]
    of_all = _of_all(country)

    blocks = [title(f"Refurbished vs New Units — {country}")]

    # Highlights ---------------------------------------------------------------
    hi = [
        bullet("Proportion.", f"{F0(r['gross'])} ({PCT1(r['share'])}) of {F0(tot['gross'])} gross unit sales are "
                              "refurb units."),
        bullet("Trend.", f"The refurb % was highest in {int(peak['Year'])}: {F0(peak[REFURB])} "
                         f"({PCT1(peak['Refurb share'])}) of {F0(peak['Total'])} units; over the last 12 full months "
                         f"it is {PCT1(last12_share)}."),
        bullet("Refunds.", f"{PCT1(r['refund_rate'])} of refurb units are refunded vs {PCT1(n['refund_rate'])} of new "
                           "units."),
        bullet("Selling Agents.", f"{F0(ag['with_refurb'])} ({PCT1(ag['with_refurb'] / ag['agents'])}) of "
                                  f"{F0(ag['agents'])} Selling Agents have sold at least one refurb unit. The top "
                                  f"{meta['top_n']} account for {PCT(ag['top_refurb_share'])} "
                                  f"of refurb units, against {PCT(ag['top_total_share'])} of all gross unit sales."),
        bullet("Days from sale to dispatch.", f"Refurb: mean {F1(dr['mean'])} · median {F0(dr['median'])} days. "
                                              f"New: mean {F1(dn['mean'])} · median {F0(dn['median'])} days. Based on "
                                              f"{F0(dr['used'])} refurb and {F0(dn['used'])} new-unit accounts "
                                              "dispatched on or after the sale date (within "
                                              f"{meta['max_dispatch_days']} days)."),
        bullet("Pending dispatch.", f"{F0(pd_r['pending'])} ({PCT1(pd_r['pending_share'])}) of {F0(pd_r['sold'])} "
                                    f"sold refurb units are pending dispatch, vs {F0(pd_n['pending'])} "
                                    f"({PCT1(pd_n['pending_share'])}) of {F0(pd_n['sold'])} sold new units."),
    ]
    if {"Cash", "PAYG"} <= set(at_ix.index):
        hi.append(bullet("Cash vs PAYG.", f"Refurb units make up {PCT1(at_ix.loc['Cash', 'Refurb share'])} of Cash "
                                          f"unit sales vs {PCT1(at_ix.loc['PAYG', 'Refurb share'])} of PAYG unit "
                                          "sales."))
    hi.append(bullet("Repeat refurb customers.", f"{F0(cs['multi'])} ({PCT1(cs['multi'] / cs['customers'])}) of "
                                                 f"{F0(cs['customers'])} customers with a refurb unit have more than "
                                                 "one."))
    blocks += [h2("Key Highlights"), *hi]

    # 1. Proportion ------------------------------------------------------------
    ov_tbl = t["overview"].rename(columns={"accounts": "Accounts", "gross": "Gross unit sales",
                                           "share": "% of gross unit sales", "refunded": "Refunded units",
                                           "net": "Net unit sales", "refund_rate": "Refund rate"})
    ov_tbl = ov_tbl[["Unit type", "Accounts", "Gross unit sales", "% of gross unit sales", "Refunded units",
                     "Net unit sales", "Refund rate"]]
    prods = t["refurb_products"].rename(columns={"Share of refurb units": of_all})
    top_prod = prods.iloc[0]
    blocks += [
        h2("Section 1: Proportion of Refurb vs New Unit Sales"),
        table(ov_tbl, formats={"Accounts": F0, "Gross unit sales": F0, "% of gross unit sales": PCT1,
                               "Refunded units": F0, "Net unit sales": F0, "Refund rate": PCT1},
              title="Gross unit sales — refurb vs new",
              note="Gross unit sales = units on sold + refunded accounts. Net = gross less refunded units. Refund "
                   f"rate = refunded units ÷ gross unit sales. {F0(bk['multi_accounts'])} accounts carry more than "
                   f"one unit; {F0(bk['accounts'])} bulk orders of {meta['bulk_qty']}+ units (up to {F0(bk['max'])} "
                   f"on one account) add {F0(bk['units'])} units, {F0(bk['refurb_units'])} of them refurb."),
        para("Takeaway:", f"Refurb units make up {PCT1(r['share'])} of gross unit sales. "
                          + _refund_takeaway(r, n, t["price"])),
        table(prods, formats={"Units": F0, of_all: PCT1}, title="Refurb units by product",
              note=f"Top {meta['top_n']} products among refurb units."),
        para("Takeaway:", f"{top_prod['Product']} is the most common refurb product: {F0(top_prod['Units'])} "
                          f"({PCT(top_prod[of_all])}) of all refurb units."),
    ]

    # 2. Trends ----------------------------------------------------------------
    m_full = last12.set_index("Month")
    blocks += [
        h2("Section 2: Yearly and Monthly Trend"),
        _split_table(yearly.astype({"Year": str}), "the year's", country,
                     title="Gross unit sales by year — refurb vs new",
                     note=f"Year of sale. Refunded accounts are dated by RefundDate where recorded "
                          f"({F0(rd['with_refund_date'])} of {F0(rd['refunded'])}), otherwise by the date the account "
                          f"was created. From {meta['trend_from']}; {F0(t['trend_excluded'])} accounts with "
                          "placeholder dates before that are left out."),
        _split_table(monthly, "the month's", country, title="Gross unit sales by month — refurb vs new",
                     note="Last 12 full months plus the current month to date."),
        para("Takeaway:", f"Among years with 1,000+ unit sales, {int(peak['Year'])} has the highest refurb % "
                          f"({PCT1(peak['Refurb share'])}). Over the last 12 full months refurb units make up "
                          f"{PCT1(last12_share)} of gross unit sales, ranging from {F0(m_full[REFURB].min())} to "
                          f"{F0(m_full[REFURB].max())} a month (highest in {m_full[REFURB].idxmax()}). Year-to-year "
                          "changes may reflect refurb stock availability and pricing as well as demand — worth "
                          "checking against stock records before reading them as a demand trend."),
    ]

    # 3. Selling agents --------------------------------------------------------
    agent_pct = _pct_of("agent's")
    at = t["agents_top"].rename(columns={"name": "Selling Agent", "role": "Role", "stl": "STL", "refurb": REFURB,
                                         "new": NEW, "total": "Total", "own_share": agent_pct,
                                         "share_of_refurb": of_all})
    at = at[["Selling Agent", "Role", "STL", REFURB, NEW, "Total", agent_pct, of_all]]
    concentrated = ag["top_refurb_share"] > 1.5 * ag["top_new_sellers_share"]
    ch = t["channels"]
    ch_main = ch[ch["Total"] >= 500]
    ch_hi = ch_main.loc[ch_main["Refurb share"].idxmax()] if len(ch_main) else None
    blocks += [
        h2("Section 3: Selling Agent Performance"),
        table(at, formats={REFURB: F0, NEW: F0, "Total": F0, agent_pct: PCT1, of_all: PCT1}, text_cols=3,
              title=f"Top {meta['top_n']} Selling Agents by refurb units",
              note=f"{agent_pct} = refurb units ÷ the agent's total unit sales ({country} overall: "
                   f"{PCT1(ag['overall_share'])}). Role and STL = current primary role and supervisor in AMT."),
        para("Takeaway:", f"The top {meta['top_n']} Selling Agents by refurb units account for "
                          f"{PCT(ag['top_refurb_share'])} of all refurb units but {PCT(ag['top_total_share'])} of all "
                          f"gross unit sales. For comparison, the top {meta['top_n']} by new units account for "
                          f"{PCT(ag['top_new_sellers_share'])} of new units. Half of refurb units come from "
                          f"{F0(ag['half_refurb'])} Selling Agents, vs {F0(ag['half_new'])} for new units. "
                          + ("This may suggest refurb sales are more concentrated among specific Selling Agents than "
                             "new unit sales. "
                             if concentrated else
                             "Refurb sales look about as spread out across Selling Agents as new unit sales. ")
                          + "Part of this may also come from when refurb stock was available — agents active in "
                            "the peak refurb years would naturally sell more of it. "
                          + (f"The top {meta['top_n']} Selling Agents sit under {ag['top_stls']} different STLs"
                             + (f" ({ag['top_stl_agents']} under {ag['top_stl']})." if ag["top_stl_agents"] > 1 else ".")
                             )),
    ]
    blocks += _stl_blocks(t, meta, country)
    blocks.append(_split_table(ch, "channel's", country, title="Refurb vs new units by sales channel",
                               note="By sales channel on the account."))
    if ch_hi is not None:
        blocks.append(para("Takeaway:", f"Among the main channels, {ch_hi['Sales channel']} has the highest refurb % "
                                        f"({PCT1(ch_hi['Refurb share'])} of its unit sales)."))

    # 4. Dispatch --------------------------------------------------------------
    ex = t["dispatch_excluded"]
    d_tbl = t["dispatch"].rename(columns={"sold": "Accounts sold", "with_dispatch": "With dispatch date",
                                          "used": "Used", "mean": "Avg days", "median": "Median days",
                                          "within_7": "Within 7 days", "within_30": "Within 30 days"})
    dm = t["dispatch_monthly"]
    low_cov = dm.loc[dm["Dispatch date recorded"] < 0.5, "Month of sale"].tolist()
    both = dm.dropna(subset=[f"{REFURB} avg days", f"{NEW} avg days"])
    faster = int((both[f"{REFURB} avg days"] < both[f"{NEW} avg days"]).sum())
    monthly_note = (f" Month by month, refurb accounts have the lower average in {faster} of the {len(both)} months "
                    "with both refurb and new dispatches in Table {dispatch_monthly}, though several of those months "
                    "rest on few refurb accounts." if len(both) else "")
    cov_note = (f" From {low_cov[0]}, fewer than half of sold accounts have a dispatch date recorded "
                f"({len(low_cov)} of the {len(dm)} months shown), so recent monthly averages rest on few accounts and "
                "the overall comparison mostly reflects earlier sales — worth checking with operations whether "
                "dispatch dates are still being captured." if low_cov else "")
    if dr["median"] == dn["median"]:
        typical = (f"A typical (median) account is dispatched {F0(dr['median'])} days after sale for both refurb and "
                   f"new units; the refurb average is {_compare(dr['mean'], dn['mean'], 'days')}, which comes from a "
                   "longer tail of slow dispatches rather than the typical refurb unit.")
    else:
        typical = (f"A typical (median) refurb account is dispatched {F0(dr['median'])} days after sale vs "
                   f"{F0(dn['median'])} for new units, and the refurb average is "
                   f"{_compare(dr['mean'], dn['mean'], 'days', 'shorter', 'longer')}.")
    blocks += [
        h2("Section 4: Days from Sale to Dispatch"),
        table(d_tbl, formats={"Accounts sold": F0, "With dispatch date": F0, "Used": F0, "Avg days": F1,
                              "Median days": F0, "Within 7 days": PCT, "Within 30 days": PCT},
              title="Days from sale to dispatch — refurb vs new",
              note=f"Days = dispatchDate − sale_date, per sold account (one dispatch per account, whatever its "
                   f"quantity). 'Used' = gaps of 0–{meta['max_dispatch_days']} days; left out: "
                   f"{F0(ex['no_dispatch'])} with no dispatch date, {F0(ex['negative'])} dispatched before the sale "
                   f"date and {F0(ex['over_max'])} over {meta['max_dispatch_days']} days. Within 7 / 30 days = share "
                   "of used accounts."),
        table(dm, formats={**{c: (F1 if "days" in c else F0) for c in dm.columns[1:]}, "Dispatch date recorded": PCT},
              title="Average days from sale to dispatch by month of sale",
              note="Average days to dispatch by month of sale (accounts with a 0–"
                   f"{meta['max_dispatch_days']} day gap). Dispatch date recorded = share of all accounts sold that "
                   "month with a dispatchDate."),
        para("Takeaway:", f"{typical} {PCT(dr['within_7'])} of refurb accounts go out within a week vs "
                          f"{PCT(dn['within_7'])} of new-unit accounts.{monthly_note} {F0(ex['negative'])} accounts "
                          "have a dispatch date before the sale date, which may point to how sale_date is set for some "
                          "accounts." + cov_note),
        table(title="Sold units pending dispatch — refurb vs new", df=t["pending_dispatch"].rename(columns={
                  "sold": "Units sold", "pending": "Dispatch Pending", "pending_share": "% pending",
                  "missing": "Dispatch Missing", "missing_share": "% missing"}),
              formats={"Units sold": F0, "Dispatch Pending": F0, "% pending": PCT1, "Dispatch Missing": F0,
                       "% missing": PCT1},
              note="Sold units by funnel_status today. Dispatch Pending = waiting to be dispatched (mostly Full "
                   "Deposit accounts). Dispatch Missing = no dispatch on record, mostly older or fully paid "
                   "accounts, so more likely a records gap than units still waiting."),
        para("Takeaway:", f"{F0(pd_r['pending'])} ({PCT1(pd_r['pending_share'])}) of sold refurb units are pending "
                          f"dispatch, vs {F0(pd_n['pending'])} ({PCT1(pd_n['pending_share'])}) of sold new units — "
                          + _pending_compare(pd_r["pending_share"], pd_n["pending_share"])),
    ]

    # 5. Account type ----------------------------------------------------------
    by_year = t["account_type_by_year"]
    at_pct = _pct_of("account type's")
    blocks += [
        h2("Section 5: Cash vs PAYG Accounts"),
        _split_table(t["account_type"], "account type's", country, title="Refurb vs new units by account type",
                     note=f"By accountType. {at_pct} = refurb units ÷ all units of that account type. {of_all} = the "
                          f"account type's part of every refurb unit sold in {country}."),
    ]
    ru = t["refurb_units_by_year"].rename(columns={"Cash": "Refurb Units (Cash)", "PAYG": "Refurb Units (PAYG)"})
    blocks.append(table(ru.astype({"Sales Year": str}),
                        formats={"Refurb Units (Cash)": F0, "Refurb Units (PAYG)": F0},
                        title="Refurb units by sales year — Cash vs PAYG",
                        note="Refurb units (sum of productQty) by year of sale and account type (refunded accounts: "
                             "year of RefundDate, or of account creation where it is empty)."))
    if len(by_year):
        blocks.append(table(by_year.astype({"Year": str}),
                            formats={c: PCT1 for c in by_year.columns if c != "Year"},
                            title="Refurb % of units by year — Cash vs PAYG",
                            note="Refurb units ÷ all units, by year of sale and account type (years with 20+ refurb "
                                 "units)."))
    blocks.append(para("Takeaway:", _account_type_takeaway(at_ix, t["account_type_test"],
                                                           by_year.set_index("Year") if len(by_year) else by_year,
                                                           "Refurb share")))

    # 6. Region ----------------------------------------------------------------
    rg = t["region"]
    rg_main = rg[(rg["Region"] != "Not recorded") & (rg["Total"] >= 1000)]
    rg_hi, rg_lo = rg_main.loc[rg_main["Refurb share"].idxmax()], rg_main.loc[rg_main["Refurb share"].idxmin()]
    rg_top = rg.loc[rg[REFURB].idxmax()]
    blocks += [
        h2("Section 6: Refurb vs New by Region"),
        _split_table(rg, "region's", country, title="Refurb vs new units by region",
                     note="By the customer's region on the account. Not recorded = no region captured."),
        para("Takeaway:", f"{rg_top['Region']} has the most refurb units: {F0(rg_top[REFURB])} "
                          f"({PCT(rg_top['Share of refurb units'])}) of all refurb units. Among regions with 1,000+ "
                          f"unit sales, the refurb % ranges from {PCT1(rg_lo['Refurb share'])} in {rg_lo['Region']} "
                          f"to {PCT1(rg_hi['Refurb share'])} in {rg_hi['Region']}. Differences may reflect where refurb "
                          "stock was available and which channels sell in each region, as well as customer demand."),
    ]

    # 7. Customers with more than one refurb unit ------------------------------
    blocks += [
        h2("Section 7: Customers with More Than One Refurb Unit"),
        table(t["refurb_customers"].rename(columns={"Share of refurb customers": "% of refurb customers"}),
              formats={"Customers": F0, "Refurb units": F0, "% of refurb customers": PCT1},
              title="Customers by number of refurb units",
              note="Customers (customerId) by the number of refurb units across all their accounts."),
        para("Takeaway:", f"{F0(cs['customers'])} customers have bought at least one refurb unit; {F0(cs['multi'])} "
                          f"({PCT1(cs['multi'] / cs['customers'])}) of them have more than one, {F0(cs['multi_units'])} "
                          f"refurb units in total (up to {F0(cs['max_units'])} for one customer). Of these, "
                          f"{F0(cs['multi_accounts'])} bought on separate accounts and {F0(cs['multi_qty'])} bought "
                          f"several units on a single account. {F0(cs['also_new'])} "
                          f"({PCT1(cs['also_new'] / cs['customers'])}) refurb customers have also bought a new unit."),
    ]

    blocks += report_sections.methodology([
        ("Scope", f"Gross unit sales, {country}: accounts with a sale date plus refunded accounts (category = Unit; "
                  "latest marts.mart_accounts record per account; test customers excluded)"),
        ("Units", "sum of productQty (a Cash account can carry several units)"),
        ("Refurb units", "products with isRefurb = 1 in amt.products (joined on productId); every other unit is New"),
        ("Dates", "year / month of sale_date; refunded accounts by RefundDate, else the date the account was created"),
        ("People", "Selling Agent = salesAgents on the account, named from employee_name; STL = the Selling Agent's "
                   "current supervisor in AMT"),
        ("Significance tests", "chi-square and binomial tests on units, 5% level"),
        ("Data", f"ClickHouse snapshot {meta['snapshot_at']:%d %b %Y %H:%M} ({meta['snapshot']})"),
    ])
    blocks += report_sections.report_details(date=meta["generated_at"], period=meta["period"])
    return _resolve_table_refs(blocks)


# placeholder -> how to recognise the table it points at (by title)
TABLE_REFS = {
    "volume": lambda title: title.startswith("Top") and "(volume)" in title,
    "rate": lambda title: title.startswith("Top") and "(rate)" in title,
    "dispatch_monthly": lambda title: title.startswith("Average days from sale to dispatch"),
}


def _resolve_table_refs(blocks):
    """Replace '{volume}'-style placeholders in text and table notes with the referenced table's number."""
    refs, n = {}, 0
    for b in blocks:
        if b["type"] == "table" and b.get("title"):
            n += 1
            for key, matches in TABLE_REFS.items():
                if key not in refs and matches(b["title"]):
                    refs[key] = n
    for b in blocks:
        field = "note" if b["type"] == "table" else "text"
        if b.get(field):
            for key, num in refs.items():
                b[field] = b[field].replace("{" + key + "}", str(num))
    return blocks


# ---------------------------------------------------------------------------
# Markdown renderer (drafting / review)
# ---------------------------------------------------------------------------
def _md_cell(text):
    return str(text).replace("|", "\\|")


def render_markdown(blocks, out_path):
    lines = []
    n_tables = 0
    for b in blocks:
        kind = b["type"]
        if kind != "bullet" and lines and lines[-1] != "":
            lines.append("")  # blank line before anything that isn't continuing a list
        if kind == "title":
            lines += [f"# {b['text']}", ""]
        elif kind == "h2":
            lines += [f"## {b['text']}", ""]
        elif kind == "h3":
            lines += [f"### {b['text']}", ""]
        elif kind == "bullet":
            lines.append(f"- **{b['lead']}** {b['text']}")
        elif kind == "para":
            lines += [f"**{b['lead']}** {b['text']}", ""]
        elif kind == "text":
            lines += [b["text"], ""]
        elif kind == "note":
            lines += [f"*{b['text']}*", ""]
        elif kind == "table":
            if not b["header"]:  # key/value table -> bold labels
                lines += [f"**{r[0]}:** {' · '.join(r[1:])}  " for r in b["rows"]] + [""]
                continue
            if b["title"]:
                n_tables += 1
                lines += [f"**Table {n_tables}: {b['title']}**", ""]
            lines.append("| " + " | ".join(_md_cell(c) for c in b["columns"]) + " |")
            lines.append("|" + "|".join(":---" if i < b["text_cols"] else "---:" for i in range(len(b["columns"]))) + "|")
            for r in b["rows"]:
                lines.append("| " + " | ".join(_md_cell(c) for c in r) + " |")
            lines.append("")
            if b["note"]:
                lines += [f"*{b['note']}*", ""]
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines).rstrip() + "\n")
    return out_path


def generate(t, meta, out_path):
    return render_markdown(build_report(t, meta), out_path)
