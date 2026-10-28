"""
Renders the tables computed by builder.py into the Battery vs Direct review (one per country).

Same structure as ../analysis/generator.py: build_report() assembles the content once and
render_markdown() lays it out. Narrative sentences are generated from the numbers, so re-running
on a newer snapshot updates the text as well as the tables. Review the wording before sharing.
"""
import os
import sys

import pandas as pd
from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, RGBColor

# Shared report sections (header, methodology) and Word fonts — analysis/_templates/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "_templates"))
import docx_styles  # noqa: E402
import report_sections  # noqa: E402

F0 ="{:,.0f}".format
F1 = "{:,.1f}".format
PCT = "{:.0%}".format
PCT1 = "{:.1%}".format

# Word styling (same palette as 01_sales_bootcamp)
HEADER_FILL = "1F3864"
ALT_FILL = "F5F8FC"
MUTED = RGBColor(0x59, 0x59, 0x59)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
PAGE_WIDTH_CM = 17.2

REFURB, NEW = "Refurb", "New"
GROUPS = ["Battery", "Direct"]
SEGMENTS = [f"{u} {g}" for g in GROUPS for u in (NEW, REFURB)]


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


def table(df, formats=None, note=None, header=True, text_cols=1, title=None, group=None):
    """A table with every value already formatted as text; the first `text_cols` columns are left-aligned.
    Titled tables are numbered 'Table 1: …', 'Table 2: …' in the order they appear in the report;
    consecutive tables with the same `group` share a number as 'Table 1.1', 'Table 1.2', …"""
    formats = formats or {}
    cells = [[_fmt(row[c], formats.get(c)) for c in df.columns] for _, row in df.iterrows()]
    return {"type": "table", "columns": [str(c) for c in df.columns], "rows": cells,
            "note": note, "header": header, "text_cols": text_cols, "title": title, "group": group}


# ---------------------------------------------------------------------------
# Column naming and shared wording
# ---------------------------------------------------------------------------
def _pct(grp):
    """Refurb units as a % of the row's Battery (or Direct) units."""
    return f"Refurb % of {grp} units"


SEG_NOTE = ("New / Refurb Battery / Direct = units sold. Refurb % of Battery units = refurb Battery units ÷ all "
            "Battery units in that row (likewise for Direct).")


def _seg_table(df, first, **kw):
    """Segment split table: New and Refurb units per product type plus the refurb % within each type."""
    cols = [first] + [c for g in GROUPS for c in (f"{NEW} {g}", f"{REFURB} {g}", f"Refurb % of {g}")]
    out = df[cols].rename(columns={f"Refurb % of {g}": _pct(g) for g in GROUPS})
    fmts = {**{s: F0 for s in SEGMENTS}, **{_pct(g): PCT1 for g in GROUPS}}
    return table(out, formats=fmts, **kw)


def _p(p):
    return "p < 0.001" if p < 0.001 else f"p = {p:.3f}"


def _vs(a, b, more="higher", less="lower"):
    """'higher' / 'lower' / 'about the same' for refurb (a) vs new (b), comparing percentages to 0.1 points."""
    if pd.isna(a) or pd.isna(b) or round(a, 3) == round(b, 3):
        return "about the same as"
    return f"{more} than" if a > b else f"{less} than"


def _cap(s):
    """Capitalise the first letter only (str.capitalize would lower-case 'PAYG', 'Battery', ...)."""
    return s[:1].upper() + s[1:]


def _one_in(share):
    """0.088 -> 'about 1 in 11'."""
    return f"about 1 in {round(1 / share)}" if share else "none"


def _mix_takeaway(pt, seg):
    """Section 1: what the refurb / new and Battery / Direct mix tells us, rather than restating the tables."""
    total = pt[REFURB].sum() / (pt[REFURB].sum() + pt[NEW].sum())
    hi, lo = sorted(GROUPS, key=lambda g: pt.loc[g, "refurb_pct"], reverse=True)
    main_new = max(GROUPS, key=lambda g: pt.loc[g, "share_new"])
    out = (f"Refurb is still a small part of sales — {_one_in(total)} units sold. It is relatively more common on "
           f"{hi} ({_one_in(pt.loc[hi, 'refurb_pct'])} {hi} units) than on {lo} ({_one_in(pt.loc[lo, 'refurb_pct'])}), "
           f"so while most new units sold are {main_new}, {hi} takes a bigger slice of refurb sales than of new sales — "
           f"refurb sales lean towards {hi}.")
    # A possible reason: how much a customer saves by choosing refurb over new, in shillings (median PAYG price)
    gap = {g: seg.loc[f"{NEW} {g}", "price_payg"] - seg.loc[f"{REFURB} {g}", "price_payg"] for g in GROUPS}
    if all(pd.notna(v) for v in gap.values()) and gap[hi] > gap[lo]:
        out += (f" One possible reason: choosing refurb saves the customer more on {hi} (about "
                f"{F0(round(gap[hi], -3) / 1000)}k at the median PAYG price, vs {F0(round(gap[lo], -3) / 1000)}k on "
                f"{lo}), which may make refurb more attractive there. Which units are available to refurbish is also "
                "likely to play a part.")
    return out


def _refurb_lean(pt):
    """The product type whose share of refurb sales is above its share of new sales."""
    return max(GROUPS, key=lambda g: pt.loc[g, "share_refurb"] - pt.loc[g, "share_new"])


def _mix_highlight(pt):
    """Key highlight: refurb's place in sales and which product type it leans to, in one plain sentence."""
    total = pt[REFURB].sum() / (pt[REFURB].sum() + pt[NEW].sum())
    lean = _refurb_lean(pt)
    other = [g for g in GROUPS if g != lean][0]
    return (f"About 1 in {round(1 / total)} units sold is refurb, and it is more common on {lean} "
            f"({_one_in(pt.loc[lean, 'refurb_pct'])}) than on {other} ({_one_in(pt.loc[other, 'refurb_pct'])}). "
            f"So although most units sold are {other}, {lean} makes up {PCT(pt.loc[lean, 'share_refurb'])} of "
            f"refurb sales against {PCT(pt.loc[lean, 'share_new'])} of new sales.")


def _refund_takeaway(seg, close=0.01):
    """Section 1: does refurb get refunded more than new within each type, and does price explain it?
    Refund rates within `close` (1 point) of each other count as 'about as often'."""
    more, same = [], []
    for g in GROUPS:
        r, n = seg.loc[f"{REFURB} {g}"], seg.loc[f"{NEW} {g}"]
        (more if r["refund_rate"] - n["refund_rate"] >= close else same).append(g)
    discount = {g: 1 - seg.loc[f"{REFURB} {g}", "price_payg"] / seg.loc[f"{NEW} {g}", "price_payg"] for g in GROUPS}
    if not more:
        return ("Refurb units are refunded about as often as new units for both Battery and Direct, so buying refurb "
                "does not appear to carry a higher refund risk.")
    out = (f"Refurb {' and '.join(more)} units are refunded noticeably more often than new ones"
           + (f", while refurb {' and '.join(same)} units are refunded about as often as new" if same else "") + ". ")
    if all(pd.notna(discount[g]) and discount[g] > 0 for g in more):
        out += (f"As refurb {' and '.join(more)} units cost roughly "
                + " / ".join(f"{PCT(discount[g])}" for g in more)
                + " less than new, price is unlikely to be what drives these refunds — the condition or performance "
                  "of the refurb units may be worth a closer look, for example through refund reasons.")
    return out


def _join(items):
    items = [str(i) for i in items]
    return items[0] if len(items) == 1 else ", ".join(items[:-1]) + " and " + items[-1]


def _spread(refurb_count, new_count):
    """How spread refurb sales are vs new, from how many sellers make up half the units."""
    if refurb_count > 1.2 * new_count:
        return "more widely spread than"
    if refurb_count < 0.8 * new_count:
        return "more concentrated than"
    return "about as spread as"


def _trend_takeaway(yearly, last12, l12, regular=0.05):
    """Section 2: when refurb took hold on each type, where it stands now, and any spike months."""
    big = yearly[yearly["Total"] >= 1000].set_index("Year")
    start = {}
    for g in GROUPS:
        yrs = big.index[big[f"Refurb % of {g}"] >= regular]
        start[g] = int(yrs.min()) if len(yrs) else None
    known = [g for g in GROUPS if start[g]]
    out = ""
    if len(known) == 2 and start[GROUPS[0]] != start[GROUPS[1]]:
        first, later = sorted(known, key=lambda g: start[g])
        out = (f"Refurb started on {first}: it became a regular part of {first} sales from {start[first]}, but only "
               f"took off on {later} from {start[later]}. ")
    out += ("Over the last 12 months refurb is " + " and ".join(f"{_one_in(l12[g])} {g} units" for g in GROUPS) + ". ")
    m = last12.set_index("Month")
    refurb = m[[f"{REFURB} {g}" for g in GROUPS]].sum(axis=1)
    share = refurb / (refurb + m[[f"{NEW} {g}" for g in GROUPS]].sum(axis=1))
    spike = share[share >= 0.4].index.tolist()
    if spike:
        out += (f"{_join(spike)} stand{'s' if len(spike) == 1 else ''} out, with refurb close to half of all units "
                "sold — a spike that may reflect a large batch of refurb stock coming through, or less new stock, "
                "rather than a sudden shift in demand; worth checking against stock records.")
    return out


def _agents_takeaway(ag, top_n):
    """Section 3: is refurb driven by a handful of Selling Agents, and do the top sellers share a team?"""
    out = ("Refurb sales are not driven by a handful of Selling Agents: half of refurb units come from "
           + " and ".join(f"{F0(ag[g]['half_refurb'])} agents on {g}" for g in GROUPS) + " — "
           + ", and ".join(f"{_spread(ag[g]['half_refurb'], ag[g]['half_new'])} new sales on {g}" for g in GROUPS)
           + ". ")
    if ag["top_stl_agents"] > 2:
        out += (f"The top refurb sellers do cluster by team, though — {ag['top_stl_agents']} of the top {top_n} sit "
                f"under {ag['top_stl']} — so team-level differences are worth a look (Section 3b).")
    return out


def _stl_takeaway(st, top_row, country):
    """Section 3b: does the team make a difference, and is the biggest refurb team just big?"""
    sig = [g for g in GROUPS if pd.notna(st[g]["chi2_p"]) and st[g]["chi2_p"] < 0.05]
    if not sig:
        return "Refurb's share of sales does not differ meaningfully between teams."
    out = (f"Which team a Selling Agent sits in does make a difference: refurb's share of {_join(sig)} sales varies "
           "between teams by more than chance would explain. "
           + _cap("; ".join(f"{st[g]['higher']} teams sell noticeably more refurb {g} than average" for g in sig))
           + ". ")
    lead = max(sig, key=lambda g: st[g]["top_rate"] / st[g]["avg"])
    out += (f"At the top, {st[lead]['top_rate_name']}'s team sells {_one_in(st[lead]['top_rate'])} {lead} units as "
            f"refurb, against {_one_in(st[lead]['avg'])} for {country}. ")
    status = {g: top_row[f"vs avg {g}"] for g in GROUPS}
    in_line = [g for g in GROUPS if status[g] == "In line"]
    if in_line:
        out += (f"The biggest refurb team, {top_row['STL']}'s, is in line with average on {_join(in_line)} — its "
                "volume comes mainly from its size rather than a higher refurb rate. ")
    return out + ("A higher refurb rate may reflect a team's region, channel or access to refurb stock as much as "
                  "how it sells.")


def _channel_takeaway(ch, min_units=500):
    """Section 3: which channels lean on refurb."""
    leaders, laggards = {}, {}
    for g in GROUPS:
        size = ch[f"{NEW} {g}"] + ch[f"{REFURB} {g}"]
        main = ch[size >= min_units]
        name = main["Sales channel"].where(main["Sales channel"] != "Other", "the 'Other' channel")
        leaders[g] = name[main[f"Refurb % of {g}"].idxmax()]
        laggards[g] = name[main[f"Refurb % of {g}"].idxmin()]
    lead = set(leaders.values())
    out = (f"{_join(sorted(lead))} lean{'s' if len(lead) == 1 else ''} most on refurb"
           + (" for both Battery and Direct" if len(lead) == 1 else "") + ", ")
    low = set(laggards.values())
    out += (f"while {_join(sorted(low))} sell{'s' if len(low) == 1 else ''} very little refurb. ")
    return out + "This may reflect where refurb stock is held and which customers each channel serves."


def _monthly_dispatch_sentence(dm, this_year, min_refurb=5, few=30):
    """Month by month this year: in how many months was refurb's median wait shorter than new's?"""
    rows = dm[dm["Month of sale"].str.contains(str(this_year)) & (dm["refurb_n"] >= min_refurb)]
    if not len(rows):
        return ""
    faster = rows[rows["refurb_median"] < rows["new_median"] - 1]["Month of sale"].tolist()
    small = int((rows["refurb_n"] < few).sum())
    out = (f"Month by month, refurb's median wait was shorter than new in {len(faster)} of the {len(rows)} months of "
           f"{this_year} with at least {min_refurb} refurb dispatches")
    if small:
        out += f", though {small} of those months rest on fewer than {few} refurb accounts"
    return out + ". "


def _dispatch_takeaway(d, pend, dy, this_year, close=1, dm=None):
    """Section 4: how quickly refurb goes out vs new (all years, then this year), and which group looks stuck."""
    bits = []
    for g in GROUPS:
        r, n = d.loc[f"{REFURB} {g}", "median"], d.loc[f"{NEW} {g}", "median"]
        if r == n:
            speed = "as quickly as"
        elif abs(r - n) <= close:
            speed = "slightly faster than" if r < n else "slightly more slowly than"
        else:
            speed = "faster than" if r < n else "more slowly than"
        bits.append(f"refurb {g} goes out {speed} new {g} (typical wait {F0(r)} vs {F0(n)} days)")
    out = "Across all years, " + "; ".join(bits) + ". "
    yb = dy.set_index("Year of sale")
    if this_year in yb.index and this_year - 1 in yb.index:
        now, prev = yb.loc[this_year], yb.loc[this_year - 1]
        out += (f"Year by year the picture has shifted: in {this_year - 1} refurb was {_speed_both(prev)}, but in "
                f"{this_year} it is {_speed_both(now)}, mainly because new units have slowed down. ")
        if dm is not None:
            out += _monthly_dispatch_sentence(dm, this_year)
    out += ("Averages run higher than medians throughout because a small number of very long waits pull them up; "
            "the median is the better guide to a typical unit. ")
    stuck = max(SEGMENTS, key=lambda s: pend.loc[s, "pending_share"])
    if stuck.startswith(REFURB):
        g = stuck.split(" ", 1)[1]
        ratio = pend.loc[stuck, "pending_share"] / pend.loc[f"{NEW} {g}", "pending_share"]
        out += (f"{stuck} has the largest share of sold units still waiting for dispatch (about {F0(ratio)}x new "
                f"{g}), which may point to refurb {g} stock not keeping up with its growing sales — worth checking "
                "with operations.")
    return out


def _account_type_takeaway(at, test, by_year, ru):
    """Section 5: who buys refurb on each type (Cash vs PAYG), and where recent refurb sales are going."""
    if not {"Cash", "PAYG"} <= set(at.index):
        return "Only one account type has unit sales, so there is no Cash vs PAYG comparison."
    bits = []
    for g in GROUPS:
        c, p, pv = at.loc["Cash", f"Refurb % of {g}"], at.loc["PAYG", f"Refurb % of {g}"], test[g]
        if pd.notna(pv) and pv >= 0.05:
            bits.append(f"on {g} there is no real difference between Cash and PAYG buyers")
            continue
        hi, lo = ("Cash", "PAYG") if c > p else ("PAYG", "Cash")
        bit = (f"on {g}, refurb leans to {hi} buyers ({_one_in(max(c, p))} {hi} {g} units vs {_one_in(min(c, p))} "
               f"{lo})")
        if len(by_year):
            yb = by_year.set_index("Year")
            ahead = int((yb[f"{g} ({hi})"] > yb[f"{g} ({lo})"]).sum())
            if ahead == len(yb):
                bit += ", every year"
            elif ahead < len(yb) / 2:
                bit += (f" — although {lo} was ahead in {len(yb) - ahead} of {len(yb)} years, so the overall lean "
                        "comes from recent, larger years")
        bits.append(bit)
    out = _cap("; ".join(bits)) + ". "
    latest = ru.iloc[0]
    payg_share = {g: latest[f"Refurb {g} (PAYG)"] / (latest[f"Refurb {g} (PAYG)"] + latest[f"Refurb {g} (Cash)"])
                  for g in GROUPS}
    if all(pd.notna(v) and v > 0.5 for v in payg_share.values()):
        out += (f"So far in {int(latest['Sales Year'])}, most refurb units of both types have gone to PAYG customers ("
                + " and ".join(f"{PCT(payg_share[g])} of refurb {g}" for g in GROUPS)
                + "), so refurb is increasingly reaching PAYG buyers. ")
    return out + "This may point to how refurb is offered or priced for Cash and PAYG buyers — worth checking with the commercial team."


def _region_takeaway(rg, pt, min_units=500, even=0.10):
    """Section 6: is refurb spread evenly across regions, and which stand out?"""
    tops, lows, ranges = {}, {}, {}
    for g in GROUPS:
        size = rg[f"{NEW} {g}"] + rg[f"{REFURB} {g}"]
        main = rg[(rg["Region"] != "Not recorded") & (size >= min_units)]
        tops[g] = main.loc[main[f"Refurb % of {g}"].idxmax(), "Region"]
        lows[g] = main.loc[main[f"Refurb % of {g}"].idxmin(), "Region"]
        ranges[g] = main[f"Refurb % of {g}"].max() - main[f"Refurb % of {g}"].min()
    even_spread = all(r < even for r in ranges.values())
    out = ("Refurb sells across all regions rather than in a few pockets — the main regions sit within a few points "
           "of each other. " if even_spread else "Refurb is unevenly spread across regions. ")
    if tops[GROUPS[0]] == tops[GROUPS[1]]:
        out += f"{tops[GROUPS[0]]} leans most on refurb for both Battery and Direct"
    else:
        out += "The highest refurb share is in " + " and ".join(f"{tops[g]} ({g})" for g in GROUPS)
    out += ", and " + (f"{lows[GROUPS[0]]} the least" if lows[GROUPS[0]] == lows[GROUPS[1]]
                       else " and ".join(f"{lows[g]} ({g})" for g in GROUPS) + " the least") + ". "
    nr = rg[rg["Region"] == "Not recorded"]
    if len(nr) and all(nr[f"Refurb % of {g}"].iat[0] < pt.loc[g, "refurb_pct"] for g in GROUPS):
        out += "Accounts with no region recorded (partly bulk orders) have the least refurb of all. "
    return out + "Differences may reflect where refurb stock was available and which channels sell in each region."


def _customers_takeaway(cs):
    """Section 7: is refurb a one-off purchase?"""
    return ("Refurb is almost always a one-off purchase: "
            + " and ".join(f"{_one_in(cs[g]['multi'] / cs[g]['customers'])} refurb {g} customers" for g in GROUPS)
            + f" have bought more than one, and only {F0(cs['both'])} customers have bought both a refurb Battery and "
              f"a refurb Direct unit. {_cap(_one_in(cs['also_new'] / cs['customers']))} refurb customers has also "
              "bought a new unit, which suggests refurb mostly reaches customers buying a single system rather than "
              "repeat or upgrading customers.")


# ---------------------------------------------------------------------------
# Key highlights — one message per bullet, readable at a glance
# ---------------------------------------------------------------------------
def _growth_highlight(yearly, l12, this_year, lookback=2):
    """Refurb now vs `lookback` years ago, per product type."""
    yb = yearly.set_index("Year")
    base_year = this_year - lookback
    if base_year not in yb.index:
        return None
    then = {g: yb.loc[base_year, f"Refurb % of {g}"] for g in GROUPS}
    grew = all(l12[g] > then[g] for g in GROUPS)
    lead = "Refurb is growing fast." if grew else "Refurb's share has not grown on both types."
    return bullet(lead, "Over the last 12 months it is " + " and ".join(f"{_one_in(l12[g])} {g} units" for g in GROUPS)
                  + f" sold, up from " + " and ".join(_one_in(then[g]) for g in GROUPS) + f" in {base_year}."
                  if grew else
                  "Over the last 12 months it is " + " and ".join(f"{_one_in(l12[g])} {g} units" for g in GROUPS)
                  + f", against " + " and ".join(_one_in(then[g]) for g in GROUPS) + f" in {base_year}.")


def _price_refund_highlight(seg, close=0.01):
    """Refurb is cheaper — is it refunded more?"""
    discount = {g: 1 - seg.loc[f"{REFURB} {g}", "price_payg"] / seg.loc[f"{NEW} {g}", "price_payg"] for g in GROUPS}
    more = [g for g in GROUPS
            if seg.loc[f"{REFURB} {g}", "refund_rate"] - seg.loc[f"{NEW} {g}", "refund_rate"] >= close]
    lo, hi = min(discount.values()), max(discount.values())
    price = f"Refurb costs about {round(lo * 100)}–{PCT(hi)} less than new (median PAYG price)"
    if not more:
        return bullet("Cheaper refurb is not refunded more often.", price + ", and is refunded about as often.")
    g = more[0] if len(more) == 1 else None
    lead = (f"Refurb {g} is refunded more than new {g}, despite being cheaper." if g else
            "Refurb is refunded more than new, despite being cheaper.")
    detail = "; ".join(f"{x}: {PCT1(seg.loc[f'{REFURB} {x}', 'refund_rate'])} of refurb units refunded vs "
                       f"{PCT1(seg.loc[f'{NEW} {x}', 'refund_rate'])} of new" for x in GROUPS)
    return bullet(lead, f"{price}. {_cap(detail)} — so price alone does not seem to explain refunds.")


def _compare_days(r, n, close=1):
    """'faster than' / 'slower than' / 'about as fast as' for refurb (r) vs new (n) median days."""
    if abs(r - n) <= close:
        return "about as fast as"
    return "faster than" if r < n else "slower than"


def _speed_both(row, close_mean=2):
    """Refurb vs new on the median and on the average — one phrase if they agree, both if they don't."""
    med = _compare_days(row["refurb_median"], row["new_median"])
    avg = _compare_days(row["refurb_mean"], row["new_mean"], close=close_mean)
    if med == avg:
        return f"{med} new"
    return f"{med} new on the median but {avg.replace('about as fast as', 'about level with')} new on average"


def _dispatch_highlight(dy, this_year):
    """Is refurb dispatched faster than new, and has that changed this year?"""
    yb = dy.set_index("Year of sale")
    if this_year not in yb.index or this_year - 1 not in yb.index:
        return None
    now, prev = yb.loc[this_year], yb.loc[this_year - 1]
    hist = yb[yb.index < this_year]
    earlier_faster = int((hist["refurb_median"] < hist["new_median"] - 1).sum())
    speed_now, speed_prev = _speed_both(now), _speed_both(prev)
    lead = (f"In {this_year}, refurb is being dispatched {speed_now}."
            if speed_now != "about as fast as new" else f"In {this_year}, refurb and new are dispatched about as fast.")
    text = (f"Refurb units sold this year wait a median {F0(now['refurb_median'])} days for dispatch vs "
            f"{F0(now['new_median'])} for new (average {F0(now['refurb_mean'])} vs {F0(now['new_mean'])}); in "
            f"{this_year - 1} refurb was {speed_prev} (median {F0(prev['refurb_median'])} vs "
            f"{F0(prev['new_median'])}, average {F0(prev['refurb_mean'])} vs {F0(prev['new_mean'])}).")
    if speed_now != speed_prev:
        text += (f" The change comes mainly from new units slowing down ({F0(prev['new_median'])} → "
                 f"{F0(now['new_median'])} days) more than refurb ({F0(prev['refurb_median'])} → "
                 f"{F0(now['refurb_median'])}).")
    if earlier_faster:
        text += f" Refurb was also quicker in {earlier_faster} of the {len(hist)} earlier years."
    if now["coverage"] < 0.5:
        text += (f" Only {PCT(now['coverage'])} of {this_year} sales have a dispatch date recorded so far, so this "
                 "needs confirming.")
    return bullet(lead, text)


def _account_type_highlight(at, ru):
    """Who buys refurb: Cash vs PAYG, per product type, and where recent refurb sales went."""
    lean = {g: "Cash" if at.loc["Cash", f"Refurb % of {g}"] > at.loc["PAYG", f"Refurb % of {g}"] else "PAYG"
            for g in GROUPS}
    if lean[GROUPS[0]] == lean[GROUPS[1]]:
        lead = f"Refurb sells more to {lean[GROUPS[0]]} buyers."
    else:
        lead = "; ".join(f"{g} refurb sells more to {lean[g]} buyers" for g in GROUPS) + "."
    latest = ru.iloc[0]
    refurb_payg = sum(latest[f"Refurb {g} (PAYG)"] for g in GROUPS)
    refurb_all = refurb_payg + sum(latest[f"Refurb {g} (Cash)"] for g in GROUPS)
    text = "; ".join(f"{_one_in(at.loc[lean[g], f'Refurb % of {g}'])} {lean[g]} {g} units is refurb" for g in GROUPS)
    if refurb_all:
        text += (f". But in {int(latest['Sales Year'])} so far, {_one_in(1 - refurb_payg / refurb_all).replace('about 1 in', 'only 1 in')}"
                 " refurb units went to Cash buyers — refurb is increasingly reaching PAYG customers"
                 if refurb_payg / refurb_all > 0.5 else "")
    return bullet(lead, _cap(text) + ".")


def _repeat_highlight(cs):
    multi = sum(cs[g]["multi"] for g in GROUPS)
    buyers = sum(cs[g]["customers"] for g in GROUPS)
    return bullet("Refurb is a one-off purchase.",
                  f"Only {_one_in(multi / buyers).replace('about ', '')} refurb customers has bought more than one "
                  f"refurb unit, and {_one_in(cs['also_new'] / cs['customers'])} has also bought a new unit.")


def _years(ys):
    ys = [str(int(y)) for y in ys]
    return ys[0] if len(ys) == 1 else ", ".join(ys[:-1]) + " and " + ys[-1]


# ---------------------------------------------------------------------------
# Report content — edit wording / structure here
# ---------------------------------------------------------------------------
def build_report(t, meta):
    country = meta["country"]
    pt = t["product_types"].set_index("Product type")
    seg = t["segments"].set_index("Segment")
    yearly, monthly = t["yearly"], t["monthly"]
    last12 = monthly.iloc[:-1]  # full months only (the last row is the current month to date)
    l12 = {g: last12[f"{REFURB} {g}"].sum() / (last12[f"{REFURB} {g}"].sum() + last12[f"{NEW} {g}"].sum())
           for g in GROUPS}
    d = t["dispatch"].set_index("Segment")
    pend = t["pending_dispatch"].set_index("Segment")
    at = t["account_type"].set_index("Account type")
    cs = t["refurb_customer_stats"]

    blocks = [title(f"Refurbished vs New Units: Battery vs Direct — {country}")]

    # Highlights — the bold lead is the message, the line after it the evidence ------------------
    hi = [
        bullet(f"Refurb leans towards {_refurb_lean(pt)}.", _mix_highlight(pt)),
        _growth_highlight(yearly, l12, meta["snapshot_at"].year),
        _price_refund_highlight(seg),
        _dispatch_highlight(t["dispatch_by_year"], meta["snapshot_at"].year),
    ]
    if {"Cash", "PAYG"} <= set(at.index):
        hi.append(_account_type_highlight(at, t["refurb_units_by_year"]))
    hi.append(_repeat_highlight(cs))
    blocks += [h2("Key Highlights"), *[b for b in hi if b]]

    # 1. Proportion ------------------------------------------------------------
    # Table 1.1: per product type, New vs Refurb (each row adds up to 100%)
    overall = pt[[NEW, REFURB]].copy()
    overall.loc["Total"] = overall.sum()
    overall["New (%)"] = overall[NEW] / overall.sum(axis=1)
    overall["Refurb (%)"] = 1 - overall["New (%)"]
    overall = overall.rename_axis("Product type").reset_index()

    # Tables 1.2 / 1.3: how refurb (new) units split between Battery and Direct (each row adds up to 100%)
    def _type_split(unit_type):
        units = pt[unit_type]
        row = {"Unit type": unit_type, **{g: units[g] for g in GROUPS},
               **{f"{g} (%)": units[g] / units.sum() for g in GROUPS}}
        return pd.DataFrame([row])

    split_fmt = {**{g: F0 for g in GROUPS}, **{f"{g} (%)": PCT1 for g in GROUPS}}
    seg_tbl = t["segments"].rename(columns={"accounts": "Accounts", "gross": "Gross unit sales",
                                            "share": "% of Battery + Direct units", "refunded": "Refunded units",
                                            "refund_rate": "Refund rate", "price_cash": "Median price/unit (Cash)",
                                            "price_payg": "Median price/unit (PAYG)"})
    seg_tbl = seg_tbl[["Segment", "Accounts", "Gross unit sales", "% of Battery + Direct units", "Refunded units",
                       "Refund rate", "Median price/unit (Cash)", "Median price/unit (PAYG)"]]
    rprod = t["refurb_products"].rename(columns={"share": "% of type's refurb units"})
    blocks += [
        h2("Section 1: Proportion of Refurb vs New Unit Sales"),
        table(overall, formats={NEW: F0, REFURB: F0, "New (%)": PCT1, "Refurb (%)": PCT1},
              title="Gross unit sales — overall", group="mix",
              note="New (%) and Refurb (%) split each product type's units, so each row adds up to 100%."),
        table(_type_split(REFURB), formats=split_fmt, title="Gross unit sales — refurb", group="mix",
              note="How refurb units split between Battery and Direct (adds up to 100%)."),
        table(_type_split(NEW), formats=split_fmt, title="Gross unit sales — new", group="mix",
              note="How new units split between Battery and Direct (adds up to 100%)."),
        para("Takeaway:", _mix_takeaway(pt, seg)),
        table(seg_tbl, formats={"Accounts": F0, "Gross unit sales": F0, "% of Battery + Direct units": PCT1,
                                "Refunded units": F0, "Refund rate": PCT1, "Median price/unit (Cash)": F0,
                                "Median price/unit (PAYG)": F0},
              title="Battery vs Direct — refurb vs new",
              note="Refund rate = refunded units ÷ gross unit sales. Median price/unit = Total_Price ÷ productQty."),
        para("Takeaway:", _refund_takeaway(seg)),
        table(rprod, formats={"Units": F0, "% of type's refurb units": PCT1}, text_cols=2,
              title="Top refurb products by product type",
              note="Top 5 refurb products within Battery and within Direct."),
    ]

    # 2. Trends ----------------------------------------------------------------
    big = yearly[(yearly["Total"] >= 1000)]
    peak = {g: big.loc[big[f"Refurb % of {g}"].idxmax()] for g in GROUPS}
    blocks += [
        h2("Section 2: Yearly and Monthly Trend"),
        _seg_table(yearly.astype({"Year": str}), "Year", title="Battery vs Direct units by year — refurb vs new",
                   note=f"Year of sale (refunded accounts: RefundDate, else the date the account was created). "
                        f"{SEG_NOTE}"),
        _seg_table(monthly, "Month", title="Battery vs Direct units by month — refurb vs new",
                   note="Last 12 full months plus the current month to date."),
        para("Takeaway:", _trend_takeaway(yearly, last12, l12)),
    ]

    # 3. Selling agents and STLs -----------------------------------------------
    ag, st = t["agent_stats"], t["stl_stats"]
    agents = t["agents_top"].rename(columns={"name": "Selling Agent", "stl": "STL",
                                             **{f"Refurb % of {g}": _pct(g) for g in GROUPS}})
    agents = agents[["Selling Agent", "STL"] + [c for g in GROUPS for c in (f"{REFURB} {g}", _pct(g))]]
    a_fmt = {**{f"{REFURB} {g}": F0 for g in GROUPS}, **{_pct(g): PCT1 for g in GROUPS}}
    stl = t["stl_top"].rename(columns={"name": "STL", **{f"Refurb % of {g}": _pct(g) for g in GROUPS}})
    stl = stl[["STL"] + [c for g in GROUPS for c in (f"{REFURB} {g}", _pct(g), f"vs avg {g}")]]
    blocks += [
        h2("Section 3: Selling Agent Performance"),
        table(agents, formats=a_fmt, text_cols=2, title=f"Top {meta['top_n']} Selling Agents by refurb units",
              note=f"Ranked by refurb Battery + Direct units. Refurb % of Battery units = refurb ÷ all the agent's "
                   "Battery units (likewise for Direct). STL = current supervisor in AMT."),
        para("Takeaway:", _agents_takeaway(ag, meta["top_n"])),
        h3("Section 3b: By Sales Team Lead (STL)"),
        table(stl, formats=a_fmt, title=f"Top {meta['top_n']} STLs by refurb units",
              note=f"Ranked by the team's refurb Battery + Direct units. vs avg = whether the team's refurb % of "
                   f"Battery (or Direct) units is higher or lower than the {country} average by more than chance "
                   f"would explain (binomial test, 5% level, Bonferroni-corrected; teams with "
                   f"{meta['min_group_units']}+ units of that type)."),
        para("Takeaway:", _stl_takeaway(st, t["stl_top"].iloc[0].rename({"name": "STL"}), country)),
        _seg_table(t["channels"], "Sales channel", title="Battery vs Direct by sales channel — refurb vs new",
                   note=SEG_NOTE),
        para("Takeaway:", _channel_takeaway(t["channels"])),
    ]

    # 4. Dispatch --------------------------------------------------------------
    ex = t["dispatch_excluded"]
    d_tbl = t["dispatch"].rename(columns={"sold": "Accounts sold", "used": "Used", "mean": "Avg days",
                                          "median": "Median days", "within_7": "Within 7 days",
                                          "within_30": "Within 30 days"})
    # Column order pairs New and Refurb for each measure, so they can be compared side by side
    days_cols = {"new_mean": "New — avg. days", "refurb_mean": "Refurb — avg. days",
                 "new_median": "New — median days", "refurb_median": "Refurb — median days",
                 "new_w7": "New — within 7 days", "refurb_w7": "Refurb — within 7 days",
                 "refurb_n": "Refurb accounts", "coverage": "Dispatch date recorded"}
    days_fmt = {"New — avg. days": F1, "Refurb — avg. days": F1, "New — median days": F0,
                "Refurb — median days": F0, "New — within 7 days": PCT, "Refurb — within 7 days": PCT,
                "Refurb accounts": F0, "Dispatch date recorded": PCT}

    def _days_table(df, first):
        cols = [first] + [c for c in days_cols if c in df.columns]
        return df[cols].rename(columns=days_cols)
    dm = t["dispatch_monthly"]
    p_tbl = t["pending_dispatch"].rename(columns={"sold": "Units sold", "pending": "Dispatch Pending",
                                                  "pending_share": "% pending"})
    low_cov = dm.loc[dm["coverage"] < 0.5, "Month of sale"].tolist()
    blocks += [
        h2("Section 4: Days from Sale to Dispatch"),
        table(d_tbl, formats={"Accounts sold": F0, "Used": F0, "Avg days": F1, "Median days": F0,
                              "Within 7 days": PCT, "Within 30 days": PCT},
              title="Days from sale to dispatch — Battery vs Direct, refurb vs new",
              note=f"Per sold account. Used = dispatched 0–{meta['max_dispatch_days']} days after sale; left out: "
                   f"{F0(ex['no_dispatch'])} with no dispatch date, {F0(ex['negative'])} dispatched before the sale "
                   f"date, {F0(ex['over_max'])} over {meta['max_dispatch_days']} days."),
        table(_days_table(t["dispatch_by_year"].astype({"Year of sale": str}), "Year of sale"),
              formats=days_fmt,
              title="Days from sale to dispatch by year of sale — refurb vs new",
              note="Battery and Direct together. Median = the typical unit; the average is pulled up by a few very "
                   "long waits. Dispatch date recorded = share of accounts sold that year with a dispatchDate; where "
                   "it is low, the year rests on few accounts."),
        table(_days_table(dm, "Month of sale"), formats=days_fmt,
              title="Days from sale to dispatch by month of sale — refurb vs new",
              note="Battery and Direct together, as in the table above. Refurb accounts = refurb accounts dispatched "
                   "that month (0–" f"{meta['max_dispatch_days']} days after sale); months with few of them can swing "
                   "a lot. Dispatch date recorded = share of all accounts sold that month with a dispatchDate."),
        table(p_tbl, formats={"Units sold": F0, "Dispatch Pending": F0, "% pending": PCT1},
              title="Sold units pending dispatch — Battery vs Direct, refurb vs new",
              note="funnel_status = Dispatch Pending today."),
        para("Takeaway:", _dispatch_takeaway(d, pend, t["dispatch_by_year"], meta["snapshot_at"].year, dm=dm)
                          + (f" Note that from {low_cov[0]} fewer than half of sold accounts have a dispatch date "
                             "recorded, so recent months rest on few accounts." if low_cov else "")),
    ]

    # 5. Account type ----------------------------------------------------------
    ru = t["refurb_units_by_year"]
    by_year = t["account_type_by_year"]
    blocks += [
        h2("Section 5: Cash vs PAYG Accounts"),
        _seg_table(t["account_type"], "Account type", title="Battery vs Direct by account type — refurb vs new",
                   note=SEG_NOTE),
        table(ru.astype({"Sales Year": str}), formats={c: F0 for c in ru.columns if c != "Sales Year"},
              title="Refurb units by sales year — Battery vs Direct, Cash vs PAYG",
              note="Refurb units (sum of productQty) by year of sale."),
    ]
    if len(by_year):
        blocks.append(table(by_year.astype({"Year": str}), formats={c: PCT1 for c in by_year.columns if c != "Year"},
                            title="Refurb % of units by year — Battery vs Direct, Cash vs PAYG",
                            note="e.g. Battery (Cash) = refurb Battery units ÷ all Battery units on Cash accounts "
                                 "that year (years with 20+ refurb units)."))
    blocks.append(para("Takeaway:", _account_type_takeaway(at, t["account_type_test"], by_year, ru)))

    # 6. Region ----------------------------------------------------------------
    rg = t["region"]
    blocks += [
        h2("Section 6: Refurb vs New by Region"),
        _seg_table(rg, "Region", title="Battery vs Direct by region — refurb vs new",
                   note=SEG_NOTE + " Not recorded = no region captured."),
        para("Takeaway:", _region_takeaway(rg, pt)),
    ]

    # 7. Customers -------------------------------------------------------------
    blocks += [
        h2("Section 7: Customers with More Than One Refurb Unit"),
        table(t["refurb_customers"], formats={f"{g} customers": F0 for g in GROUPS},
              title="Customers by number of refurb units — Battery vs Direct",
              note="Customers (customerId) by refurb units of each type across all their accounts."),
        para("Takeaway:", _customers_takeaway(cs)),
    ]

    blocks += report_sections.methodology([
        ("Scope", f"Gross unit sales, {country}: accounts with a sale date plus refunded accounts (category = Unit; "
                  "latest marts.mart_accounts record per account; test customers excluded)"),
        ("Units", "sum of productQty (a Cash account can carry several units)"),
        ("Refurb units", "products with isRefurb = 1 in amt.products (joined on productId); every other unit is New"),
        ("Product type", "Battery = pump + controller + battery (Climate Smart Battery, CSB); Direct = pump connected "
                         "straight to the panels (Climate Smart Direct, CSD); from data/product_groups_v2.csv "
                         "(hand-maintained; products not in it count as Other). Only Battery and Direct products are "
                         "reported"),
        ("Dates", "year / month of sale_date; refunded accounts by RefundDate, else the date the account was created"),
        ("People", "Selling Agent = salesAgents on the account, named from employee_name; STL = the Selling Agent's "
                   "current supervisor in AMT"),
        ("Significance tests", "chi-square and binomial tests on units, 5% level"),
        ("Data", f"ClickHouse snapshot {meta['snapshot_at']:%d %b %Y %H:%M} ({meta['snapshot']})"),
    ])
    blocks += report_sections.report_details(date=meta["generated_at"], period=meta["period"])
    return blocks


# ---------------------------------------------------------------------------
# Markdown renderer (drafting / review)
# ---------------------------------------------------------------------------
def _md_cell(text):
    return str(text).replace("|", "\\|")


def _number_tables(blocks):
    """Give every titled table its number ('3', or '1.2' for consecutive tables sharing a `group`)."""
    n_tables, n_sub, last_group = 0, 0, None
    for b in blocks:
        if b["type"] != "table" or not b.get("title"):
            continue
        group = b.get("group")
        if group and group == last_group:
            n_sub += 1
        else:
            n_tables += 1
            n_sub = 1
        last_group = group
        b["number"] = f"{n_tables}.{n_sub}" if group else str(n_tables)
    return blocks


def render_markdown(blocks, out_path):
    lines = []
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
                lines += [f"**Table {b['number']}: {b['title']}**", ""]
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


# ---------------------------------------------------------------------------
# Word renderer (stakeholder version) — same content and table numbers as the Markdown
# ---------------------------------------------------------------------------
def _shade(cell, fill):
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), fill)
    cell._tc.get_or_add_tcPr().append(shd)


def _set_cell(cell, text, bold=False, color=None, align=None, size=docx_styles.TABLE_PT):
    cell.text = ""
    p = cell.paragraphs[0]
    p.paragraph_format.space_after = Pt(0)
    if align is not None:
        p.alignment = align
    run = p.add_run(text)
    run.font.size = Pt(size)
    run.bold = bold
    if color:
        run.font.color.rgb = color


def _docx_small(doc, text):
    p = doc.add_paragraph()
    p.paragraph_format.space_after = Pt(4)
    run = p.add_run(text)
    run.italic = True
    run.font.size = Pt(docx_styles.NOTE_PT)
    run.font.color.rgb = MUTED


def _docx_lead(doc, lead, text, style=None, before=0):
    p = doc.add_paragraph(style=style)
    p.paragraph_format.space_before = Pt(before)
    p.paragraph_format.space_after = Pt(2 if style else 3)
    p.add_run(lead + " ").bold = True
    p.add_run(text)
    return p


def _docx_table(doc, b):
    if not b["header"]:  # label / detail list (methodology, report details) -> bold-label lines
        for r in b["rows"]:
            p = _docx_lead(doc, f"{r[0]}:", " · ".join(r[1:]))
            p.paragraph_format.space_after = Pt(1)
        return
    if b["title"]:
        cap = doc.add_paragraph()
        cap.paragraph_format.space_before = Pt(6)
        cap.paragraph_format.space_after = Pt(2)
        cap.paragraph_format.keep_with_next = True
        cap.add_run(f"Table {b['number']}: {b['title']}").bold = True
    cols, ncols = b["columns"], len(b["columns"])
    tbl = doc.add_table(rows=1, cols=ncols)
    tbl.style = "Table Grid"
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    tbl.autofit = False
    text_share = 0.24 if b["text_cols"] == 1 else 0.40  # page width given to the label column(s)
    widths = [text_share / b["text_cols"] if i < b["text_cols"] else (1 - text_share) / (ncols - b["text_cols"])
              for i in range(ncols)]

    def align(i):
        return WD_ALIGN_PARAGRAPH.LEFT if i < b["text_cols"] else WD_ALIGN_PARAGRAPH.CENTER

    for i, col in enumerate(cols):
        cell = tbl.rows[0].cells[i]
        _set_cell(cell, col, bold=True, color=WHITE, align=align(i))
        _shade(cell, HEADER_FILL)
    for n, r in enumerate(b["rows"]):
        cells = tbl.add_row().cells
        for i, text in enumerate(r):
            _set_cell(cells[i], text, align=align(i), bold=(r[0] == "Total"))
            if n % 2:
                _shade(cells[i], ALT_FILL)
    for r_idx, tr in enumerate(tbl.rows):
        tr._tr.get_or_add_trPr().append(OxmlElement("w:cantSplit"))
        for i, cell in enumerate(tr.cells):
            cell.width = Cm(PAGE_WIDTH_CM * widths[i])
            if b["note"] or r_idx < len(tbl.rows) - 1:  # keep the table (and its note) on one page
                for p in cell.paragraphs:
                    p.paragraph_format.keep_with_next = True
    if b["note"]:
        _docx_small(doc, b["note"])


def render_docx(blocks, out_path):
    doc = Document()
    for section in doc.sections:
        section.left_margin = section.right_margin = Cm(1.9)
        section.top_margin = section.bottom_margin = Cm(1.6)
    docx_styles.apply_defaults(doc)  # Calibri: content 10 pt, section titles 13 pt
    doc.styles["Normal"].paragraph_format.space_after = Pt(3)

    for i, b in enumerate(blocks):
        kind = b["type"]
        if kind == "title":
            doc.add_heading(b["text"], level=1).paragraph_format.space_after = Pt(4)
        elif kind in ("h2", "h3"):
            h = doc.add_heading(b["text"], level=2 if kind == "h2" else 3)
            h.paragraph_format.space_before = Pt(10 if kind == "h2" else 6)
            h.paragraph_format.space_after = Pt(3)
        elif kind == "bullet":
            _docx_lead(doc, b["lead"], b["text"], style="List Bullet")
        elif kind == "para":
            _docx_lead(doc, b["lead"], b["text"], before=3)
        elif kind == "text":
            doc.add_paragraph(b["text"])
        elif kind == "note":
            _docx_small(doc, b["text"])
        elif kind == "table":
            _docx_table(doc, b)
        # A lead-in line directly before a table stays on the same page as it
        if kind in ("para", "text") and i + 1 < len(blocks) and blocks[i + 1]["type"] == "table":
            doc.paragraphs[-1].paragraph_format.keep_with_next = True
    doc.save(out_path)
    return out_path


def generate(t, meta, out_path, fmt="md"):
    blocks = _number_tables(build_report(t, meta))
    return render_docx(blocks, out_path) if fmt == "docx" else render_markdown(blocks, out_path)
