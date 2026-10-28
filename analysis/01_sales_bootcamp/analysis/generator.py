"""
Renders the tables computed by builder.py into a short stakeholder digest.

The report content (headings, bullets, tables, wording) is assembled once by
build_report(); render_markdown() and render_docx() only lay it out. Draft and
agree on the .md, then produce the .docx — both always say the same thing.

Narrative sentences are generated from the numbers, so re-running on newer data
updates the text as well as the tables. Review the wording before sharing.
"""
import pandas as pd
from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Pt, RGBColor, Cm

HEADER_FILL = "1F3864"
HOLDOUT_FILL = "EDEDED"
ALT_FILL = "F5F8FC"
MUTED = RGBColor(0x59, 0x59, 0x59)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
PAGE_WIDTH_CM = 17.2

F1 = "{:.1f}".format
F0 = "{:,.0f}".format
PCT = "{:.0%}".format
SPCT = "{:+.0%}".format
SPP = "{:+.0f} pp".format
SNUM = "{:+,.0f}".format


# ---------------------------------------------------------------------------
# Content blocks
# ---------------------------------------------------------------------------
def _fmt(value, fmt):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "–"
    if isinstance(value, pd.Period):
        return value.strftime("%b %Y")
    return fmt(value) if fmt else str(value)


def title(text):
    return {"type": "title", "text": text}


def h2(text):
    return {"type": "h2", "text": text}


def bullet(lead, text):
    return {"type": "bullet", "lead": lead, "text": text}


def para(lead, text):
    return {"type": "para", "lead": lead, "text": text}


def text(body):
    return {"type": "text", "text": body}


def note(text):
    return {"type": "note", "text": text}


def item(text):
    """Plain bullet point (no bold lead)."""
    return {"type": "item", "text": text}


def table(df, formats=None, holdout_rows=(), note=None, header=True, first_col=0.3, text_cols=1):
    """A table with every value already formatted as text.
    holdout_rows = row positions to grey out; first_col = share of page width for column 0 (docx);
    the first `text_cols` columns are left-aligned."""
    formats = formats or {}
    cells = [[_fmt(row[c], formats.get(c)) for c in df.columns] for _, row in df.iterrows()]
    return {"type": "table", "columns": [str(c) for c in df.columns], "rows": cells,
            "holdout_rows": set(holdout_rows), "note": note, "header": header,
            "first_col": first_col, "text_cols": text_cols}


def _months_label(months):
    return f"{months[0].strftime('%b')}–{months[-1].strftime('%b %Y')}"


def _chg(v):
    """Signed % change for prose; near-zero reads as 'flat'."""
    return "flat" if abs(v) < 0.005 else SPCT(v)


def _vs_ly(pp):
    """How a 2026 change compares with the same period in 2025 — descriptive, not causal."""
    if abs(pp) < 5:
        return "closely mirrors"
    return "is ahead of" if pp > 0 else "is behind"


# ---------------------------------------------------------------------------
# Facts used by the narrative (all derived from the tables)
# ---------------------------------------------------------------------------
def _facts(t):
    season = t["seasonality"].set_index(["Cohort", "Segment"])
    boot_season = season.loc["Bootcamp"]
    seg_season = boot_season.drop("All segments")
    trend = t["monthly_bootcamp"]
    post = trend[trend["Phase"] == "Post"]
    peak = post.loc[post["productivity"].idxmax()]
    fc = t["field_compare"]
    drv = t["drivers"].set_index("Segment")
    return {
        "all": boot_season.loc["All segments"],
        "season": boot_season,
        "best_seg": seg_season["2026 vs 2025 (pp)"].idxmax(),
        "peak_month": peak["month"].strftime("%B"), "peak": peak["productivity"],
        "last_month": post.iloc[-1]["month"].strftime("%B"), "last": post.iloc[-1]["productivity"],
        "ratio_pre": fc[fc["Phase"] == "Pre"]["Ratio"], "ratio_post": fc[fc["Phase"] == "Post"]["Ratio"],
        "drv": drv,
        "tele_ssc_share": drv.loc[["Telesales", "SSC"], "Share of growth"].sum(),
        "cov": t["cascade_coverage"],
        "yoy": t["yoy"].set_index(["Cohort", "Segment"]).loc["Bootcamp"],
        "late": t["yoy_late"],
        "matched": t["tenure_matched"],
        "joiners": t["new_joiners"],
        "newer": _pooled_tenure_change(t["tenure_field"], ["0–5 months", "6–11 months", "1–2 years"]),
        "senior": _pooled_tenure_change(t["tenure_field"], ["3+ years"]),
    }


def _pooled_tenure_change(tf, bands):
    """Pre->post productivity change for a group of tenure bands, per cohort (units / agent-months)."""
    x = tf[tf["Tenure band"].isin(bands)]
    out = {}
    for key in ["B", "NB"]:
        pre = (x[f"{key} pre"].fillna(0) * x[f"{key} agent_months_pre"]).sum() / x[f"{key} agent_months_pre"].sum()
        post = (x[f"{key} post"].fillna(0) * x[f"{key} agent_months_post"]).sum() / x[f"{key} agent_months_post"].sum()
        out[key] = post / pre - 1
    out["B_agents"] = x["B agents"].sum()
    return out


# ---------------------------------------------------------------------------
# Report content — edit wording / structure here; both formats follow
# ---------------------------------------------------------------------------
def build_report(t, meta):
    f = _facts(t)
    pre_lbl, post_lbl = _months_label(meta["pre_months"]), _months_label(meta["post_months"])
    a, s = f["all"], f["season"]
    blocks = [title("Weekly Data Digest — May 2026 Sales Bootcamp: 4-Month Review")]

    info = pd.DataFrame([
        ("Bootcamp", f"{meta['bootcamp_start']:%d}–{meta['bootcamp_end']:%d %b %Y} · {meta['participants']} participants"),
        ("Comparison", f"{pre_lbl} (pre) vs {post_lbl} (post) · May 2026 held out · benchmark: same months in 2025"),
        ("Data", f"Sales through {meta['data_through']:%d %b %Y} · generated {meta['generated_at']:%d %b %Y}"),
    ], columns=["Item", "Detail"])
    blocks.append(table(info, header=False, first_col=0.16, text_cols=2))

    # Context ------------------------------------------------------------------
    blocks += [
        h2("Context"),
        text(f"The May 2026 Sales Bootcamp brought together {meta['participants']} selected participants across Field "
             "Sales Agents (FSAs), Telesales, Service Center Associates, Sales Team Leaders (STLs), Regional Sales "
             "Managers (RSM) and support functions for four days of sales training, held from "
             f"{meta['bootcamp_start']:%d}–{meta['bootcamp_end']:%d %B %Y}."),
        text("The June digest reported an early uplift based on the first weeks after training. With four full months "
             f"of post-training sales now available ({post_lbl}), we have re-run the analysis to see whether that "
             "uplift held, how it compares with the same period last year, and what has changed since."),
        text("This digest unpacks what changed and who drove it."),
    ]

    # Highlights ---------------------------------------------------------------
    b, fs = s.loc[f["best_seg"]], s.loc["Field Sales"]
    ya, late = f["yoy"].loc["All segments"], f["late"]
    blocks += [
        h2("Key Highlights"),
        bullet("Productivity is up, in a pattern similar to last year.",
               f"The Bootcamp cohort moved from {a['2026 Jan-Apr']:.1f} to {a['2026 Jun-Sep']:.1f} units per selling agent "
               f"per month ({SPCT(a['2026 change'])}). Over the same months in 2025, with no bootcamp, the same group rose "
               f"{SPCT(a['2025 change'])} — a similar pattern that may point to a seasonal component in the lift."),
        bullet("Volume grew alongside a larger selling team.",
               f"Year on year, Jun–Sep units are {SPCT(ya['Units YoY'])} while productivity is {_chg(ya['Jun-Sep YoY'])}; "
               f"the cohort had {ya['Agents/mo 2026']:.0f} selling agents a month vs {ya['Agents/mo 2025']:.0f} last year."),
        bullet("The June peak has not held.",
               f"Productivity peaked at {f['peak']:.1f} in {f['peak_month']}; {late['months']} averaged {late['2026']:.1f}, "
               f"compared with {late['2025']:.1f} in the same months of 2025."),
        bullet(f"{f['best_seg']} shows the strongest improvement.",
               f"{SPCT(b['2026 change'])} vs {SPCT(b['2025 change'])} over the same months last year — the segment furthest "
               "ahead of its 2025 pattern."),
        bullet("Telesales and SSC accounted for most of the growth; Field Sales was flat.",
               f"They delivered {PCT(f['tele_ssc_share'])} of added units. Bootcamp Field Sales moved "
               f"{SPCT(fs['2026 change'])}, compared with {SPCT(fs['2025 change'])} over the same months in 2025."),
        bullet("Bootcamp field agents were strong performers going in — and kept their lead.",
               f"Compared with non-bootcamp field selling agents of the same tenure, bootcamp agents out-sold them "
               f"{f['matched']['pre']['ratio']:.1f}× before training and {f['matched']['post']['ratio']:.1f}× after."),
    ]

    # 1. Monthly trend -------------------------------------------------------------
    trend = t["monthly_bootcamp"].merge(
        t["field_compare"][["month", "Non-Bootcamp productivity"]], on="month", how="left")
    trend = trend[["month", "agents", "units", "productivity", "Field Sales", "Telesales", "SSC",
                   "Non-Bootcamp productivity"]].rename(columns={
        "month": "Month", "agents": "Selling Agents", "units": "Units", "productivity": "Productivity (Overall)",
        "Field Sales": "Productivity (FSA)", "Telesales": "Productivity (Telesales)", "SSC": "Productivity (SSC)",
        "Non-Bootcamp productivity": "Productivity (Non-Bootcamp FSA)"})
    holdout = [i for i, m in enumerate(t["monthly_bootcamp"]["Phase"]) if m == "Bootcamp month"]
    blocks += [
        h2("1. Monthly Productivity — Bootcamp Cohort"),
        table(trend, formats={"Selling Agents": F0, "Units": F0, **{c: F1 for c in trend.columns if c.startswith("Productivity")}},
              holdout_rows=holdout, first_col=0.11,
              note="Productivity = units per selling agent. Grey/italic = May 2026 hold-out. Last column = field "
                   "selling agents who did not attend (excluding those who joined after the bootcamp), for comparison."),
        para("Takeaway:", f"Productivity varies considerably month to month (strong January, low March–April). The "
                          f"post-bootcamp peak in {f['peak_month']} has not been sustained."),
    ]

    # 2. Seasonality -----------------------------------------------------------------
    sea = t["seasonality"].copy()
    sea.insert(0, "Group", [f"{c} — {g}" if g != "All segments" else f"{c} — all segments"
                            for c, g in zip(sea["Cohort"], sea["Segment"])])
    sea = sea[["Group", "2026 Jan-Apr", "2026 Jun-Sep", "2026 change", "2025 change", "2026 vs 2025 (pp)"]].rename(
        columns={"2026 change": "Change 2026", "2025 change": "Change 2025 (no bootcamp)",
                 "2026 vs 2025 (pp)": "Difference vs 2025"})
    blocks += [
        h2("2. Comparison with the Same Period Last Year"),
        table(sea, formats={"2026 Jan-Apr": F1, "2026 Jun-Sep": F1, "Change 2026": SPCT,
                            "Change 2025 (no bootcamp)": SPCT, "Difference vs 2025": SPP},
              first_col=0.32,
              note="Change = Jun–Sep vs Jan–Apr productivity. 2025 uses the same people (current cohort tag) active at the time."),
        para("Takeaway:", f"Overall, the 2026 change ({SPCT(a['2026 change'])}) {_vs_ly(a['2026 vs 2025 (pp)'])} the "
                          f"same period in 2025 ({SPCT(a['2025 change'])}). The similarity may suggest a seasonal "
                          "pattern, though other factors (team size, product mix) also changed between the years. "
                          f"{f['best_seg']} shows the largest improvement relative to last year."),
    ]

    # Year-on-year: same months 2025 vs 2026
    ym = t["yoy_monthly"].rename(columns=lambda c: c.replace("Field Sales", "FSA").replace(" 2025", " '25")
                                 .replace(" 2026", " '26"))
    blocks += [
        para("Year on year:", f"the cohort sold {PCT(abs(ya['Units YoY']))} {'more' if ya['Units YoY'] >= 0 else 'fewer'} "
                              "units in Jun–Sep than in the same months "
                              f"of 2025, but with {ya['Agents/mo 2026']:.0f} selling agents a month vs "
                              f"{ya['Agents/mo 2025']:.0f} last year — so productivity per agent was {_chg(ya['Jun-Sep YoY'])}."),
        table(ym, formats={c: F1 for c in ym.columns if c != "Month"}, first_col=0.1,
              note="Bootcamp cohort productivity, Jun–Sep month by month, 2025 vs 2026."),
        para("Takeaway:", f"In 2026, the gains came early — productivity ran above 2025 in June–July, then fell below it "
                          f"in {late['months']}. In 2025, productivity rose steadily through the same months."),
    ]

    # 3. Employee tenure ---------------------------------------------------------------
    med = t["tenure_median_years"]
    segs = [c for c in t["tenure_dist"].columns if c not in ("Tenure band", "All")]
    dist = t["tenure_dist"].astype({c: object for c in segs + ["All"]})
    dist.loc[len(dist)] = ["Median tenure (years)"] + [f"{med[s]:.1f}" for s in segs] + [f"{med['All']:.1f}"]
    dist = dist.rename(columns={"Field Sales": "FSA"})
    tf = t["tenure_field"]
    m = f["matched"]
    tfm = pd.DataFrame({
        "Tenure band": tf["Tenure band"],
        "Bootcamp Selling Agents/mo": tf["B agents"], "Bootcamp Jan–Apr": tf["B pre"],
        "Bootcamp Jun–Sep": tf["B post"], "Bootcamp Change": tf["B change"],
        "Non-Bootcamp Selling Agents/mo": tf["NB agents"], "Non-Bootcamp Jan–Apr": tf["NB pre"],
        "Non-Bootcamp Jun–Sep": tf["NB post"], "Non-Bootcamp Change": tf["NB change"],
    })
    tfm.loc[len(tfm)] = ["Tenure-matched overall", tf["B agents"].sum(), m["pre"]["bootcamp"], m["post"]["bootcamp"],
                         m["post"]["bootcamp"] / m["pre"]["bootcamp"] - 1, tf["NB agents"].sum(),
                         m["pre"]["non_bootcamp"], m["post"]["non_bootcamp"],
                         m["post"]["non_bootcamp"] / m["pre"]["non_bootcamp"] - 1]
    j, newer, senior = f["joiners"], f["newer"], f["senior"]
    band = tf.set_index("Tenure band").to_dict("index")
    blocks += [
        h2("3. Field Sales by Employee Tenure"),
        text("To check whether the gap between cohorts reflects experience, selling agents are grouped by tenure at "
             "the time of the bootcamp."),
        table(dist, first_col=0.3, note="Bootcamp selling agents who sold in Jan–Apr or Jun–Sep 2026, by tenure band."),
        table(tfm, formats={"Bootcamp Selling Agents/mo": F0, "Bootcamp Jan–Apr": F1, "Bootcamp Jun–Sep": F1,
                            "Bootcamp Change": SPCT, "Non-Bootcamp Selling Agents/mo": F0, "Non-Bootcamp Jan–Apr": F1,
                            "Non-Bootcamp Jun–Sep": F1, "Non-Bootcamp Change": SPCT},
              first_col=0.16,
              note=f"Field Sales productivity by tenure band. Tenure-matched overall = non-bootcamp bands weighted to the "
                   f"bootcamp tenure mix. Excludes ~{j['agents_per_month']:.0f} non-bootcamp selling agents a month "
                   f"({PCT(j['share_agent_months'])} of the Jun–Sep base, productivity {j['productivity']:.1f}) who joined "
                   "after the bootcamp. Telesales and SSC are not shown: nearly all their selling agents attended, leaving "
                   "no comparison group (see Section 2 for their year-on-year view)."),
        para("Takeaway:", f"Bootcamp and non-bootcamp field selling agents have a similar tenure mix, and bootcamp agents "
                          f"sell more within every band ({m['pre']['ratio']:.1f}× tenure-matched before the bootcamp, "
                          f"{m['post']['ratio']:.1f}× after). The lead is not explained by tenure — participants were "
                          "already among the stronger performers going in. By band the picture is mixed: the newest bootcamp agents "
                          f"(0–5 months) improved {SPCT(band['0–5 months']['B change'])} vs "
                          f"{SPCT(band['0–5 months']['NB change'])} for peers, but across all agents under 2 years the "
                          f"difference is small ({SPCT(newer['B'])} vs {SPCT(newer['NB'])}). The most experienced bootcamp "
                          f"agents (3+ years) moved {SPCT(senior['B'])} vs {SPCT(senior['NB'])} for peers. With only a handful "
                          "of bootcamp agents per band each month, these are signals to watch rather than conclusions."),
    ]

    # 4. Regional comparison ------------------------------------------------------------
    rf = t["region_field"]
    rt = pd.DataFrame({
        "Region": rf["Region"],
        "Bootcamp Jan–Apr": rf["B pre"],
        "Bootcamp Jun–Sep": rf["B post"], "Bootcamp Change": rf["B change"],
        "Non-Bootcamp Jan–Apr": rf["NB pre"],
        "Non-Bootcamp Jun–Sep": rf["NB post"], "Non-Bootcamp Change": rf["NB change"],
    })
    pending_label = "Pending installation (no region yet)"
    regions = rf[~rf["Region"].isin(["Overall", pending_label])].assign(gap=lambda d: d["B change"] - d["NB change"])
    pend = t["region_pending_share"]
    ahead = regions[regions["gap"] >= 0.10].sort_values("gap", ascending=False)
    behind = regions[regions["gap"] <= -0.10].sort_values("gap")
    overall = rf[rf["Region"] == "Overall"].iloc[0]

    def _list(d):
        return ", ".join(f"{r['Region']} ({SPCT(r['B change'])} vs {SPCT(r['NB change'])})" for _, r in d.iterrows())

    region_note = ("Field Sales (FSA, POP, STL) only, grouped by Customer Region. Regional productivity = units sold "
                   "into the region ÷ selling agents with a sale there that month; an agent selling into several regions "
                   "counts in each, so regions do not add up to Overall. Sales still pending installation have no "
                   "customer region yet and are shown separately. Excludes non-bootcamp agents who joined after the bootcamp.")
    if t["region_other"]:
        region_note += (f" Not shown: {', '.join(sorted(t['region_other']))} (fewer than 2 bootcamp selling agents a "
                        "month); included in Overall.")
    blocks += [
        h2("4. Monthly Productivity — Regional Comparison (Before & After)"),
        para("Read as directional:", f"{PCT(pend['post'])} of bootcamp field units sold in {post_lbl} are still pending "
                                     f"installation and have no customer region yet (vs {PCT(pend['pre'])} in {pre_lbl}). "
                                     f"Regional {post_lbl} figures are therefore understated, and the % changes are not a "
                                     "like-for-like comparison."),
        table(rt, formats={"Bootcamp Jan–Apr": F1, "Bootcamp Jun–Sep": F1,
                           "Bootcamp Change": SPCT, "Non-Bootcamp Jan–Apr": F1,
                           "Non-Bootcamp Jun–Sep": F1, "Non-Bootcamp Change": SPCT},
              first_col=0.16, note=region_note),
        para("Takeaway:", (f"On current data, bootcamp field productivity rose faster than non-bootcamp peers in {_list(ahead)}"
                           if len(ahead) else "On current data, no region shows bootcamp agents clearly ahead of non-bootcamp peers")
             + (f", and slower in {_list(behind)}." if len(behind) else ".")
             + " These regional movements should be treated as indicative only until pending installations are assigned "
               "to regions; the overall Field Sales figures in Sections 1–3 are unaffected by this. "
               "Each region also has only a few bootcamp selling agents a month."),
    ]

    # 5. Drivers & shifts -------------------------------------------------------------
    drv = f["drv"]
    mp = t["mix_product"][t["mix_product"]["Product"] != "All others"]
    up, down = mp.nlargest(1, "Change (pp)").iloc[0], mp.nsmallest(1, "Change (pp)").iloc[0]
    ms = t["mix_source"][t["mix_source"]["Lead Source"] != "All others"]
    top_src, src_up = ms.iloc[0], ms.nlargest(1, "Change (pp)").iloc[0]
    blocks += [
        h2("5. Performance Drivers"),
        bullet("By segment:", ", ".join(
            f"{seg} {SNUM(drv.loc[seg, 'Added units/mo'])} units/month ({PCT(drv.loc[seg, 'Share of growth'])})"
            for seg in drv.index) + " of the cohort's growth."),
        bullet("Product mix:", f"{up['Product']} {PCT(up['Share (pre)'])} → {PCT(up['Share (post)'])} of units; "
                               f"{down['Product']} {PCT(down['Share (pre)'])} → {PCT(down['Share (post)'])} — this may "
                               "reflect portfolio or stock changes as well as selling behaviour."),
        bullet("Lead source:", f"{top_src['Lead Source']} remains the main channel ({PCT(top_src['Share (post)'])}, "
                               f"{top_src['Change (pp)']:+.0f} pp); {src_up['Lead Source']} grew {src_up['Change (pp)']:+.0f} pp."),
        bullet("STL cascade:", f"{PCT(f['cov']['unit_share_attended_stl'])} of post-bootcamp field units were sold under "
                               "an STL who attended, so there is no longer a large enough comparison group to confirm "
                               "the June ~30% finding."),
    ]

    # Top improvers (part of Performance Drivers) ---------------------------------------
    rows = [(seg, r["name"], r["pre"], r["post"], r["change"])
            for seg, top in t["top_improvers"].items() for _, r in top.head(3).iterrows()]
    imp = pd.DataFrame(rows, columns=["Segment", "Agent", pre_lbl, post_lbl, "Change"])
    imp.loc[imp["Segment"].duplicated(), "Segment"] = ""
    blocks += [
        bullet("Top improvers to recognise:", f"largest increase in units sold, {post_lbl} vs {pre_lbl}."),
        table(imp, formats={pre_lbl: F0, post_lbl: F0, "Change": SNUM}, first_col=0.18, text_cols=2,
              note="Units sold in each 4-month window."),
    ]

    # Methodology ----------------------------------------------------------------------
    blocks += [
        note("Method: Units = sum of Units where Category = Unit, by Sale Date. Productivity = units ÷ distinct selling "
             "agents per month (multi-month: total units ÷ agent-months). Field = FSA, POP, STL; Telesales = TeleSales "
             "Agent, Telesales Team Lead; SSC = Service Center Associate, SSCSales Agent. Cohort from 'Employee - Bootcamp Cohort'. "
             "Employee tenure = months between an agent's first sale month (used as the joining date; 'Analysis 2' tab, "
             f"MIN of Sale Month) and the bootcamp month ({meta['tenure_as_of'].strftime('%B %Y')}). Tenure is fixed at the "
             "bootcamp so each agent stays in one band across both comparison windows; agents whose first sale came after "
             "the bootcamp are reported separately and excluded from bootcamp vs non-bootcamp comparisons. "
             f"Source: {meta['source']}."),
    ]

    # References (footer) ----------------------------------------------------------------
    blocks += [
        h2("References & Additional Resources"),
        item("Sales Data - From Sales Dashboard >> Sales Details"),
        item("Sales Bootcamp Planner - by P&C"),
        item("Detailed Analysis"),
    ]
    return blocks


# ---------------------------------------------------------------------------
# Markdown renderer (drafting / review)
# ---------------------------------------------------------------------------
def _md_cell(text):
    return str(text).replace("|", "\\|")


def render_markdown(blocks, out_path):
    lines = []
    for b in blocks:
        kind = b["type"]
        if kind not in ("bullet", "item") and lines and lines[-1] != "":
            lines.append("")  # blank line before anything that isn't continuing a list
        if kind == "title":
            lines += [f"# {b['text']}", ""]
        elif kind == "h2":
            lines += [f"## {b['text']}", ""]
        elif kind == "bullet":
            lines.append(f"- **{b['lead']}** {b['text']}")
        elif kind == "item":
            lines.append(f"- {b['text']}")
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
            lines.append("| " + " | ".join(_md_cell(c) for c in b["columns"]) + " |")
            lines.append("|" + "|".join(":---" if i < b["text_cols"] else "---:" for i in range(len(b["columns"]))) + "|")
            for n, r in enumerate(b["rows"]):
                cells = [_md_cell(c) for c in r]
                if n in b["holdout_rows"]:
                    cells = [f"*{c}*" if c else c for c in cells]
                lines.append("| " + " | ".join(cells) + " |")
            lines.append("")
            if b["note"]:
                lines += [f"*{b['note']}*", ""]
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines).rstrip() + "\n")
    return out_path


# ---------------------------------------------------------------------------
# Word renderer (final stakeholder version)
# ---------------------------------------------------------------------------
def _shade(cell, fill):
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), fill)
    cell._tc.get_or_add_tcPr().append(shd)


def _set_cell(cell, text, bold=False, color=None, align=None, size=8.5):
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
    p.paragraph_format.space_after = Pt(2)
    run = p.add_run(text)
    run.italic = True
    run.font.size = Pt(7.5)
    run.font.color.rgb = MUTED


def _docx_lead(doc, lead, text, style=None, before=0):
    p = doc.add_paragraph(style=style)
    p.paragraph_format.space_before = Pt(before)
    p.paragraph_format.space_after = Pt(2 if style else 3)
    r = p.add_run(lead + " ")
    r.bold = True
    p.add_run(text)


def _docx_table(doc, b):
    cols, ncols = b["columns"], len(b["columns"])
    tbl = doc.add_table(rows=1 if b["header"] else 0, cols=ncols)
    tbl.style = "Table Grid"
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    tbl.autofit = False
    rest = (1 - b["first_col"]) / (ncols - 1) if ncols > 1 else 0
    widths = [Cm(PAGE_WIDTH_CM * (b["first_col"] if i == 0 else rest)) for i in range(ncols)]

    def align(i):
        return WD_ALIGN_PARAGRAPH.LEFT if i < b["text_cols"] else WD_ALIGN_PARAGRAPH.CENTER

    if b["header"]:
        for i, col in enumerate(cols):
            cell = tbl.rows[0].cells[i]
            _set_cell(cell, col, bold=True, color=WHITE, align=align(i))
            _shade(cell, HEADER_FILL)
    for n, r in enumerate(b["rows"]):
        cells = tbl.add_row().cells
        holdout = n in b["holdout_rows"]
        for i, text in enumerate(r):
            _set_cell(cells[i], text, color=MUTED if holdout else None,
                      bold=(not b["header"] and i == 0), align=align(i))
            if holdout:
                _shade(cells[i], HOLDOUT_FILL)
            elif n % 2:
                _shade(cells[i], ALT_FILL)

    for r_idx, tr in enumerate(tbl.rows):
        tr._tr.get_or_add_trPr().append(OxmlElement("w:cantSplit"))
        for i, cell in enumerate(tr.cells):
            cell.width = widths[i]
            if b["note"] or r_idx < len(tbl.rows) - 1:  # keep the whole table (and its note) on one page
                for p in cell.paragraphs:
                    p.paragraph_format.keep_with_next = True
    if b["note"]:
        _docx_small(doc, b["note"])


def render_docx(blocks, out_path):
    doc = Document()
    for section in doc.sections:
        section.left_margin = section.right_margin = Cm(1.9)
        section.top_margin = section.bottom_margin = Cm(1.6)
    normal = doc.styles["Normal"]
    normal.font.name = "Calibri"
    normal.font.size = Pt(10)
    normal.paragraph_format.space_after = Pt(3)

    for i, b in enumerate(blocks):
        kind = b["type"]
        if kind == "title":
            doc.add_heading(b["text"], level=1).paragraph_format.space_after = Pt(4)
        elif kind == "h2":
            h = doc.add_heading(b["text"], level=2)
            h.paragraph_format.space_before = Pt(10)
            h.paragraph_format.space_after = Pt(3)
        elif kind == "bullet":
            _docx_lead(doc, b["lead"], b["text"], style="List Bullet")
        elif kind == "item":
            doc.add_paragraph(b["text"], style="List Bullet").paragraph_format.space_after = Pt(2)
        elif kind == "para":
            _docx_lead(doc, b["lead"], b["text"], before=3)
        elif kind == "text":
            doc.add_paragraph(b["text"])
        elif kind == "note":
            _docx_small(doc, b["text"])
        elif kind == "table":
            _docx_table(doc, b)
        # A lead-in line directly before a table stays on the same page as it
        if kind in ("bullet", "para", "text") and i + 1 < len(blocks) and blocks[i + 1]["type"] == "table":
            doc.paragraphs[-1].paragraph_format.keep_with_next = True
    doc.save(out_path)
    return out_path


def generate(t, meta, out_path, fmt="md"):
    blocks = build_report(t, meta)
    return render_docx(blocks, out_path) if fmt == "docx" else render_markdown(blocks, out_path)
