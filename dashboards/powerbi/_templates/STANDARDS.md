# Power BI Standards

The checklist every SunCulture dashboard is built and reviewed against. Claude uses this file when reviewing or editing a dashboard, so keep it current.

## File format and repo

- [ ] Saved as **.pbip** (Power BI Project) and committed to git. Do not commit `.pbix` files (see `../.gitignore`).
- [ ] Folder: `dashboards/powerbi/<country>/<domain>/<Dashboard Name>/` (e.g. `ke/sales/`).
- [ ] `Readme.md` next to the project: purpose, owner, audience, data sources, refresh schedule, workspace.

## Data sources

- [ ] All tables come from **dataflows** (or a governed DB view). No Google Sheets, Excel-from-URL or local files in the dashboard itself; route them through a dataflow.
- [ ] **No hard-coded data** in DAX `DATATABLE`s or `Table.FromRows`. Targets, mappings and sort orders live in a maintained source.
- [ ] Business logic (status mappings, de-duplication, name fixes, country filters) runs **upstream** in the dataflow or SQL, not in the dashboard's Power Query.
- [ ] Only the columns the dashboard needs are loaded. No PII (ID photos, next-of-kin, credit scores, raw phone numbers) unless a visual needs it and access is restricted.

## Model

- [ ] **Auto date/time is OFF.**
- [ ] One `Date` table, marked as date table, with a contiguous date range. All fact dates relate to it.
- [ ] Star schema: facts → dimensions, **single-direction**, many-to-one. Any bidirectional or many-to-many relationship has a comment in the README explaining why.
- [ ] Row-by-row lookups (`CALCULATE(... FILTER(ALL(other)...))` in calculated columns, `LOOKUPVALUE`) are replaced by joins upstream.
- [ ] All measures sit in a `_Measures` table, in display folders (`Sales`, `Leads`, `Refunds`, `Targets`, `Agents`, …).
- [ ] Every measure has a **description** and a **format string**.
- [ ] Measure names follow the reporting terminology (e.g. "Selling Agents", "SSC", "Productivity (X)").
- [ ] Technical columns (keys, sort columns) are hidden.
- [ ] No unused tables, columns, measures or parameters.
- [ ] RLS roles defined where the audience goes beyond the owning team.

## DAX

- [ ] Time logic uses Date-table columns or time-intelligence functions, not `FORMAT(date, "yyyymm")` string comparisons.
- [ ] Division uses `DIVIDE()`.
- [ ] No `.[Date]` auto-date references.
- [ ] Variables used for repeated sub-expressions; no unused variables.

## Report

- [ ] Theme: `_templates/themes/sunculture.json`; layout, logo and colour rules in `_templates/BRAND.md` (no per-visual colour overrides unless they encode meaning).
- [ ] One canvas size across all pages: **1280 × 720** (vertical scroll allowed).
- [ ] Multi-page dashboards: the **navigation sidebar** on every page (`BRAND.md`, built with `tools/sidebar_nav.py` from a `nav.json` next to the `.pbip`), with a button for every page and the current page highlighted. No old menu buttons, page-tab button rows or navigation bookmarks.
- [ ] After adding, removing or renaming a page: update `nav.json` and rerun the tool, so the sidebar matches the pages.
- [ ] Chart colours in theme order; one series = slot 1; status colours only with icon + label.
- [ ] No dual-axis charts.
- [ ] **≤ 6 slicers per page.** Everything else goes in the filter pane or a drill-through page.
- [ ] ≤ ~20 visuals per page.
- [ ] Page order: Overview → analysis pages → detail/drill-through pages.
- [ ] Every visual has a plain-language title; no "Chart 7:" prefixes.
- [ ] A "Last refreshed" card on the overview page.
- [ ] No images or icons loaded from external URLs.
- [ ] No unused custom visuals embedded.
- [ ] No visuals or filters pointing to fields that do not exist.

## Before publishing

- [ ] Opened in Desktop with no errors; key KPI cards match the previous published version.
- [ ] Change described in the PR, with before/after screenshots for visual changes.
- [ ] Published to the **dev/test workspace** first, then promoted.
