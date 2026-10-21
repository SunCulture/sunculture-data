# Sales Dashboard – Ke: Architecture Review

**File reviewed:** `dashboards/powerbi/ke/sales/Sales Dashboard - Ke.pbix` (174 MB, Desktop 2.159, created from the Service, release 2026.02)
**Date:** 2026-10-20
**Method:** extracted the report definition from the .pbix, opened the file in Power BI Desktop, and read the live model through the local Analysis Services engine (TOM + storage DMVs). No changes were made to the file.

---

## 1. Summary

The dashboard works, but the model carries a lot of weight it does not use. Most of the clean-up is low-risk and mechanical, which is the kind of work Claude can do quickly once the file is saved as a **Power BI Project (.pbip)**.

| Area | Finding | Impact |
|---|---|---|
| Size | ~561 MB in memory; **56 of 80 tables** are hidden auto date/time tables (~86 MB) | Slower refresh and publish; nearer to capacity limits |
| Unused data | ~**127 MB** of column dictionaries sit in columns no visual, measure or relationship uses (accounts_mv 77 cols, leads_tunda 20, Customer 44, Customer Data Survey 42, refunds 24) | Size, refresh time, PII exposure |
| Targets | Targets come from **6+ places**: 4 hard-coded DAX `DATATABLE`s, 2 Google Sheets, 1 dataflow | Changing a target means editing and republishing the .pbix |
| Relationships | 27 real relationships: **14 bidirectional** (including the main `accounts_mv → calendar` link), **8 many-to-many**, 9 inactive | Ambiguous filter paths; totals that are hard to explain |
| Broken references | Visuals reference `Lead[...]`, `employees[...]`, `measures_table[...]`, `basic[#leads]`, which no longer exist in the model | Visuals or filters may error or silently do nothing |
| Report | 100 visuals, **54 of them slicers** (18 on *Sales Analysis*, 16 on *Leads Analysis*); 5 different canvas sizes | Slow page loads; inconsistent look |
| Dead weight | 7 embedded custom visuals (~3.3 MB) + 8 declared public visuals: **none used** | File size |
| Governance | 37 measures, 0 with descriptions; no RLS roles; PII loaded (national ID pics, next-of-kin, DOB, credit score, phone numbers) | Hard to hand over; data-protection risk |

---

## 2. Current architecture

```
Source DBs (AMT / ClickHouse / Salesforce)
        │
        ▼
Power BI Dataflows ── workspace 6b82f3d7…  (8 separate dataflows, ~1 entity each)
        │   accounts_mv, leads_tunda, customer_survey_mv, refunds + refund_categories,
        │   account_status_change, vw_employees, vw_refunds, sales_targets_ke
        ▼
Sales Dashboard - Ke.pbix   (Import mode, one file = model + report)
   ├─ + Google Sheets: actual_targets_sales_revenue1, stl_targets, weekly_targets
   ├─ + DAX DATATABLEs: daily_target, telesales_targets (from 2020), rsm_targets, Department Forecasts
   ├─ + calculated tables: calendar, calendar2, supervisors, analysis_type, sub_catogorySortTable
   └─ 5 report pages → published to the Service each time
```

### Tables in the model (24 real)

| Table | Source | Columns (used/total) | Notes |
|---|---|---|---|
| accounts_mv | Dataflow | 79/156 | 18 calc columns; 163 MB; main fact table |
| leads_tunda | Dataflow | 36/56 | 249 MB, 757k rows; the largest table |
| Customer | Dataflow `customer_survey_mv` | 9/53 | same entity as below |
| Customer Data Survey | Dataflow `customer_survey_mv` | 7/49 | **duplicate load** of the same entity |
| refunds / vw_refunds / refund_categories | Dataflows | 16/40, 4/6, 2/3 | refunds has 6 row-by-row lookup calc columns |
| vw_employees, Inactive accounts | Dataflows | 5/13, 2/3 | |
| target | Dataflow `sales_targets_ke` | 5/8 | |
| actual_targets_sales_revenue1 | Google Sheet | 1/7 | |
| stl_targets, weekly_targets | Google Sheet (xlsx export URL) | 1/3, 6/7 | month mapping hard-coded to 2024 |
| daily_target, telesales_targets, rsm_targets, Department Forecasts | DAX `DATATABLE` | – | **targets typed into the model** |
| calendar, calendar2 | DAX calculated | 13/19, 1/1 | **two date tables**, neither marked as date table |
| basic | Empty M table | – | used as the measure container (23 measures) |
| supervisors, analysis_type, product_subtitle_rank, sub_catogorySortTable | DAX / static | – | analysis_type appears unused |

Leftover queries: `Dates Query`, `amt_new`, `RangeStart`/`RangeEnd`, `Errors in accounts` are **not used by any table**. `RangeStart`/`RangeEnd` suggest someone started setting up incremental refresh, but no refresh policy is configured.

---

## 3. Findings and recommendations

Ordered by value for effort.

### 3.1 Quick wins (low risk, ~1 day)

1. **Turn off Auto date/time** (Options → Current file → Data load). This removes 56 hidden tables and ~86 MB. Two of these tables have ~455k rows each because `Customer[Date_of_Birth__c]` and `Customer Data Survey[Date_of_Birth__c]` contain dates spanning ~1,250 years. That **may suggest** placeholder or bad birth dates in the source, which is worth checking upstream.
   - Note: `leads_tunda[Leads Conversion (Same Day)]`, `[lead_to_sale_duration]` and `accounts_mv[inactivation_date]` use the `.[Date]` auto-date syntax. Rewrite these first (for example `INT([leads.createdAt]) = INT([ConvertedDate])`).
2. **Remove unused columns** at source or with `Table.SelectColumns` (Claude can generate the keep-lists). The largest are in `leads_tunda` (`leads.Id` vs `leads.leadId`, `phoneNumber`, `firstName`/`lastName`, `leads.updatedAt`) and `accounts_mv` (`warranty_card_pic`, `nationalIdFrontPic`, `externalId`, `first_name`/`last_name`…). The full list is reproducible from the review scripts.
3. **Delete dead objects:** `calendar2` (after moving its relationship to `calendar`), `analysis_type`, the 4 unused queries, 3 unused measures (`% Difference Vs Budget`, `% Difference Vs YTD(Previous Year)`, `Units Target YTD`), and the empty `accounts_mv[region_sales]` column.
4. **Remove the unused custom visuals** from the report (Format → … → Remove custom visuals), about 3.3 MB.
5. **Fix the broken references.** These visuals point to objects that no longer exist:
   - `Lead[Referral_Name__c]` (6 visuals on *Sales Analysis* and *Refunds*), `Lead[department]`, `Lead[Lead_Source]` (*Leads Analysis*)
   - `employees[employee_names]`, `accounts_mv[company_region]` (*Refunds Overview*)
   - `measures_table[sales_total]` (*Sales Overview*), `basic[#leads]` (*Leads Analysis*)

   Some may be leftover filter-pane entries rather than visible errors. Each should be removed or re-pointed.
6. **Replace the external traffic-light images** in `Department Forecasts[Symbol Dep]` (they load from a third-party WordPress site, `avising.com`) with conditional-formatting icons.

### 3.2 Model restructure (~1–2 weeks)

7. **One target table.** Consolidate `daily_target`, `telesales_targets`, `rsm_targets`, `Department Forecasts`, `stl_targets`, `weekly_targets`, `actual_targets_sales_revenue1` and `target` into one long-format table (`date | grain | department | sub_category | agent/rsm/stl | metric | value`), maintained in one sheet or DB table and loaded through the dataflow. This one change removes most of the "edit the .pbix to change a number" work and most of the many-to-many relationships.
8. **Star schema around a single Date table.** Keep `calendar`, mark it as the date table, and remove all bidirectional filters except where there is a written reason. Even the main fact relationship `accounts_mv[date] → calendar[Date]` is bidirectional today. `accounts_mv ↔ target`, `accounts_mv ↔ Department Forecasts`, `accounts_mv ↔ weekly_targets`, `refunds ↔ accounts_mv` and `leads_tunda ↔ accounts_mv` are many-to-many *and* bidirectional. That combination can cause filters to travel in ways nobody intended.
9. **Push logic upstream into the dataflow or SQL view.** Several transformations live in the .pbix but belong at source:
   - `accounts_mv`: the `nithio_account_status` mapping, name fixes (e.g. hard-coded supervisor name corrections), `Table.Distinct(account_id)`, first/last name split.
   - `leads_tunda`: `companyRegionId = 1` filter (currently all countries' leads are pulled and then filtered) and `Table.Distinct(mobilePhone)`. **Check this rule:** de-duplicating leads by phone number may drop legitimate repeat leads from the same customer, which could affect `# Leads` and conversion rate.
   - Calc columns that do row-by-row lookups across tables (`refunds[units]`, `[total_price]`, `[sale_date]`, `accounts_mv[location]`, `leads_tunda[ConvertedDate]`, `[Employee Primary Role]`) are better as joins in the dataflow. They slow refresh as tables grow.
   - Merge `Customer` and `Customer Data Survey` (same entity loaded twice).
10. **Simplify time-intelligence measures.** `sales_this_month`, `sales_week`, `sales_last_month` etc. compare `FORMAT()` strings over the calendar. Using calendar columns (`YearMonthNo`, a `RelativeMonth` offset) is faster and easier to read. `sales_last_month` falls back to "last December" when the previous month is blank, which looks unintended.
11. **Move measures into a `_Measures` table** with display folders (Sales, Leads, Refunds, Targets, Agents) and a one-line description on every measure. Claude can draft all 37 descriptions from the DAX for an analyst to approve.

### 3.3 Report and governance

12. **Slicers → filter pane.** Keep 4–6 high-use slicers per page (date, region, department, product) and move the rest (account id, customer id, phone number, lead id…) to the filter pane or a drill-through page. This is likely the biggest page-load improvement.
13. **One canvas size** (1920×1080 or 1280×720, with vertical scroll where needed) and one theme from `_templates/`.
14. **PII:** remove columns the dashboard does not display (national ID photos, next-of-kin, credit scores). Consider RLS by region or department before widening access. Export is set to *Allow summarized*; confirm that is intended.
15. **Repo hygiene:** a .pbix this size cannot be pushed to GitHub (100 MB file limit) and does not diff. Save as **.pbip** and commit that instead (see §4).

---

## 4. How Claude can help

### The enabler: .pbip

| Format | What it is | Can Claude edit it? |
|---|---|---|
| **.pbix** | Zip: report + **binary** data model + data | Read-only analysis (as in this review) |
| **.pbit** | Zip: report + model **schema**, no data | Readable, but still one zip blob; useful for distribution |
| **.pbip** | **Folder of text files**: report as PBIR JSON, model as TMDL | **Yes**: diffable, reviewable in PRs, editable by Claude |

This file already uses the PBIR report format internally, so saving it as .pbip is a one-step change: **File → Save as → Power BI project (.pbip)**. If the option is missing, enable it under *Options → Preview features*. The dataflow connection is unchanged, and the published report still works the same way.

### What Claude can do in practice

| Task | How | Human role |
|---|---|---|
| Review a dashboard against standards (like this one) | Read PBIP files + query the open model in Desktop | Decide what to act on |
| Add, rename or document measures; write DAX and M | Edit TMDL files directly | Open in Desktop, check numbers |
| Clean-up from §3.1 | Edit TMDL/PBIR; generate column keep-lists | Validate before publishing |
| Push logic upstream | Write dataflow M or SQL views | Review the SQL |
| New dashboard from template | Copy `_templates/`, apply theme, scaffold pages | Layout polish, sign-off |
| Bulk changes (rename a column across 30 visuals, apply theme, fix titles) | Scripted edits over PBIR JSON | Spot-check |
| Publish | Fabric Git integration or REST API from CI (service principal) | Approve the PR; production deploys stay human-triggered |

**Limits to plan around:** Claude cannot see the rendered page unless given a screenshot. Hand-authoring complex visuals in PBIR JSON is verbose, so Claude works best on modelling, DAX, bulk edits and reviews, while analysts keep visual layout. Every Claude change should be opened in Desktop before publishing. Fabric Git integration needs a Premium/PPU/Fabric-capacity workspace, so check the licence; on Pro, publishing stays manual or goes through the REST API.

### Suggested pilot

1. Save this file as .pbip and commit it (with the `.gitignore` in `dashboards/powerbi/`).
2. Claude applies §3.1 items 1–5 on a branch; an analyst validates in Desktop and compares the KPI cards against the current published version.
3. Measure the time taken against a normal change cycle. That gives a real number for the development-time question.

---

## Appendix: reproducing this review

Scripts used: extract parts from the .pbix zip; dump the model with TOM (`JsonSerializer.SerializeDatabase`) from the Desktop instance at `localhost:<port>`; storage sizes from `$SYSTEM.DISCOVER_STORAGE_TABLE_COLUMNS` / `..._COLUMN_SEGMENTS`; cross-reference PBIR field references against model objects. These can be packaged as a reusable `review` script under `_templates/tools/` if useful.
