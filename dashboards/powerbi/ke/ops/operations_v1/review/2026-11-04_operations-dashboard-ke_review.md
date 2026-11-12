# Operations Dashboard – KE: Architecture Review

**File reviewed:** `Operations Dashboard - KE.pbip`, saved from `Operations Dashboard - KE.pbix` (245 MB)
**Date:** 2026-11-04
**Method:** read the project files (TMDL model and PBIR report) with a script that inventories tables, sources, relationships, measures and every field reference in visuals, filters and bookmarks, then traces which objects the report actually uses. Checked against `_templates/STANDARDS.md`. No changes were made to the file. Memory sizes per column were not measured (the file was not open in Desktop); they can be added from the local engine.

---

## 1. Summary

The dashboard works on dataflows, which is right, but most of the model is unused, the report breaks several layout standards, and one visible page shows fields that no longer exist.

| Area | Finding | Impact |
|---|---|---|
| Size | **98 of 136 tables** are hidden auto date/time tables (Auto date/time is ON) | Slower refresh; larger file |
| Unused model | **700 of 972 columns** and **243 of 326 measures** are not used by any visual, filter, measure or relationship. 6 tables are not used at all (`cum`, `Funnel Stages`, `Matrix Header`, `Metrics`, `days_to_fail_index`, `sscs`) | Size, refresh time, maintenance |
| Calculated tables | `SLT_KPIS` has 162 calculated columns, 1 of them used. `points` has 65 calculated columns | Every refresh recomputes them |
| Row-by-row lookups | 40+ calculated columns use `LOOKUPVALUE` / `FILTER(ALL(...))` (`points`, `vw_jsf`, `Sundesk Tickets`, `schedule_ch`, `refunds`, `SLT_KPIS`) | Slow refresh; logic hidden in the dashboard instead of the dataflow |
| Broken fields | **19 field references** to tables or columns that no longer exist (`premises`, `turn_around_time`, `employee`, 7 `points` columns) on *CCRP JSFs*, *Warranty Status* and the hidden *Clients With JSF* | Visuals error or show blanks |
| Composite model | `credit`, `cum`, `schedule` are DirectQuery to the *Credit History Data* model in the **Test** workspace | Depends on a test dataset; slower visuals |
| Relationships | 28 real relationships: **9 bidirectional**, 13 inactive; three date tables (`calendar`, `calendar2`, `AS Calendar`) plus `Ticket Date` and `calendar3` | Ambiguous filter paths |
| Hard-coded data | DAX `DATATABLE`s (`Fail Age Bands`, `Funnel Stages`, `Metrics`, `Matrix Header`, `sscs`) and *Enter data* tables (`engineers`, `days_to_fail_index`) | Changing a mapping means republishing |
| Personal data | `engineers` (typed in) holds engineers' phone numbers and emails. `points`, `customer_survey_mv`, `leads_tunda` load national ID photos, ID numbers, next-of-kin, credit scores, phone numbers, GPS | Data-protection risk; **`engineers` must not be committed as is** |
| Report | 12 pages, **9 different canvas sizes** (up to 2560 × 1840); **3 hidden pages**; up to **23 slicers** (*Warranty Status*) and 57 visuals on one page; no navigation sidebar | Slow pages; inconsistent look |
| Dead weight | 11 custom visuals embedded; none of them is used on a page | File size |
| Leftovers | 6 database parameters (`db_replica_server`, `amt_db_server`, …) and `Errors in points` are not used by any table; two "Query Errors" groups from Jan and Aug 2025 | Clutter; server names in the file |
| Governance | 259 of 326 measures have no description; measures spread over 4 tables (`basic`, `Aftersales KPIs`, `Measures_`, `vw_jsf`…); no RLS | Hard to hand over |

---

## 2. Current architecture

```
Source DBs (AMT / ClickHouse / Salesforce / Sundesk)
        │
        ▼
Power BI Dataflows ── workspace 6b82f3d7…
        │   accounts_mv (as "points"), jsf_mv, mart_all_tickets, account_status_change, refunds,
        │   vw_refunds, fma_repo, vw_employees, leads_tunda, customer_survey_mv, Master Data, Sheet1
        ▼
Operations Dashboard - KE  (Import + DirectQuery composite model)
   ├─ + DirectQuery: credit, cum, schedule ← "Credit History Data" (Test workspace)
   ├─ + Enter data: basic (measure container), engineers, days_to_fail_index
   ├─ + DAX DATATABLEs: Fail Age Bands, Funnel Stages, Metrics, Matrix Header, sscs
   ├─ + calculated tables: calendar, calendar2, calendar3, AS Calendar, Ticket Date, date_periods,
   │     SLT_KPIS, schedule_ch, Unit Failures, Ticket Unit Link, KPIs, Aftersales KPIs, Measures_
   └─ 12 report pages
```

### Tables (38, excluding auto date/time)

| Table | Source | Columns used / total | Measures used / total | Notes |
|---|---|---|---|---|
| points | Dataflow `accounts_mv` | 86/206 | 0/1 | Main fact table; 65 calc columns, 8+ row-by-row lookups; PII |
| vw_jsf | Dataflow `jsf_mv` | 46/56 | 14/14 | Job status forms; 20 calc columns |
| Sundesk Tickets | Dataflow `mart_all_tickets` | 45/51 | 9/13 | 26 calc columns linking tickets to JSFs and accounts |
| customer_survey_mv | Dataflow | 6/62 | – | PII (next-of-kin, DOB, credit score) |
| leads_tunda | Dataflow | 3/46 | – | PII; joined to `points` on phone number (bidirectional) |
| refunds / vw_refunds | Dataflows | 5/14, 2/6 | – | |
| status_change, end_date, accounts_ch | Dataflow `account_status_change` / derived | 3/4, 2/3, 3/13 | – | |
| repossessions_fma | Dataflow `fma_repo` | 3/9 | – | |
| vw_employees | Dataflow | 10/21 | – | |
| Home3, Sheet1 (3) | Dataflows `Master Data`, `Sheet1` | 1/3, 1/11 | – | Names suggest spreadsheet uploads; confirm owners |
| credit, schedule | DirectQuery (Test workspace) | 5/47, 9/122 | 0/2 | |
| cum | DirectQuery (Test workspace) | – | 0/85 | **Not used** |
| basic | Enter data (empty) | – | 11/133 | Measure container |
| Aftersales KPIs, Measures_ | calculated | – | 46/61, 3/7 | Measure containers |
| SLT_KPIS | calculated | 1/168 | 0/9 | 162 calc columns; targets via lookups |
| schedule_ch | calculated | 9/35 | – | 21 calc columns, uses `.[Date]` |
| Unit Failures, Ticket Unit Link | calculated | 12/13, 2/2 | – | Failure analysis pages |
| calendar, calendar2, calendar3, AS Calendar, Ticket Date, date_periods | calculated | 4/18, 3/16, 1/1, 3/6, 2/6, 1/3 | – | Five date tables |
| Fail Age Bands, KPIs | DATATABLE / calculated | 3/3, 0/3 | – | |
| engineers, days_to_fail_index | Enter data | 1/7, 0/2 | – | `engineers` has phone numbers and emails |
| Funnel Stages, Matrix Header, Metrics, sscs | DATATABLE | 0 | – | **Not used** |

"Used" means referenced by a visual, filter or bookmark, or by a measure, calculated column, sort column or relationship that is itself used. Field parameters or measures referenced only by name in text are not detected, so check each removal in Desktop.

### Pages

| Page | Canvas | Visuals | Slicers | Notes |
|---|---|---|---|---|
| ALL JSFs | 2150 × 1600 | 27 | 9 | |
| Engineer SLA | 2100 × 1320 | 13 | 9 | |
| Warranty Status | 2200 × 1080 | 57 | **23** | 3 broken fields (`points[department_name]`, `[customer_type]`, `[refund_date]`) |
| Clients With JSF | 1280 × 720 | 11 | 8 | Hidden; table visual built on removed tables |
| CCRP JSFs | 1280 × 720 | 11 | 8 | **Visible; table uses removed `premises[...]` fields** |
| JSF Syncing TAT | 1880 × 1200 | 12 | 5 | Hidden |
| Cash Release Reconciliation | 1880 × 1200 | 11 | 5 | |
| All Aftersale Tickets | 2560 × 1840 | 25 | 9 | Hidden; near-copy of the next page |
| Testing & Repairs Aftersale Tickets | 2560 × 1840 | 27 | 9 | |
| Installations & Failures | 1920 × 1500 | 19 | 7 | Own tab buttons (3 action buttons) |
| Product Failure Occurrences | 1920 × 1900 | 24 | 7 | |
| Product Failure Rate | 1920 × 1600 | 22 | 6 | |

---

## 3. Recommendations

Ordered by value for effort.

### 3.1 Quick wins (low risk)

1. **Turn off Auto date/time** after rewriting the 4 `.[Date]` references in `SLT_KPIS` and `schedule_ch`. Removes 98 hidden tables.
2. **Delete unused objects:** the 6 unused tables, the 243 unused measures (122 in `basic`, 85 in `cum`), unused columns (keep-lists per table with `Table.SelectColumns`), the 6 unused parameters, `Errors in points`, the two "Query Errors" groups and the 11 custom visuals.
3. **Remove personal data:** move `engineers` to a dataflow (or `vw_employees`) without phone numbers and emails; drop ID photos, next-of-kin, DOB, credit scores and raw phone numbers that no visual needs.
4. **Fix or remove the broken fields** on *CCRP JSFs* and *Warranty Status*; decide whether the hidden pages (*Clients With JSF*, *JSF Syncing TAT*, *All Aftersale Tickets*) are still needed.

### 3.2 Model (medium effort)

5. **One date table.** Mark `calendar` as the date table and point the other date relationships at it (inactive where needed); remove `calendar2`, `calendar3`, `AS Calendar`, `Ticket Date` and `date_periods` once their visuals are moved.
6. **Move lookups upstream.** The JSF ↔ ticket ↔ account links (`Sundesk Tickets`, `vw_jsf`), engineer and supervisor names, refund dates and targets (`SLT_KPIS`) belong in the dataflow SQL.
7. **Replace the DirectQuery link to the Test workspace** with the dataflows behind it (`vw_credit_schedule`, as in *Credit History – Ke*), or point it at the production model.
8. **One `_Measures` table** with display folders (JSFs, Tickets, Installations, Failures, Warranty, Refunds) and a description on each measure.
9. **Single-direction relationships.** Replace the 9 bidirectional ones (`points` ↔ `calendar`, `status_change`, `accounts_ch`, `leads_tunda`, `refunds`) with measures that use `CROSSFILTER` where a two-way filter is really needed.

### 3.3 Report

10. Apply the SunCulture theme and `BRAND.md` layout: 1280-wide pages with vertical scroll, header, and the **navigation sidebar** (`nav.json` + `sidebar_nav.py`) in place of the per-page tab buttons.
11. **≤ 6 slicers per page**; move the rest to the filter pane. Split *Warranty Status* (57 visuals).
12. A "Last refreshed" card on the first page; plain titles on every visual.

### 3.4 Questions for the owner

- Who owns the dashboard and who uses which page? This decides which pages and hidden pages to keep.
- Are `Home3` (`Master Data`) and `Sheet1 (3)` (`Sheet1`) spreadsheet uploads? Who maintains them?
- Should `credit` / `schedule` keep coming from the *Credit History Data* model in **Test**?
- Is there a target source for the `SLT_KPIS` targets other than the lookups in the model?
