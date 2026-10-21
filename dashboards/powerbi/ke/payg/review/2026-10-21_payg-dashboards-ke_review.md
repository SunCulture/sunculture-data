# PAYG Dashboards – Ke: Architecture Review

**Files reviewed:**
- `dashboards/powerbi/ke/payg/PAYG Analysis.pbix` (194 MB, 15 pages)
- `dashboards/powerbi/ke/payg/PAYG Overview.pbix` (415 MB, 9 pages)

**Date:** 2026-10-21
**Method:** same as the [Sales Dashboard review](../../sales/review/2026-10-20_sales-dashboard-ke_review.md). I extracted the report definitions from each .pbix, opened each file in Power BI Desktop (2.159), and read the live model through the local Analysis Services engine (TOM + storage DMVs). Then I cross-referenced every visual field against the model. No changes were made to either file. Checklist references (e.g. *Model › Auto date/time*) point to [`_templates/STANDARDS.md`](../../../_templates/STANDARDS.md).

---

## 1. Summary

These two dashboards report on the same PAYG book from the same dataflows, but each one rebuilds the credit logic itself in DAX, **in different ways**. The biggest risk is not file size. It is that **PAR30, "% at risk", FPD and arrears can show different numbers in the two reports** for the same portfolio. Both files are also heavy, and a large share of that weight is never shown.

| Area | PAYG Analysis | PAYG Overview |
|---|---|---|
| Size in memory | ~**495 MB** | ~**1,180 MB** |
| Unused data | `Credit History` (**199 MB**, 1.9M rows) used by nothing; ~123 MB of unused column dictionaries | `installment_payments` (**202 MB**, 1.9M rows) used by nothing; ~320 MB of unused column dictionaries |
| Auto date/time | **78** hidden tables (~27 MB) | **156** hidden tables (~48 MB) |
| Broken objects | **9** columns/measures in error (missing `payments` / `collections` tables) → PTP metrics cannot compute; 4 visuals point at fields that no longer exist | 1 column in error (missing `discount_notes`); 2 bookmarks filter on a column that no longer exists |
| Logic in the dashboard | 84 calculated columns on `payg_clients`; RM assignment hard-coded in DAX; CRB submission layout built in DAX | `Vintage_data` (1.6M rows, 57 calc columns) and `Schedules` (70 calc columns), both built row by row in DAX |
| Relationships | 14: **11 bidirectional**, 2 many-to-many | 24: 4 bidirectional |
| Report | 226 visuals, **133 slicers/filters**; 12 of 15 pages over the 6-slicer limit; 8 canvas sizes | 152 visuals (63 are nav buttons); 8 of 9 pages hidden behind button navigation; 1 canvas size |
| Dead weight | 2 embedded custom visuals (~10 MB) + 3 declared ones, all unused; unused DirectQuery link to the IoT telemetry cluster | 4 copy-pasted calendar queries; 6 date tables, none marked |
| Governance | 57 measures, 0 descriptions, ~26 unused; **no RLS** while pages show names, phones, next-of-kin, GPS, national ID, DOB, income | 80 measures, 0 descriptions, ≥44 unused; no RLS |

**What is already good:** both files take all their tables from dataflows in the same workspace (no Google Sheets or typed-in targets, unlike the Sales dashboard). Overview mostly uses single-direction relationships and one canvas size. Analysis has a "last refreshed" card.

---

## 2. Current architecture

```
Source DBs (AMT) ──► Power BI Dataflows ── workspace 6b82f3d7…
                       accounts_mv · installment_payments · installment_schedules
                       account_status_change · ptps · jsf_mv · fma_repo
                       mart_cds_responses · account_devices · vw_payplan_changes
                          │                                  │
                          ▼                                  ▼
             PAYG Analysis.pbix (Import + DQ)      PAYG Overview.pbix (Import)
             ├─ payg_clients (accounts_mv)          ├─ PAYG_clients (accounts_mv)
             │   + 84 DAX calc columns: balance,    │   + Schedules (70 calc cols)
             │   arrears2, PAR, FPD, RM, CRB fields │   + CH (installment_payments, 19 calc cols)
             ├─ Credit History  ◄── unused          │   + installment_payments ◄── unused
             ├─ Reported/Desired/Query1 (DirectQuery│   + CH2 = copy of CH (1.9M rows)
             │   → Azure Data Explorer IoT) ◄─ unused│   + Vintage_data = accounts × month-ends
             └─ 15 pages                            │     (1.6M rows, 57 calc cols)
                                                    └─ 9 pages
     Each file refreshes the same entities separately and computes its own credit metrics.
```

`accounts_mv`, `installment_payments` and `account_status_change` are loaded by **both** files. `installment_payments` is loaded **three times** in total and used once. `account_status_change` is loaded 3× in Analysis and 4× in Overview, each copy with a different status filter.

### Tables

**PAYG Analysis** (21 real tables)

| Table | Source | Rows | Size | Notes |
|---|---|---|---|---|
| payg_clients | Dataflow `accounts_mv` | 122k | 173 MB | 230 cols (84 calculated), ~80 unused; main fact |
| Credit History | Dataflow `installment_payments` | 1.93M | **199 MB** | **No visual, measure or relationship uses it** |
| jsf_mv | Dataflow | 121k | 42 MB | only feeds one calc column (`Engineer Name`); 18 of 29 cols unused |
| account_status_change, Completion Date, Write off date | Dataflow `account_status_change` ×3 | 147k / 36k / 2k | 23 MB | same entity three times; `Completion Date` unused |
| ptps | Dataflow | 78k | 12 MB | 5 calc columns in error (see §3.1) |
| collections_rm | DAX `SUMMARIZE` | 110k | 4 MB | RM per customer assigned by hard-coded rules |
| mart_cds_responses, fma_repo | Dataflow | 77k / 8k | 8 MB | lookup-only |
| calendar | DAX | 3.5k | 7 MB | not marked as date table; `Year` is text |
| Reported, Desired, Query1 | **DirectQuery** → Azure Data Explorer | – | – | IoT lock-status queries; **not used** by any visual |
| aging_dimensions, aging_dimensions2, income_groups, month_dimensions, list, Jsf Calendar | DAX `DATATABLE` / static | – | – | only `aging_dimensions` is used by visuals |

**PAYG Overview** (28 real tables)

| Table | Source | Rows | Size | Notes |
|---|---|---|---|---|
| Vintage_data | DAX `CROSSJOIN(accounts, month-ends)` | 1.59M | **283 MB** | 57 calc columns, many with `FILTER(ALL(...))` per row |
| CH | Dataflow `installment_payments` | 1.93M | 220 MB | 19 calc columns incl. two running totals |
| Schedules | Dataflow `installment_schedules` | 1.38M | 218 MB | 70 calc columns (aging, retention, attrition…) |
| installment_payments | Dataflow `installment_payments` | 1.93M | **202 MB** | **duplicate of CH; no visual or measure uses it** |
| PAYG_clients | Dataflow `accounts_mv` | 51k | 97 MB | 157 cols, ~98 unused |
| CH2, sch, PAYG_clients2/3, Monthly_collected | DAX calculated tables | up to 1.9M | 61 MB | copies/reshapes of CH and PAYG_clients |
| Write offs, Repossessions, Completed, status_change | Dataflow `account_status_change` ×4 | 2k–147k | 22 MB | |
| Calendar, Calendar_paid_date, Calendar_expected_date, Calendar_installment_date | M (same template pasted 4×, 2017–2030) | 4.8k each | 3 MB | 2 of the 4 unused; none marked as date table |
| Calendar_schedule_paid_date, Calendar_paid_date_calc, Calendar_vd | DAX | – | 18 MB | three more date tables |
| class, aging_start, aging_end, Brackets, credit_unit_economics | DAX static | – | – | `Brackets` and all 10 `credit_unit_economics` measures unused by visuals |

---

## 3. Findings and recommendations

Ordered by value for effort.

### 3.1 Fix first: correctness (≈1–2 days)

1. **The two dashboards define the same KPIs differently.** *(Model › single source of logic; Data sources › business logic upstream)*

   | Metric | PAYG Analysis | PAYG Overview |
   |---|---|---|
   | PAR30 | Snapshot: balance of accounts with `days_late_current > 30` ÷ balance, limited to accounts with a payment and an installment month ≤ this month | Monthly vintage: ending balance where the dashboard-rebuilt `days_in_arrears > 30` ÷ ending balance |
   | % at risk | `arrears2 ÷ balance` (both DAX-computed) | `arrears ÷ ending_balance` (vintage) |
   | FPD | paid < 1 installment **and** status = "Arrears" **and** `arrears2 > 0` | `months_in_book = 1` **and** `arrears > 10` |
   | Month length for expected payments | **28 days** (`basic_measures[days]` returns a hard-coded 28) in `expected_number_of_payments`, which feeds `expected_payment_amount` on the *Summary* page | 365.25 / 12 |
   | Test/staff accounts excluded | report-level filter: 5 named accounts + "contains test" | Power Query: 5 named accounts, **one of them different** + "contains Test" |
   | Statuses excluded | No Deposit, Refunded, Rejected, InActive | Refunded, Rejected, plus `Total_Paid > 0` |

   Each of these may be deliberate (snapshot vs. cohort views answer different questions), but nothing in either file says so. A user comparing the two reports **may see different PAR30 or FPD figures** for the same month without knowing why. **Recommendation:** agree one written definition per metric with the credit/collections owner, and name the variants explicitly where both are needed (e.g. "PAR30 (snapshot)" vs "PAR30 (vintage, month-end)").

2. **Analysis has three balances and two arrears figures, and mixes them.** `balance` (DAX), `PowerBI_balance` and `account_balance` (both from `accounts_mv`), `arrears` (source) and `arrears2` (DAX). The *Overview* page plots `arrears` in the combo chart and shows `arrears2` on the cards. The *CRB Report* uses source `arrears` with DAX `balance`. The `Balance Check` / `Balance Status` columns on *PAYG Clients* exist to spot where the DAX and system balances disagree, which **may suggest** known differences. Pick one source of truth (ideally the upstream value) and remove the others.
3. **PTP metrics are broken.** `ptps[paymentDate]` and `ptps[paidAmount]` reference a `payments` table that is no longer in the model, so 5 dependent columns (`days_late`, `ptp_kept`, `weighted_days_late`, …) are in error. As a result, *PTP kept %*, *collection effective index* and *avg days to pay* cannot compute. The *PTP* page is hidden, which **may suggest** it was hidden because of this. `repossession_cost` (missing `collections` table) and `pending_dispatch` are also in error. *(Report › no visuals pointing to missing fields)*
4. **Fix or remove broken visual references** *(Report › no visuals pointing to missing fields)*:
   - Analysis: `collections[collection_id]` (*PTP Details* table), `payg_clients[expected_first_payment_year_month]` (*FPD Rates*), `payg_clients[next_of_kin]` and `[next_of_kin_phone_number]` (*Turaco Clients*, probably renamed to `Next_of_Kin_Name__c` / `…Phone_Number__c`), and 3 slicers bound to a deleted auto-date table (*FPD Rates*, *PTP*, *Turaco Clients*).
   - Overview: `PAYG_clients[Discount Name]` (missing `discount_notes` table); 2 bookmarks filter on `company_region`, but the column is now `companyRegion`, so those bookmarks' Kenya filter **may silently do nothing**.
5. **Smaller DAX defects** *(DAX checklist)*:
   - `% PAR30(days late) 2`: the denominator includes status "pending repossession on" but the numerator does not, which understates the ratio.
   - Ten or more ratios use `/` instead of `DIVIDE()` (`% PAR60/90`, `% PAR30`, `right_party_contacts_rate`, `ptp_kept`, `avg_days_to_pay`, `non_performing_ratio`, …).
   - `Expected_start_pay_date` builds a **text** date (`m/d/yyyy`) that `start_pay_yr_month` then formats. This depends on locale and will break if the culture changes.
   - `%retention_rate(<30/60/90)` compute a blank-safe variable (`retention2`) and then return the unsafe one.
   - `.[Date]` auto-date syntax in 10+ columns (`Expected_last_pay_date`, `write_off_date`, `active_write_off`, `ptps[days_late]`, `Vintage_data[4_months_ago]`, `Schedules[status_change]`, …). Rewrite these **before** turning off Auto date/time.

### 3.2 Quick wins: size and clean-up (low risk, ≈1–2 days)

6. **Delete unused tables.** Analysis: `Credit History`, `Completion Date`, `Jsf Calendar`, `list`, `income_groups`, `aging_dimensions2`, and the DirectQuery trio `Reported`/`Desired`/`Query1`. Removing the DirectQuery tables also turns Analysis back into a plain Import model and removes the need for Azure Data Explorer credentials in the Service. Overview: `installment_payments`, `Calendar_expected_date`, `Calendar_installment_date`, `Brackets`, `credit_unit_economics`. Together that is about **200 MB from each file** before any column work. *(Model › no unused tables)*
7. **Turn off Auto date/time** in both files (234 hidden tables, ~75 MB). Rewrite the `.[Date]` columns from item 5 first. *(Model › Auto date/time is OFF)*
8. **Remove unused columns** with `Table.SelectColumns`: ~80 in Analysis `payg_clients`, ~98 in Overview `PAYG_clients`, 13 in `CH`, 23 in `Schedules`, 14 in `Vintage_data` (incl. `vd_id`, an 88 MB dictionary). Claude can generate the keep-lists. *(Data sources › only needed columns)*
9. **Delete unused measures**: ~26 in Analysis (e.g. `% At Risk`, an exact duplicate of `% Arrears`; `% PAR60`; `% PAR90`; `%fpd_accounts2/3`; `date_selected` = `DATE(2020,1,27)`) and ≥44 in Overview. *(Model › no unused measures)*
10. **Remove unused custom visuals** from Analysis: embedded `heatmap` and `vdt_standalone` (~10 MB), plus the declared `advancedtrellis`, `lolipopthree`, `threejs`. Keep `textFilter`, which is used. *(Report › no unused custom visuals)*
11. **Remove leftover parameters and queries**: `db_replica_server`, `amt_server` (points to a `sunculture-test` RDS host), `amt_new`, `amt_db`, `db_host`, `db_name`, `old_db_name`, `Errors in payg_clients`, `Errors in CH`. None of these is used.

### 3.3 Model restructure (≈2–3 weeks)

12. **One shared PAYG semantic model, two thin reports.** Both reports read the same entities, so build one `PAYG Portfolio – Ke` model that holds the agreed definitions from item 1, and connect *PAYG Analysis* and *PAYG Overview* to it as thin reports (live connection). This removes duplicate refreshes, removes the definition drift, and halves the places a metric has to be fixed.
13. **Move the credit engine upstream.** The heaviest logic runs as DAX calculated columns over millions of rows. Many columns use `FILTER(ALL(table))` or `EARLIER` per row, which scales badly and **likely** dominates refresh time:
    - `Vintage_data`: account × month-end grid with running totals (`cum_expected_amount`, `monthly_cum_amount_paid`, `last_due_date`, `days_in_arrears`…).
    - `Schedules`: per-installment lateness, aging transitions, retention/attrition/reactivation flags.
    - Analysis `payg_clients`: `balance`, `arrears2`, `expected_number_of_payments*`, `fpd_client`, `status_change`, PTP lookups.

    These would sit better as tables built by the data pipeline, for example a monthly `mart_payg_account_month` (account, month-end, expected, paid, balance, days in arrears, status) and a `mart_payg_installments` table in ClickHouse via Dagster. That is the same pattern planned for ESG. Power BI then only aggregates. **Check this rule:** `Table.Distinct(account_id)` in Analysis keeps an arbitrary row when `accounts_mv` has duplicates, which may hide a data issue upstream.
14. **Move hard-coded mappings into maintained sources** *(Data sources › no hard-coded data)*:
    - `collections_rm[rm]` assigns relationship managers by region and `MOD(customer_id, n)`, with staff names typed into DAX, and `Station` maps those names to SSCs. Every staffing change means editing and republishing the .pbix, and the split by customer-id parity may not match how accounts are actually allocated. This belongs in an RM-assignment table maintained by the collections team.
    - County-name fixes (`count_redone`), department grouping (`department_redone`), and status lists repeated in `status_change` (4 spellings of "Complete – Over Paid"; the spelling variants **may suggest** status values are not normalised at source).
    - Test/staff-account exclusions: move them to one flag upstream (`is_test_account`) instead of name lists in two places.
    - Country filter: both reports filter `company_region = kenya` at report level, so all countries are loaded and then hidden. Filter in the dataflow. *(Data sources › country filters upstream)*
15. **One Date table** per model, marked as date table, with role-playing via `USERELATIONSHIP` instead of 6–7 calendar copies. Replace `FORMAT(date,"yyyymm")` comparisons (`month_before`, `thefuture`, `inactive`, `w/o`, `repo`…) with calendar columns. *(Model › one Date table; DAX › no FORMAT string comparisons)*
16. **Relationships:** make the 11 bidirectional relationships in Analysis single-direction unless there is a written reason. That includes the main `payg_clients → calendar` link and the many-to-many `mart_cds_responses ↔ payg_clients` and `Reported ↔ payg_clients`. Most of the 1:1 joins to status tables (`Write off date`, `fma_repo`, `account_status_change`) could be columns merged upstream instead. *(Model › star schema)*
17. **`_Measures` table with display folders and descriptions** (Portfolio, Arrears & PAR, FPD, Collections, PTP, Vintage) in both files. Claude can draft the 137 descriptions from the DAX for an analyst to approve. *(Model › measures in `_Measures`, descriptions + format strings; several measures, e.g. `balance` and `fpd_balance`, have no format string)*

### 3.4 Report and governance

18. **Access and PII** *(Data sources › no PII unless needed and restricted; Model › RLS)*. Unlike the Sales dashboard, these pages **display** personal data on purpose: customer names, phone numbers, GPS, next-of-kin names/phones (*PAYG Clients*, *Written Off Clients*, *Turaco Clients*), plus national ID, date of birth and income (*CRB Report*). Neither file has RLS, and export is set to *Allow summarized*, which for table visuals still exports row-level data. Consider:
    - RLS by region/RM for collections users.
    - Moving the *CRB Report* page into a separate, restricted report.
    - Checking who has access to the workspace.
19. **CRB submission built in the dashboard.** The *CRB Report* page assembles a credit-bureau layout in DAX: lender name, branch code and product type as constant columns, prudential classification, and names split on spaces into Forename/Surname. If this table is exported and submitted, the name splitting may misassign one-word or four-part names, and no test protects the format. A regulatory file would be safer generated by the pipeline with validation checks, with Power BI used only to review it.
20. **Slicers → filter pane** *(Report › ≤ 6 slicers per page)*. Analysis has 133 slicer/filter visuals; *PAYG Clients* alone has 21 (and 32 visuals in total, over the ~20 limit). Keep date, region, product, status and RM on the canvas; move account ref, device id, tranche, amortisation day etc. to the filter pane or a drill-through page.
21. **Canvas and layout** *(Report › one canvas size 1280×720; page order)*. Analysis uses 8 sizes (up to 2220×1080). Overview uses 1520×720 throughout. Overview's 3 retention, 3 attrition and 1 reactivation pages are near copies that differ only by the 30/60/90 bucket. One page with a field parameter or a bucket slicer would replace 7 pages and ~40 navigation buttons.
22. **Add a "Last refreshed" card to Overview**, and apply the SunCulture theme from `_templates/` to both files. Analysis uses a 2018 base theme; Overview uses a 2021 one. *(Report › theme; last refreshed card)*
23. **Repo hygiene:** both .pbix files exceed GitHub's 100 MB limit (Overview is 415 MB). Save as **.pbip** and commit that instead. *(File format and repo)*

---

## 4. How Claude can help

The tooling and the .pbip workflow are the same as in the Sales review (§4 there). For these two dashboards specifically:

| Task | Human role |
|---|---|
| Draft a one-page **PAYG metric dictionary** (PAR30, % at risk, FPD, arrears, exclusions) from the DAX in both files, showing where they differ | Credit/collections owner decides the definitions |
| §3.1 fixes and §3.2 clean-up on the .pbip (TMDL/PBIR edits, keep-lists, broken references) | Open in Desktop; compare KPI cards to the published version |
| Translate `Vintage_data` / `Schedules` / `arrears2` logic into ClickHouse SQL for Dagster assets, with reconciliation queries against the current DAX output | Review SQL; sign off the reconciliation |
| Build the shared semantic model and repoint both reports to it | Validate pages; approve the publish |
| Draft measure descriptions (137) and display folders | Approve |

### Suggested order

1. Agree the metric definitions (item 1). This is a business decision and should come before any rebuild.
2. Save both files as .pbip and commit them (with the `.gitignore` in `dashboards/powerbi/`).
3. Claude applies §3.1 and §3.2 on a branch. An analyst validates in Desktop. Expect the Overview file to drop well below its current ~1.2 GB in memory.
4. Build the upstream PAYG marts, then the shared model (§3.3).

---

## Appendix: reproducing this review

Same scripts as the Sales review: extract the .pbix zip (minus `DataModel`); dump the model with TOM (`JsonSerializer.SerializeDatabase`) from the Desktop instance at `localhost:<port>`; storage sizes from `$SYSTEM.DISCOVER_STORAGE_TABLE_COLUMNS`, `…_COLUMN_SEGMENTS` and `…_STORAGE_TABLES`; object `state`/`errorMessage` from TOM for broken columns and measures; cross-reference PBIR field references and table usage (visuals, DAX, relationships) against model objects. Sizes are dictionary + segment bytes and are approximate. "Unused" means not referenced by any visual, filter, bookmark, measure, calculated column, relationship or sort-by column.
