# Sales Dashboard – Ug: Architecture Review

**File reviewed:** `dashboards/powerbi/ug/sales/Sales Dashboard - Ug.pbix` (56 MB, 10 pages)
**Date:** 2026-10-21
**Method:** same as the [Ke Sales review](../../../ke/sales/review/2026-10-20_sales-dashboard-ke_review.md). I extracted the report definition, opened the file in Power BI Desktop, read the live model through TOM and the storage DMVs, and cross-referenced every visual field against the model. No changes were made. Checklist references point to [`_templates/STANDARDS.md`](../../../_templates/STANDARDS.md).

---

## 1. Summary

This is a separate build from the Kenya Sales dashboard, with different tables (`Calculations`, `SupervisorsNew`, `leads_validation`) and different measure definitions. It is smaller and in some ways cleaner: targets come from a dataflow, and accounts and leads are filtered to Uganda in Power Query. The main risks are a **sales-team hierarchy typed into DAX** and **agent counts that may double-count** in the productivity measure.

| Area | Finding | Impact |
|---|---|---|
| Size | ~216 MB in memory; **78 of 95** tables are auto date/time (~58 MB), one with 455k rows from survey birth dates | Slower refresh; bad DOBs upstream |
| Unused data | `JSF` (64 MB, all countries) and `Customer survey` (16 MB) feed no visual; ~88 MB of unused column dictionaries | About two-thirds of the model could go |
| Hard-coded org structure | 7 calculated columns on `SupervisorsNew` map ~80 employee IDs to regions, STLs, supervisors, LGA supervisors and RSMs, with staff names typed into DAX | Every team change means editing and republishing the .pbix |
| Metric definitions | `Total FSA's` = Working Agents + Selling Agents, so agents who both log leads and sell are counted twice; `Productivity` divides by it | Productivity may be understated |
| Broken references | 11 visual fields point to objects that no longer exist (`Customers[…]`, `Lead[…]`, `basic[…]`, 4 `accounts` columns); 1 calculated column in error | Visuals or filters that error or do nothing |
| Relationships | 15: 5 bidirectional, 2 many-to-many (incl. leads ↔ accounts on **national ID**) | Ambiguous filters; conversion depends on ID matching |
| Report | 152 visuals, 67 slicer-type visuals; 6 of 10 pages over the 6-slicer limit; **9 different canvas sizes** (up to 2160×2400) | Slow pages, inconsistent layout |
| Dead weight | 9 embedded custom visuals (~4 MB), none used | File size |
| Governance | 65 measures, 0 descriptions, ~15 unused; no RLS; customer national ID and phone shown on 2 pages | Hand-over and data-protection risk |

---

## 2. Current architecture

```
Dataflows (workspace 6b82f3d7…)
   accounts_mv · leads_tunda · vw_employees · jsf_mv · customer_survey_mv
   refunds · refund_categories · Sheet1 (targets)
        │
        ▼
Sales Dashboard - Ug.pbix (Import)
 ├─ accounts         accounts_mv → uganda, Distinct(account_id)       18k rows
 ├─ refunds          accounts_mv again → uganda, refunded, no sale     600 rows
 ├─ leads_validation leads_tunda → companyRegionId = 3                 74k rows
 ├─ SupervisorsNew / TSA/SCA   vw_employees ×2 → phone starts "256"
 ├─ Targets          dataflow entity "Sheet1"
 ├─ JSF, Customer survey       all countries ◄ unused
 ├─ 5 date tables: calendar, calendar2, calendar3, calendarNEW, Calendar L&S
 └─ Calculations (35 measures), Month on Month Leads + Sales (calc table)
```

Uganda employees are identified by a phone number starting with "256". This **may** miss staff registered with a non-Ugandan number, or include others. A country field on `vw_employees` would be more reliable.

---

## 3. Findings and recommendations

### 3.1 Fix first (≈1 day)

1. **Agent counts and productivity** *(Model › measure names follow reporting terminology)*:
   - `Total FSA's` = `[Working Agents] + [Selling Agents]`. An agent who logged leads **and** made sales in the month is counted in both, and `Productivity` = sales ÷ `Total FSA's` is lower than it would be with a distinct count. If the intent is "agents active in the month", use a single distinct count, as the unused `Active Agents` measure already does.
   - `Cumulative working agents` / `Cumulative selling agents` add up **monthly** distinct counts. The same agent is counted once per month, so these are agent-months, not unique agents.
   - `Conversion` = units sold ÷ distinct leads. This mixes units with leads, so a multi-unit sale lifts conversion.
   - `Revenue` only includes PAYG accounts with a positive credit check (plus cash) and excludes extra items and TSR. `Unit sales` has the same filter **commented out**. Revenue and units therefore cover different account sets.
   - `Monthly_Target` always uses **today's** month and ignores the date slicer.
2. **Fix or remove broken references** *(Report › no visuals pointing to missing fields)*:
   - `Customers[CustomerLeads]`, `[ReferralStatus]`, `[employee_names]` (*Leads Analysis*, *Monthly Sales Analysis*, *Weekly Sales Tracker*, *LGAs/Super agents*)
   - `Lead[Referral_Name__c]`, `basic[Daily_target]`, `basic[sales_moving_average_7_days]`, `[…_30_days]`, `accounts[accountType]`, `[sale_date_clickhouse]`, `[total_price]` (*Ipos Sales Analysis*)
   - `accounts[total_amount]` (*Sales Overview*, *Sales Analysis*)
   - `accounts[refundReason]` is in error: it looks for `refunds[accountId]`, but the column is `account_id`. There is also an empty measure called `Measure`.
3. **Leads → sales matching** relies on national ID. `leads_validation[saledate]` scans `accounts` row by row for a matching `identification_number` (TRIM only), and a many-to-many **bidirectional** relationship links the two tables on the same ID. Leads without an ID, or with a differently formatted one, never convert. A customer with several accounts takes the latest sale date. A lead → account key from the source (`leads.leadId` on the account) would be more reliable. **Check this rule** with the sales ops owner.

### 3.2 Quick wins (≈1 day)

4. **Turn off Auto date/time** *(Model › Auto date/time is OFF)*. Check `Customer survey[Date_of_Birth__c]` upstream; dates spanning centuries **may suggest** placeholder values.
5. **Delete unused tables and queries**: `JSF`, `Customer survey`, `TSA/SCA` (relationship only), `calendar3`, `Dates Query`, and parameters `amt_db_server` (points at the `sunculture-test` host), `amt_new`, `db_replica*`. *(Model › no unused tables)*
6. **Delete ~15 unused measures** (`Repos Sold`, `Total sales`, `Count of account_id for (Blank)`, `Month on Month sales`, `Month on month leads`, `Sales_Vs_Target`, `Target_per_Month`, `Show Accounts`, …). `Month on Month sales` and `Month on month leads` also evaluate a variable inside `CALCULATE`, which does not re-evaluate it per month, so they return wrong values. *(Model › no unused measures)*
7. **Remove the 9 embedded custom visuals** (heatmap, multiKpi, HTML viewer, TBATS forecast, scrolling text, simpleImage, summaryTable, ZoomCharts, one more) and the declared but unused `kpiTicker`. Keep `textFilter`. *(Report › no unused custom visuals)*
8. **`Refresh_Time` is typed as a date**, so the refresh card can show the day but not the time. Change the type to datetime.

### 3.3 Model restructure (≈1 week)

9. **Org hierarchy as data, not DAX** *(Data sources › no hard-coded data)*. `SupervisorsNew[Region]`, `[STL]`, `[Supervisor ID]`, `[SA supervisor]`, `[LGA Supervisor]`, `[LGA Supervisor ID]`, `[SCA RSM]` and `TSA/SCA[TSA field agents]` override the HR data with typed-in employee-ID lists and names. Move these into a maintained mapping table (`employee_id | role | supervisor_id | stl_id | rsm_id | region | valid_from | valid_to`) loaded through a dataflow. Adding `valid_from/valid_to` would also let past months keep the structure that applied at the time.
10. **One Date table**: there are 5 today, all calculated in DAX. Keep one, mark it as date table, and replace `FORMAT(…,"yyyymm")` comparisons (`current_year1`, `current_month`, `month_sales`) with calendar columns. *(Model › one Date table; DAX › no FORMAT comparisons)*
11. **Single-direction relationships**: `accounts → calendar`, `refunds ↔ accounts`, `TSA/SCA ↔ SupervisorsNew` and the many-to-many `leads ↔ accounts` are bidirectional. *(Model › star schema)*
12. **Push logic upstream**: the product-to-warranty mapping (`warranty_status`), product grouping (`product_sub_title`, `Pump_Category`), `first_time_buyer` (scans all of a customer's accounts) and the refund reason/category lookups (`FIRSTNONBLANK` over `vw_refunds` per row) belong in the dataflow or SQL. *(Data sources › business logic upstream; Model › no row-by-row lookups)*
13. **`_Measures` table** with display folders (Sales, Leads, Agents, Targets, Refunds) and descriptions for the 65 measures. Several measures, e.g. `Productivity` and `Monthly_sales_vs_Target`, have no format string. *(Model › measures)*

### 3.4 Report and governance

14. **PII** *(Data sources › no PII unless needed; Model › RLS)*:
    - *Sales Analysis* and *Ipos Sales Analysis* show customer name, national ID and phone.
    - *Leads Analysis* shows lead phone, ID number and referrer phone.
    - *Sellling vs Working Agents* shows agent phone and gender.

    There is no RLS, and export is *Allow summarized*.
15. **Layout** *(Report › one canvas size; ≤ 6 slicers; ≤ ~20 visuals)*: 9 canvas sizes from 1280×600 to 2160×2400. *Ipos Sales Analysis* has 12 slicers and 22 visuals; *Sales Analysis* has 27 visuals. Apply the SunCulture theme and 1280×720 pages. Fix the page-name typo "Sellling".
16. **Cross-country consistency**: Kenya and Uganda Sales are separate builds with different measure names and logic. If regional or group reporting compares them, the "Selling Agents", "Productivity" and conversion definitions should be aligned. As with PAYG, a shared sales model with a country filter would be the long-term fix.
17. **Save as .pbip** and commit. *(File format and repo)*

---

## Appendix

Method and scripts as in the Ke reviews. "Unused" means not referenced by any visual, filter, bookmark, measure, calculated column, relationship or sort-by column. Sizes are dictionary + segment bytes and are approximate.
