# PAYG Dashboards – Ug: Architecture Review

**Files reviewed:**
- `dashboards/powerbi/ug/payg/PAYG Analysis - Ug.pbix` (191 MB, 10 pages)
- `dashboards/powerbi/ug/payg/PAYG Overview - Ug.pbix` (149 MB, 6 pages)

**Date:** 2026-10-21
**Method:** same as the [Ke PAYG review](../../../ke/payg/review/2026-10-21_payg-dashboards-ke_review.md). I extracted the report definitions, opened each file in Power BI Desktop and read the live model through TOM and the storage DMVs, then cross-referenced every visual field against the model. I also compared same-named measures and calculated columns with the Kenya files. No changes were made. Checklist references point to [`_templates/STANDARDS.md`](../../../_templates/STANDARDS.md).

These two files began as copies of the Kenya PAYG dashboards and have since diverged. Many findings in the Ke review apply here too and are only summarised below. This review focuses on what is different or worse for Uganda.

---

## 1. Summary

The main issue is **scale mismatch**. Each file reports on a small Uganda book (3,233 PAYG accounts in Analysis; 1,239 in Overview), but loads payment, schedule and status tables for **all countries**. As a result, most of the memory in both files holds rows that no Uganda visual can ever show.

| Area | PAYG Analysis – Ug | PAYG Overview – Ug |
|---|---|---|
| Size in memory | ~**541 MB** | ~**360 MB** |
| Uganda accounts in the model | 3,233 (`payg_clients`, 12 MB) | 1,239 (`PAYG_clients`, 8 MB) |
| Multi-country tables | `payments` 1.83M rows (**158 MB, unused**), `installment_payments` 1.93M, `installment_schedules` 1.34M, `account_status_change` 147k, `sales` 160k, `ptps` 78k | `CH` 1.93M rows (190 MB) + `CH2` copy (32 MB), `status_change` 147k, `Discounts` / `Addons` 160k each |
| Auto date/time | 78 hidden tables (~50 MB, incl. a 455k-row table from survey birth dates) | 117 hidden tables (~20 MB) |
| Broken objects | 2 visuals use `payg_clients[arrearsX]`, which does not exist | 12 calculated columns in error in `Collections_summary`; 3 visuals on *Collections Over Time* use fields that no longer exist |
| Definitions vs. Kenya | **29 of 102** same-named measures/columns differ from Ke PAYG Analysis | **35 of 256** differ from Ke PAYG Overview |
| Relationships | 23: **17 bidirectional**, 5 inactive | 43: 16 bidirectional, **14 inactive**; `Discounts` is a hub with 8 relationships |
| Model logic | 106 calculated columns, 73 measures (~29 unused) | 235 calculated columns, 159 measures (~90 unused) |
| Report | 131 visuals, 66 slicers/filters; 4 pages over the 6-slicer limit; 3 hidden pages; 3 canvas sizes | 67 visuals; 1 page over the limit; 2 canvas sizes (2160×1800 / 1860×1800) |
| Governance | 0 measure descriptions; no RLS; *Turaco Insurance* shows DOB, national ID and next-of-kin | 0 descriptions; no RLS; names, national ID and phone on 3 pages |

**What is already good:** all tables come from dataflows; the main client tables are filtered to Uganda in Power Query; Overview has few slicers and no embedded custom visuals; both files have a refresh-time card.

---

## 2. Current architecture

```
Dataflows (workspace 6b82f3d7…) — all entities hold every country
   accounts_mv · installment_payments · installment_schedules · vw_payments
   account_status_change · ptps · jsf_mv · customer_survey_mv · vw_payplan_changes
        │                                         │
        ▼                                         ▼
 PAYG Analysis - Ug.pbix                    PAYG Overview - Ug.pbix
 ├─ payg_clients: accounts_mv → PAYG + uganda    ├─ PAYG_clients: accounts_mv → PAYG + uganda
 │   (3,233 rows, 82 calc cols)                  │   + Kenya test/staff name exclusions (1,239 rows)
 ├─ payments (1.83M, all countries) ◄ unused     ├─ CH = installment_payments (1.93M, unfiltered)
 ├─ installment_payments / _schedules (all)      ├─ CH2 = SUMMARIZE(CH) (1.93M)
 ├─ sales, ptps, account_status_change (all)     ├─ Schedules (filtered by merge to Uganda clients)
 ├─ accounts_mv loaded 6× (payg_clients,         ├─ Vintage_data = clients × month-ends (20k rows, 83 calc cols)
 │   discount_notes, amtdb account_devices,      ├─ accounts_mv loaded 5× (PAYG_clients, PAYG_clients (2),
 │   sales, PAYG-UG, collectors)                 │   Discounts, Addons, Relationship Managers)
 └─ 10 pages                                     └─ 6 pages
```

---

## 3. Findings and recommendations

### 3.1 Fix first: correctness (≈1–2 days)

1. **Uganda numbers are defined differently from Kenya's, and differently between the two Uganda files.** *(Data sources › business logic upstream)*

   | Metric | Ke PAYG Analysis | Ug PAYG Analysis | Ug PAYG Overview |
   |---|---|---|---|
   | PAR30 base | accounts with `total_payment > 0` | accounts with `PaidAmount > 0` | vintage month-end, rebuilt `days_in_arrears` |
   | Arrears used in % Arrears / FPD | DAX `arrears2` | source `arrears` | rebuilt `Vintage_data[arrears]` |
   | FPD | status = Arrears | status ∈ {Arrears, Repossession, Pending Repossession} | `months_in_book = 1` and arrears > 10 |
   | Accounts in scope | all PAYG (Kenya) | all PAYG Uganda (3,233) | Uganda with a payment, not refunded/rejected, product and installment date set (1,239) |
   | Test/staff exclusions | 5 Kenyan names (report filter) | none | **the same 5 Kenyan names**, copied from Ke |

   Uganda's *Analysis* and *Overview* therefore count a different set of accounts, and a country comparison of PAR30 or FPD would compare differently defined numbers. The exclusion list in Overview contains names from the Kenya file, which **may suggest** it was copied across without a Uganda equivalent. Any Ugandan test accounts would then still be included. The 28-day month (`basic_measures[days]`) noted in the Ke review is also present here.
2. **Broken references** *(Report › no visuals pointing to missing fields)*:
   - Analysis: `payg_clients[arrearsX]` in 2 visuals on the *Overview* page.
   - Overview: `Schedules[monthly_expected_collections]`, `[…_current]` and `[…_arrears]` in 3 visuals on *Collections Over Time* (these columns exist on `Vintage_data`, not `Schedules`). There is also one reference to a deleted auto-date table.
   - Overview: the `Collections_summary` calculated table has 12 columns in error because `Vintage_data[YearMonth2]` no longer exists. No visual uses it, so it can be deleted.
3. **DAX issues carried over from Kenya**: `% PAR30(days late) 2` uses a different status list in numerator and denominator; ratios use `/` instead of `DIVIDE()`; `Expected_start_pay_date` is a text date; `.[Date]` auto-date syntax in several columns. See Ke review item 5. *(DAX checklist)*

### 3.2 Quick wins: size and clean-up (≈1–2 days)

4. **Filter every table to Uganda in Power Query, or better, in the dataflow.** This is the single largest saving *(Data sources › country filters upstream)*:
   - Analysis: `installment_payments`, `installment_schedules`, `account_status_change`, `Completion Date`, `Write off date`, `sales`, `ptps`, `JSF`, `Customer Data Survey`.
   - Overview: `CH` (and therefore `CH2`), `status_change`, `Write offs`, `Completed`, `Discounts`, `Addons`, `payplan_changes`.

   For a ~3k-account book, both models would **likely** fall well under 100 MB.
5. **Delete unused tables.** Analysis: `payments` (158 MB), `list`, `aging_dimensions2`, `My measures`, `calendar2`, and the relationship-only tables `tat`, `sales`, `PAYG-UG` and `amtdb account_devices` once their relationships are checked. Overview: `Collections_summary`, `Brackets`, `credit_unit_economics`, `Calendar_installment_date`, `Calendar_paid_date_calc`, `class`. *(Model › no unused tables)*
6. **Turn off Auto date/time** in both files. The largest auto-date table (455k rows, 33 MB) comes from `Customer Data Survey[Date_of_Birth__c]`, the same bad-birth-date pattern seen in the Ke Sales file. *(Model › Auto date/time is OFF)*
7. **Delete unused measures**: ~29 in Analysis and ~90 in Overview (51 of the 100 in `Basic_Measures` and 23 of 25 in `Schedules` are not used by any visual). *(Model › no unused measures)*
8. **Remove leftovers**: embedded custom visuals `heatmap` and `vdt_standalone` (~10 MB) plus 3 declared ones in Analysis; queries `Call Disposition` (Salesforce), `Case`, `Errors in payg_clients`, `Errors in Case`; and parameters pointing at the `sunculture-test` RDS host. *(Report › no unused custom visuals)*

### 3.3 Model restructure (≈2–3 weeks, shared with Kenya)

9. **One PAYG model for both countries, filtered by country.** Kenya and Uganda run four files that began as two templates and have drifted apart (29 and 35 differing definitions). A single shared PAYG semantic model with a `country` column, and RLS or report filters per country, would:
   - give both countries the same definitions,
   - turn four refreshes into one,
   - make fixes once instead of four times.

   This extends Ke review item 12.
10. **Move the credit engine upstream** (Ke review item 13). Here the payoff is mostly maintainability rather than size, because Uganda's `Vintage_data` is only 20k rows.
11. **Simplify relationships.** The many 1:1 bidirectional joins between `sales`, `discount_notes`, `PAYG-UG`, `PAYG Contracts` (Analysis) and `Discounts`, `Addons`, `PAYG_clients (2)` (Overview), most of them inactive, give the engine several possible filter paths. They are better as columns merged into the client table upstream. `accounts_mv` is loaded 5–6 times per file to feed these joins. *(Model › star schema)*
12. **One Date table**: Overview has 8 (`Calendar`, `Calendar_expected_date`, `Calendar_paid_date`, `Calendar_installment_date`, `Calendar_schedule_paid_date`, `Calendar_paid_date_calc`, `Calendar_vd`, `CalendarTable`). Analysis has 5. *(Model › one Date table)*
13. **RM assignment.** Analysis takes `relationship_manager` from `accounts_mv` through a `collectors` merge, which is better than Kenya's hard-coded `MOD()` rule. Overview builds a separate `Relationship Managers` table from the same source. Use one.

### 3.4 Report and governance

14. **PII and access** *(Data sources › no PII unless needed and restricted; Model › RLS)*:
    - *Turaco Insurance* (Analysis) shows date of birth, national ID, gender, next-of-kin name/phone and relationship.
    - *PAYG Clients* shows GPS, phone and next-of-kin.
    - Overview shows names, national ID and phone on *Aging Portfolio Balance*, *FPD* and *ArearsPer RM/STL/TSA/SCA*.

    Neither file has RLS, and export is *Allow summarized*. The insurance page may be better as a separate, restricted report.
15. **Slicers** *(≤ 6 per page)*: *PAYG Clients* has 16 and *Expected Payments Analysis* 12. **Canvas** *(1280×720)*: Analysis uses 3 sizes; Overview uses 2160×1800, about 2.5× the standard. **Hidden pages**: *PAYG Portfolio*, *Account Balances Per Customer* and *Reserve* in Analysis. Confirm they are still needed.
16. **Refresh time**: Overview's `Refresh Table` uses `DateTime.LocalNow()`, which returns UTC in the Service. The card **may** therefore show a time 3 hours behind Kampala.
17. **Save both files as .pbip** and commit. *(File format and repo)*

---

## 4. Suggested order

1. Agree the PAYG metric dictionary for **both countries together**. Kenya item 1 and Uganda item 1 above are the same conversation.
2. Save as .pbip; apply §3.1–3.2. The country filters (item 4) alone should remove most of the weight.
3. Build the shared PAYG model (item 9) and repoint all four reports.

---

## Appendix

Method and scripts as in the Ke reviews. The Ke ↔ Ug comparison matched measures and calculated columns by name (case-insensitive) and compared their expressions after removing whitespace. "Differs" therefore includes small edits as well as real logic changes. Sizes are dictionary + segment bytes and are approximate.
