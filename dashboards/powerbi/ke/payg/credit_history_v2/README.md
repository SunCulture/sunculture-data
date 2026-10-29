# Credit History – Ke

Monthly credit performance of the Kenya PAYG book: credit history, receivables aging, amortisation, wallet installment payments, unit economics, MFR, outstanding receivables, and month-on-month **retention, attrition and reactivation** of accounts by cohort.

| | |
|---|---|
| Project | `Credit History - Ke.pbip` (report in PBIR, model in TMDL) |
| Built from | `../credit_history_v1/Credit History -New (Ke).pbix`, plus 7 pages from `../payg_v1/PAYG Overview.pbix` |
| Owner | BI team (TBC) |
| Audience | Credit, collections, finance |
| Data sources | Power BI dataflows in workspace `6b82f3d7…`: `vw_credit_schedule` (live) and `credit_schedule` (monthly snapshot), `installment_payments`, `installment_schedules`, `accounts_mv` |
| Refresh | TBC |
| Workspace | not published |

## Pages

| Pages | Purpose |
|---|---|
| Monthly Credit History, Receivables Aging Monthly, Amortization Schedule, Wallet Installment Payments, Credit & Unit Economics, MFR Analysis, Outstanding Receivables | Credit history pages, as in the original file |
| Retention Rates, 60 Retention Rates, 90 Retention Rates, Attrition Rates, 60 Attrition Rates, 90 Attrition Rates, Reactivation Rates | Share of accounts in each cohort (installment month, rows) that stay current, roll into later aging bands, or recover, by months since each installment month (columns). Each page has a rate matrix and a count matrix; tab buttons switch between the 30/60/90-day versions. |

## Data model

59 tables (52 of them auto date/time tables), 76 measures, 56 relationships, Import mode.

| Table | Grain | Notes |
|---|---|---|
| `schedule` | One row per account per month, in two copies: `source = "live"` and `source = "snapshot"` (previous month-end) | Main fact table. Aging at the start and end of each month (`days_late_previous_month`, `days_late`, `aging_previous_index`, `aging_current_index`), `transition_type`, payments and balances come from the warehouse. Holds Kenya, Uganda and Côte d'Ivoire rows. |
| `CH`, `CH2` | One row per installment payment | Payment allocation (advance / current / arrears). |
| `installment_schedules` | One row per expected installment | |
| `accounts_mv` | One row per account | |
| `calendar3`, `analysis_type` | Lookups | |

### Transition measures (display folder *Transitions* on `schedule`)

| Measure | Counts account-months where |
|---|---|
| `retention(<30)` | at most 30 days late at the start **and** the end of the month |
| `retention(<60)` / `retention(<90)` | at most 60 / 90 days late at the end of the month |
| `attrition(<30)` | at most 30 days late at the start, 31–60 at the end, from the second installment month |
| `attrition(<60)` / `attrition(<90)` | 61–90 / 91–120 days late at the end of the month |
| `reactivation(<30)` | more than 30 days late at the start, at most 30 at the end, from the third installment month |
| `transition_base_rows` | all account-months in scope (denominator) |

Each `%…_rate` measure is its count divided by `transition_base_rows`. Scope: **snapshot copy, Kenya, `period >= 0`, months up to the plan's last installment** (`is_within_plan`). Months since each installment month come from the column `months_from_installment_to_current`.

**Definitions are PAYG Overview's, unchanged.** PAYG Overview only split the start of the month into "at most 30 days late" and "more than 30", so the <60 and <90 measures do not check where the account started. A corrected version that compares start and end bands is an open question for the metric owner.

**Agreement with PAYG Overview** (2,973 cohort × month cells, months 1 and later; Overview data mid-October, Credit History snapshot 30 September):

| Measures | Cells within 1 percentage point | Weighted rate, Overview vs Credit History |
|---|---|---|
| retention(<30), attrition(<30), reactivation(<30) | 97–99% | 65.7% vs 65.8%, 3.6% vs 3.6%, 1.7% vs 1.6% |
| retention(<60/<90), attrition(<60/<90) | 22–64% (90–97% within 5 pp) | e.g. retention(<90) 73.7% vs 76.2% |

Row counts agree to within 0.1%. The <60/<90 differences come from the lateness calculation: the warehouse's `days_late` and Overview's DAX lateness agree at 30 days but differ at the 60/90/120-day lines.

## Open questions

- **Broken fields on the original credit pages.** Visuals on *Monthly Credit History*, *Receivables Aging Monthly*, *Amortization Schedule*, *Wallet Installment Payments*, *Credit & Unit Economics* and their bookmarks reference tables that are not in the model (`payg`, `cum`, `payments`, `calendar`, `schedule[amount_due]`; 67 references). They were already broken in the source file.
- **Snapshot `period` values.** Pre-installment months are stored as 4,294,967,295 in the snapshot (−1 as an unsigned number); the live copy has negative values. The transition measures exclude them.
- **Auto date/time is on** (52 hidden date tables). `STANDARDS.md` asks for it off with one Date table.
- **Corrected <60/<90 definitions** (see above).
