# Carbon Dashboard – KE

One Power BI dashboard for the carbon programme in Kenya. It replaces three separate reports (kept unchanged in `../carbon_v1/`): **Customer Contracts**, **Carbon credit CDS** and **Carbon Safeguards**.

| | |
|---|---|
| File | `Carbon Dashboard - KE.pbip` (PBIR report + TMDL model) |
| Audience | Carbon team, operations, P&C (safeguards) |
| Data | Power BI dataflows in the SunCulture workspace: `accounts_mv`, `jsf_mv`, `customer_contracts`, `mart_cds_responses`, `carbon_training_log`, `carbon_lsc_consultation_matrix` |
| Refresh | The published dataset refreshes weekly (Monday 10:00). Contract follow-up lists are up to a week old; a daily refresh is recommended. |

What every number means: [METRIC_GUIDE.md](METRIC_GUIDE.md).

## Pages

Every page is 1280 wide with the SunCulture sidebar (`nav.json`), header and filter pane. **Filter Here →** opens the pane over the header band (the score cards stay visible); the number next to it counts active filters; **Reset Filters** puts every filter back to its default (Country = Kenya on Contracts Overview, everything else cleared). Pages 2–5 are hidden from the page tabs; use the sidebar.

| Section | Page | What it shows |
|---|---|---|
| Contracts | Contracts Overview | Units sold, installed pumps, contracts submitted / outstanding, submission rate; monthly trend (latest 18 months), submission rate by region, sales cohorts by sale year, outstanding contracts by JSF engineer |
| Contracts | Customer Details | One row per installed account with its contract status (follow-up list) |
| Customer data | Carbon Credit CDS | Customer Data Survey (CDS) responses: customers by pump type and the CDS customer list |
| Safeguards | Training | Carbon training register by division / team and the carbon ambassador roster |
| Safeguards | LSC & Feedback | Local stakeholder consultations: reach by county, participants by round, issues raised |

## Definitions

| Number | Definition |
|---|---|
| **Installations** | **One pump = one contract.** Kenya accounts of Category *Unit* (pump kits; not ADDON) with a completed **INSTALLATION** JSF whose engineer recommendation is **Installed**. Each account counts once, at its latest such JSF, and adds its number of pumps (`units`, at least 1): a 10-pump distributor account is 10. Dated by that JSF's date. Accounts refunded after installation still count (the installed pump still needs a contract). |
| **Contracts Submitted** | Installed pumps whose signed contract is in the contract register with Received = Yes. |
| **Contracts Outstanding** | Installations − Submitted. |
| **Submission Rate** | Submitted ÷ Installations. |
| **Units Sold** | Same as the Operations dashboard and the Sales Funnel: `units` on Kenya accounts with a sale date, Category = Unit (no add-ons, financial services or TSR), not Refunded / Rejected, not test. Since 1 Jan 2025 = 26,917 in both dashboards (16 Nov 2026). |
| **Number of Customers (CDS)** | Distinct CDS responses (`cdsId`) linked to a Kenya account. |

**Why installations used to go missing.** Customer Contracts took "installed" from the account's latest-JSF summary in `accounts_mv` (which can lag, or point at a later aftersale or pending JSF) and required a device ID on the account. 111 of 405 accounts reported missing on 10 Nov were dropped for those reasons; most of the rest only needed a refresh. Installations now come from the installation JSF itself.

**Compared with the Operations dashboard.** Units sold match exactly. Installations match Operations' Table 2 *Installed* for the same sale-date cohort (20,530 vs 20,526 for sales since Jan 2025, 16 vs 13 Nov). Operations' *# JSFs Completed* card counts every JSF of every type and is not comparable.

## Data model

14 tables, Import mode, auto date/time off. ~141 MB in memory (was 189 MB after the first merge; the three originals loaded all 141 `accounts_mv` columns, 101 CDS columns and ~70 auto date tables).

| Table | Rows | Notes |
|---|---|---|
| `accounts_mv` | ~139k (Kenya only) | 17 of 141 columns. Calculated: `units`, `sale_date_only`, `Sale Year`, `Dispatch Month`, `Has Received Contract` (via the contracts relationship) |
| `jsf_mv` | ~61k | Only completed INSTALLATION JSFs marked Installed. Calculated: `Installed JSF`, `Latest Installation Flag` (latest per account), `Is Contract`, `jsf_date_clean` |
| `payg_cystomer_contracts` | contract register | 4 columns; one row per account ref; 1:1 with `accounts_mv` |
| `Customer Data Survey` | ~79k | 7 of 101 columns plus looked-up customer columns (Customer Name, Account Ref, Town, Account Type, Country), so the CDS table does not join every account per lead |
| `Calendar` | daily | Active on `jsf_mv[jsf_date_clean]`; inactive on the sale date and the account JSF date (used with `USERELATIONSHIP`) |
| Safeguards | small | `Training_Teams`, `Ambassadors`, `LSC_Events`, `LSC_Issues`, `LSC_Savings`, `County_Lookup`, `Grievances` (typed-in, empty) |
| `_Measures`, `Last Refresh` | – | All measures; refresh time in EAT |

The Kenya county map for *Consultation Reach x County* is `StaticResources/RegisteredResources/kenyan-counties….json` (4.8 MB).

## Customer data

No phone numbers, GPS or national IDs are loaded or shown. Customer names and account refs are shown on Customer Details and the CDS list. The model has no row-level security.

## Known limitations

- Power BI Desktop does not apply a slicer's saved selection when the file opens; click **Reset Filters** after opening.
- The contract register (`customer_contracts`) and the dataset refresh weekly, so recent months show few submitted contracts until the next sync.
- `accounts_mv` has a JSF date in 2028 (bad source data), so the Calendar runs to 2028.

## Open questions

- **Refresh schedule:** move the published dataset (and the `customer_contracts` sync) to daily.
- **Grievances:** the table is typed in and empty; connect it to the grievance log or remove it.
- **Training register names:** the `Training_Teams` query corrects a few staff names and division heads in code; move these fixes to the source sheet.
- **Publishing:** this replaces three published reports; agree which workspace and retire the old ones.
