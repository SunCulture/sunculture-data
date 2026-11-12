# ESG Dashboard

SunCulture's KPIs against the UN Sustainable Development Goals: one table with a row per KPI and a column per year (2024 onwards). The KPIs and their definitions follow the ESG team's guide *SunCulture KPIs Mapped to UN Sustainable Development Goals (SDGs)*.

| | |
|---|---|
| Project | `ESG Dashboard.pbip` (report in PBIR, model in TMDL) |
| Built from | the *SDG Summary* page of `SDG Dashboard.pbix` |
| Owner | Austin (BI) |
| Audience | ESG team, management, impact reporting |
| Data sources | Power BI dataflows in workspace `6b82f3d7…`: `accounts_mv`, `mart_cds_responses`, `vw_employees`, `device_daily_usage`, and `customers`, `policies`, `products` (SunCulture Protect) |
| Refresh | TBC |
| Workspace | not published |

## Pages

| Page | Purpose |
|---|---|
| SDG Summary | One row per KPI, grouped by SDG (1, 2, 3, 5, 6, 7, 8, 9, 11, 12, 13, 17) with the UN colour of each goal as a thin bar before the SDG name. One column per year; **Show** and **Country** slicers in the header. The report stays a single page; the methodology lives in the questionnaire workbook (`dashboard_methodology` sheet). |

### Year columns

The year columns come from the field parameter `SDG Years`, which lists 2024 up to the current year (`YEAR(TODAY())`), so a 2027 column appears on 1 January 2027 with no change to the report. Measures `SDG 2024` … `SDG 2030` exist; add `SDG 2031` and its row in `SDG Years` before 2031.

Each cell is the KPI's value at 31 December of that year (today for the current year), by the row's *Basis*:

| Basis | Meaning |
|---|---|
| Cumulative | Everything up to the year end (e.g. customers, systems deployed, CO₂ avoided) |
| Snapshot | Current status only (e.g. active customers, retention, IoT connectivity); shown in the current year only, because no history is kept |
| Annual | That calendar year only (recruitment, solar MJ generated) |
| Waiting survey | Will come from the SDG impact survey (questions in `kpi_mapping`) |
| Pending | Data not yet available from a non-survey source (P&C records, MRV dashboard, carbon registry); the current-year cell says which |

**Show** (table `SDG Period`): *Cumulative to date* (default) or *In the year*. In the year shows, for cumulative count rows (#, kW, MWh, t), only what was added in that year (year-end value minus the previous year-end value); percentages and snapshots are unchanged.

## Data model

16 tables, 72 measures (table `ESG Measures`), 6 relationships, Import mode.

| Table | Grain | Notes |
|---|---|---|
| `accounts_mv` | One row per account | Loads only the 18 columns the measures use (the view has 140+, incl. photos and personal data). Customers, sales, products, gender, status. `'Account Date'` (createdAt) → `Calendar`; `companyRegion` → `Country` |
| `mart_cds_responses` | One row per CDS (customer data survey) response | 7 of 100+ columns loaded. Credit history, diesel use, other income, hours spent irrigating; joined to accounts on `leadId` inside measures |
| `vw_employees` | One row per employee | Workforce, gender, age, supervisors; `'Joined Date'` → `Calendar` |
| `device_daily_usage` | One row per device per day | IoT pumping; feeds `IoT Usage` and `IoT Connection` |
| `customers` | One row per SunCulture Protect customer | `'Enrolment Date'` → `Calendar` |
| `policies` | One row per person per cover type | Three columns: `idNumber`, `Cover Type`, `Policy Date`. **Active policies only**; cover type (Climate / Device / Hospicash / Life / Other / Unmatched) worked out in Power Query from the product name; renewals and repeat products collapsed to the first start date. Refresh still downloads the whole dataflow table (~1.3 GB, ~2.8M rows) because dataflow tables don't fold; see open questions |
| `products` | One row per insurance product | `id` comes through the dataflow as text (cast with `toString` in the ClickHouse query) |
| `SDG Summary` | One row per KPI (57) | Calculated table: SDG, KPI, key, unit, basis, pending note, UN colour |
| `SDG Period` | 2 rows | Disconnected table for the Show slicer |
| `SDG Years` | One row per year | Field parameter for the year columns |
| `Calendar`, `Country`, `Product Watts` | Lookups | `Product Watts` holds the pump wattage per product for kW and MWh |

### KPI definitions (non-obvious ones)

| KPI | Definition |
|---|---|
| Active customers | Accounts with status Complete, Completed, Complete - Over Paid, Current, Arrears, Advance, Repossession On Hold or Subscription Active |
| Total sales | Accounts with a sale date **and** a full deposit date, counted on the sale date |
| Climate / device insurance % | People (ID number) with an ACTIVE policy of that cover ÷ active customers. Climate = APA, ACRE Africa and El Niño products; device = SunCulture Gadget Insurance |
| Hospicash / Life insurance | People with an ACTIVE Basic Family Care (Turaco) / Credit Linked policy, KE and UG |
| Kilimo Boost / tanks | Customers with product `Farm Products` / `Tank` |
| % additional income source | CDS respondents naming another household income source ÷ those who answered |
| % women in leadership | Women among staff who supervise at least one person (`employee_supervisor_id`) |
| Renewable energy (MWh), CO₂ avoided | Installed unit-days × average kWh per device-day (IoT); CO₂ = MWh × 0.8 t |
| Solar MJ in year | (MWh at period end − MWh at previous 31 Dec) × 3,600 |
| % replacing diesel / petrol pumps | CDS: said they **owned** a pump before and named diesel or petrol ÷ asked about a prior pump (was 10.9% when "No" answers with a fuel type were counted; now ~6.3%) |
| PAYG sales / first-time borrowers | PAYG accounts under the total-sales rule / PAYG customers whose CDS credit history is "First-time borrower" (survey respondents only) |
| Women reached via productive use | Sales to women excluding TV, tanks, insurance, Farm Products, extras, aftersales, service lines and TSR |
| Direct jobs | Active people on the staff list (agents included) by gender (`Homme` = male) |
| Indirect jobs | 1 per sale (assumption), split by customer gender |
| Second-life sales | Sales of products named Refurb / Refurbished / ending "- R" |
| Avg daily pumping hours | IoT run-time ÷ days with data, last 120 days (2.8 h measured vs the 4 h design assumption) |

## Questionnaire

`questionnaire/sunculture_sdg_survey.xlsx` is the customer impact survey as an XLSForm for **KoboToolbox** (agent interviews in Kobo Collect, or customers via the web link). It collects the survey-based KPIs in the guide (income and poverty, food security, yield, resilience, women's land rights and time saved, indirect jobs, fuel type, litres and spend, water use and purification, wells running dry). Its `kpi_mapping` sheet links each KPI to its questions and calculation; `dashboard_methodology` lists every dashboard row with its formula, source and status (Live / Waiting survey / Pending) plus the key. Answers are linked to accounts by phone number.

## Open questions

- **`policies` refresh downloads ~1.3 GB.** Fix upstream: a dataflow / ClickHouse table from `soil_testing.policies` grouped to idNumber + cover type + first start date (a few MB). `customers` (117 MB, 3 columns used) likewise.
- **Waiting survey rows** (income saved from fuel, water withdrawn, water purification, climate training, tCO₂ by pumps sold / active / lifetime, petrol avoided, diesel MJ) become live once survey responses are loaded.
- **Pending non-survey rows:** full-time vs part-time staff, written contracts and social protection (P&C), waste diversion, Scope 1–3 and climate finance (MRV), carbon credits (registry).
- **Productive-use exclusions:** confirm `TSR` (2,939 accounts, 32 installed) is not a productive-use product.
- **Indirect jobs** assume 1 job per sale.
- **Guide vs dashboard:** the guide still has an SDG 4 section (removed here), a single insurance KPI (split here), and hours saved under SDG 5 (moved to SDG 8).
- **Device insurance** is 4 people (0.0%).
- **`device_daily_usage` is stale** upstream (latest data 15 October 2026); connectivity, utilisation and MWh lag until the dataflow is fixed.
- Unused measures `Data Refreshed` and `SDG Summary Note` (their visuals were removed).
