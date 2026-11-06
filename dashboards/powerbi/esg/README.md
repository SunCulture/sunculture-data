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
| SDG Summary | One row per KPI, grouped by SDG (1, 2, 3, 5, 6, 7, 8, 9, 11, 13, 17) with the UN colour of each goal as a thin bar before the SDG name. One column per year; a Country slicer in the header. |

### Year columns

The year columns come from the field parameter `SDG Years`, which lists 2024 up to the current year (`YEAR(TODAY())`), so a 2027 column appears on 1 January 2027 with no change to the report. Measures `SDG 2024` … `SDG 2030` exist; add `SDG 2031` and its row in `SDG Years` before 2031.

Each cell is the KPI's value at 31 December of that year (today for the current year), by the row's *Basis*:

| Basis | Meaning |
|---|---|
| Cumulative | Everything up to the year end (e.g. customers, systems deployed, CO₂ avoided) |
| Snapshot | Current status only (e.g. active customers, retention, IoT connectivity); shown in the current year only, because no history is kept |
| Annual | That calendar year only (recruitment) |
| Pending | Data not yet available; the current-year cell says what is needed |

## Data model

15 tables, 59 measures (table `ESG Measures`), 6 relationships, Import mode.

| Table | Grain | Notes |
|---|---|---|
| `accounts_mv` | One row per account | Customers, sales, products, gender, status. `'Account Date'` (createdAt) → `Calendar`; `companyRegion` → `Country` |
| `mart_cds_responses` | One row per CDS (customer data survey) response | Credit history, diesel use, other income, hours spent irrigating; joined to accounts on `leadId` inside measures |
| `vw_employees` | One row per employee | Workforce, gender, age, supervisors; `'Joined Date'` → `Calendar` |
| `device_daily_usage` | One row per device per day | IoT pumping; feeds `IoT Usage` and `IoT Connection` |
| `customers` | One row per SunCulture Protect customer | `'Enrolment Date'` → `Calendar` |
| `policies` | One row per person per insurance product | **Active policies only**, renewals collapsed to one row with the first start date (the dataflow has ~2.8M rows, one per renewal). `'Cover Type'` maps the product to Climate / Device / Hospicash / Life |
| `products` | One row per insurance product | `id` comes through the dataflow as text (cast with `toString` in the ClickHouse query) |
| `SDG Summary` | One row per KPI | Calculated table: SDG, KPI, key, unit, basis, pending note, UN colour |
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

## Questionnaire

`questionnaire/sunculture_sdg_survey.xlsx` is the customer impact survey as an XLSForm for **KoboToolbox** (agent interviews in Kobo Collect, or customers via the web link). It collects the survey-based KPIs in the guide (income and poverty, food security, yield, resilience, women's land rights and time saved, indirect jobs, diesel use). Its `kpi_mapping` sheet links each KPI to its questions and calculation. Answers are linked to accounts by phone number.

## Open questions

- **Pending KPIs need an owner and a source:** % farmers trained on climate resilience (LearnInk), water withdrawn and consumed (IoT or meters), % employees with written contracts and with social protection (P&C), waste diversion (Warehouse), Scope 1–3 emissions and climate finance (MRV / Finance).
- **The survey KPIs are not on the dashboard yet.** They can be added as Pending rows once the survey runs.
- **Guide vs dashboard:** the guide still has an SDG 4 section (removed here), a single insurance KPI (split here), and hours saved under SDG 5 (moved to SDG 8).
- **Device insurance** is 4 people (0.0%).
- **`device_daily_usage` is stale** upstream (latest data 15 October 2026); connectivity, utilisation and MWh lag until the dataflow is fixed.
- Unused measures `Data Refreshed` and `SDG Summary Note` (their visuals were removed).
