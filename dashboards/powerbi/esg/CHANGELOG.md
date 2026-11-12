# Changelog

## 2026-11-12

- **Changed** model size: `accounts_mv`, `mart_cds_responses`, `customers`, `products` and `vw_employees` load only the columns the measures use (in memory ~507 MB before; `accounts_mv` was 274 MB with photos, contracts, names and phone numbers). Removes customer and staff personal data from the model.
- **Changed** `policies` to one row per person per cover type with three columns; cover type now worked out in Power Query (was a row-by-row DAX column). Insurance measures no longer filter on status (all rows are active).
- **Fixed** % customers replacing diesel / petrol pumps: counts only customers who said they owned a pump before (10.9% → ~6.3%).
- **Added** KPIs: PAYG sales and first-time borrowers (SDG 1); productive-use sales to women, women first-time borrowers, direct and indirect jobs by gender (SDG 5); direct jobs, % women in active workforce, indirect jobs, female turnover (SDG 8); second-life refurbished sales (new SDG 12); solar MJ generated in the year (SDG 13).
- **Changed** climate insurance and climate-resilience training moved from SDG 1 to SDG 13; added SDG 13 rows for tCO₂ (pumps sold, active pumps, lifetime), petrol avoided, diesel MJ and carbon credits; SDG 6 water purification.
- **Added** *Waiting survey* status for KPIs the SDG survey will collect; other gaps say *Pending – <source>*. No individual names in the report.
- **Changed** questionnaire (version 2026111202): Not sure and Prefer not to say separated; post-harvest loss question removed; food access labels Easier / Harder; well / borehole running-dry questions; fuel type and weekly fuel spend; new Water use section (source, pumping days, water pumped per day, uses, water purification system); `dashboard_methodology` sheet with every KPI's formula, source and status.

## 2026-11-04

- **Changed** the SDG Summary page to a single table: one row per KPI, a thin UN-colour bar before the SDG name, and one column per year from 2024 (field parameter `SDG Years`, adds the new year automatically). Removed the score and change columns, the note card, the data-refreshed card and the footer; Country slicer moved to the header.
- **Changed** the KPI list to the ESG team's proposal: SDG 4 removed; SDG 2 renamed *Zero Hunger* with active customers, total sales and Kilimo Boost; SDG 1 insurance split into climate and device; added % farmers trained on climate resilience (Pending, LearnInk) and % with an additional income source; SDG 3 now Hospicash (Turaco) and Life insurance; SDG 5 % women in leadership replaces retention by gender, promotion by gender removed; average daily hours saved moved to SDG 8; tanks under SDG 6.
- **Changed** `Active Customers` to the agreed account statuses (was funnel status "Active"), and total sales to accounts with a sale date and a full deposit date.
- **Added** tables `policies` and `products` (SunCulture Protect dataflow) and measures for climate, device, Hospicash and Life cover. `policies` keeps active policies only, one row per person per product.
- **Added** measures `% Women in Leadership`, `% Additional Income Source`, `Total Sales`, `Kilimo Boost Customers` (product `Farm Products`), `Water Tank Customers` (product `Tank`).
- **Changed** the theme colours to the UN SDG palette.
- **Added** `questionnaire/sunculture_sdg_survey.xlsx`, the KoboToolbox survey for the survey-based KPIs.

## 2026-10-29

- **Added** `ESG Dashboard.pbip` (single ESG Overview page).
