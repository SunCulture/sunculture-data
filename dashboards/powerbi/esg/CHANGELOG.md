# Changelog

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
