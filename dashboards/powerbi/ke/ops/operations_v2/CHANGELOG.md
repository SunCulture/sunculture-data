# Changelog

## 2026-11-13

- **Changed** slicer pane on every page: opens over the header band only (104 px, two 46 px rows), quieter styling (grey labels, white boxes, thin teal outline, outline buttons); page content starts under it so it never covers the score cards. *Warranty Status* and *CCRP JSFs* header cards moved into a card row; *ALL JSFs* cards in one row.
- **Changed** *Clear Filters* → **Reset Filters** (one bookmark per page): filters go back to their defaults (JSF Date from 1 Jan 2025 on *ALL JSFs* and *Engineer SLA*) instead of being wiped; full history stays selectable.
- **Changed** `# Sales` (Tables 1–3): Category = Unit only, not Refunded / Rejected, `points[units]`; same as the Sales Funnel. Restored `points[Category]` (dropped by the column trim). Tables 1–3 now follow the page Date filter on the sale date.
- **Fixed** pending measures return blank for months with no sales (no rows of zeros before 2025).
- **Added** `METRIC_GUIDE.md`: what every number on *ALL JSFs* and the aftersales pages means.

## 2026-11-12

- **Added** `Operations Dashboard - KE.pbip`, from `Operations Dashboard - KE.pbix` (see `../operations_v1/review/`).
- **Removed** unused tables, columns and measures, auto date/time tables and the DirectQuery link to the Test workspace (16 tables, 115 measures remain).
- **Removed** phone numbers, ID numbers and emails from the typed-in `engineers` table.
- **Changed** every page to the SunCulture layout: 1280-wide canvas, header, left sidebar (`nav.json`, *All Aftersale Tickets* left out), hideable slicer pane with a **Filter Here →** button and an active-filter count, "Data refreshed" under the sidebar, numbered centred titles, legends bottom centre. Text is Segoe UI at small sizes; original chart colours, table fills and conditional formatting kept.
- **Changed** *ALL JSFs*: kept the KPI cards and charts and added the Uganda-style overview tables by sale month (`points[Sale Month]`, *Overview* measures). *# JSFs Completed* still counts every JSF; *JSFs Syncing Rate* now uses the same date range top and bottom; *Installation TAT (Without Outliers)* replaced by *Installation TAT (Median)*.
- **Changed** `Sundesk Tickets[Confirmed Failure]`: a Testing & Repairs ticket with a failure cause other than No Fault Found.
- **Changed** `Unit Failures`: the install base is each account's first JSF with recommendation Installed.
- **Changed** *Product Failure Rate*: cards grouped with the chart they belong to, captions and chart subtitles; Chart 1 = cumulative failure rate with installed and failed to date; Chart 2 = failure rate by install month; Refurb and Non-Refurb rate cards; "1 in every N units" under each rate; *First 90 Days* shows "Too early" / "No data" instead of blank; Chart 4 replaced by *Table 1: Failure Rate by Part*.
- **Added** `Sundesk Tickets[Failed Part]`, `Unit Failures[Pump Type]`, `[Panels]`, `[Pumps]`, `[Controllers]`, `[TVs]`, `[Parts per Unit]`, `[First Failed Part]`, table `Unit Parts`, and measures *Parts in Field*, *Parts Failed*, *Part Failure Rate*, *Share of Failures*.
