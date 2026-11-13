# Operations Dashboard – KE

Kenya operations: installation job status forms (JSFs), engineer SLA, warranty status, cash release reconciliation, aftersale tickets, and product failure rates.

| | |
|---|---|
| Project | `Operations Dashboard - KE.pbip` (report in PBIR, model in TMDL) |
| Built from | `Operations Dashboard - KE.pbix` (245 MB), reviewed in `../operations_v1/review/2026-11-04_operations-dashboard-ke_review.md` |
| Owner | BI team (TBC) |
| Audience | Operations, engineering, aftersales |
| Data sources | Power BI dataflows in workspace `6b82f3d7…`: `jsf_mv` (as `vw_jsf`), `accounts_mv` (as `points`), `mart_all_tickets` (as `Sundesk Tickets`), `account_status_change`, `vw_employees` |
| Refresh | TBC |
| Workspace | not published from this folder |

## Pages

All pages are 1280 wide with the SunCulture header, the left sidebar (`nav.json`) and a hideable slicer pane: **Filter Here →** opens it over the header band only (two rows of filters, so the score cards stay visible), the number next to it counts active filters, **Reset Filters** puts every filter back to its default (JSF Date from 1 Jan 2025 on ALL JSFs and Engineer SLA, everything else cleared), and the "Data refreshed" time sits under the sidebar. Score cards sit in one row on every page.

What every number on ALL JSFs and the aftersales pages means: [METRIC_GUIDE.md](METRIC_GUIDE.md).

| Page | What it shows |
|---|---|
| ALL JSFs | 7 KPI cards, JSF trend, JSFs by region / engineer / recommendation, in-progress JSFs by engineer; then the Uganda-style overview by sale month (Tables 1–3: dispatch, installation, summary KPIs) and the completed / in-progress JSF lists |
| Engineer SLA | Turnaround and SLA by engineer |
| Warranty Status | Units under / out of warranty by account type, region, status and product |
| CCRP JSFs | CCRP job status forms |
| Cash Release Reconciliation | Engineer tasks and costs |
| All Aftersale Tickets | Hidden in view mode and not in the sidebar |
| Testing & Repairs Aftersale Tickets | Bench tickets by status, type, product and failure cause |
| Installations & Failures | Installation visits and failed installs |
| Product Failure Occurrences | Confirmed failure tickets by component, month and age |
| Product Failure Rate | Cumulative and per-install-month failure rates, refurb vs non-refurb, failure rate by part, product scorecard |

## Data model

16 tables, 115 measures (most in `_Measures`), 19 relationships (12 inactive, switched on with `USERELATIONSHIP`), Import mode. Auto date/time is off.

| Table | Grain | Notes |
|---|---|---|
| `vw_jsf` | One row per JSF | Install Outcome, Days Taken, SLA columns |
| `points` | One row per account | Sale, dispatch and install status; warranty status (from `warranty_expiry_date` and `TODAY()`) |
| `Sundesk Tickets` | One row per SunDesk ticket | Failure Cause, Failure Mode, Confirmed Failure, Failed Part |
| `Unit Failures` | One row per installed account | Calculated: first Installed JSF joined to its confirmed failures; pump type and part counts |
| `AS Calendar`, `Ticket Date`, `calendar` | Dates | Role-playing date tables; `AS Calendar` drives the aftersales pages |
| `Unit Parts`, `Fail Age Bands` | Axes | DAX `DATATABLE`s |
| `engineers` | One row per engineer | Typed-in table: Engineer, Contract Status, Location, Manager. Phone numbers, ID numbers and emails were removed before committing. |

**Bidirectional relationships (5):** `points` ↔ `calendar` (4, three of them inactive) and `points` ↔ `status_change`. They are carried over from the original file; they should be made single-direction once the visuals that rely on them are checked.

## Definitions

**JSFs (ALL JSFs page)**

| Card | Definition |
|---|---|
| # JSFs Completed | Every JSF in the date range, any status (as on the original dashboard) |
| JSFs Syncing Rate | JSFs with status COMPLETED ÷ all JSFs, same date range top and bottom |
| Installation Failure Rate | Not Installed visits caused by a product or pump fault ÷ all installation visits (Installed + Not Installed) |
| Installation TAT | Average Days Taken |
| Installation TAT (Median) | Median Days Taken for Installed / Not Installed visits; replaces "TAT (Without Outliers)", whose filter kept only three outcome reasons |

**Units sold, dispatched and installed (ALL JSFs Tables 1–3)**

| Measure | Definition |
|---|---|
| # Sales | `points[units]` (product quantity, at least 1) with a sale date, `Category` = Unit (pump kits; add-ons, financial services and TSR excluded), not Refunded / Rejected. Same definition as the Sales Funnel (13 Nov 2026: 31 units on 12 Nov, 339 in November, 26,896 since 1 Jan 2025 in both). When the page Date filter is set, only sales whose sale date is inside that range count |
| # Shipped | Sold units with a dispatch date or an Installed JSF |
| # Installed | Sold units whose account has an Installed JSF |
| Pending columns | Sales − Shipped / Installed; blank for months with no sales |

Rows are sale months; Installed means units from that month's sales installed by now, whenever that happened.

**Product failures**

- **Confirmed failure:** a Testing & Repairs ticket with a recorded failure cause other than No Fault Found. The Product Failure Occurrences ticket count equals the Testing & Repairs page filtered the same way.
- **Installed unit:** an account's first JSF with recommendation Installed (about 55,000 units, all time).
- **Failed unit:** an installed unit with at least one confirmed failure. Each unit counts once, at its **first** failure. Units With Faults (Occurrences page) can be a little higher because it also counts accounts with no Installed JSF.
- **Cumulative failure rate (Chart 1):** failed to date ÷ installed to date, at each month end.
- **Failure rate (Chart 2, cards):** of the units installed in the selected months, the share that has failed since. Recent months are low because those units are new.
- **Failure rate by part:** units whose first failure was this part ÷ that part in the field. Kits: RM2CK (Kubwa, FM1, FR2, Falcon) and surface pumps = 2 panels + pump + controller; RM2S = 1 panel + pump + controller. Controller covers the whole controller box (electronics, battery, switch, SIM / modem, firmware).

Failure tickets start in October 2025, when Testing & Repairs began recording failure causes in SunDesk; earlier failures are not in the data, so cumulative rates build up from there.

## Formatting

Follows `_templates/BRAND.md` with one exception: the report uses **Segoe UI** (the theme copy in `StaticResources` has Segoe UI and no font sizes) and small text (cards 8 / 16 pt, chart text 7 pt, titles 9 pt), as requested by the operations team. Chart and table colours from the original file (series colours, table fills, conditional formatting) are kept.

## Open questions

- **Do not publish over the current "Operations Dashboard - KE" yet.** The published Sales Funnel live-connects to that model and uses `basic[count_sales_funnel]`, `[count_funnel]`, `[active_sales_funnel]`, 13 `points` columns (incl. `cds_done`, `schedule_status`, `Engineer_Name`), `vw_jsf[customer_preferred_status]` and `leads_tunda`, which v2 does not have. Restore them (brings back customer and staff phone numbers) or repoint the Sales Funnel first. v2 is published to a personal workspace for review.
- **Sales Dashboard alignment:** its Sales Overview counts every category except TSR (369 in Nov 2026 vs 339 here). Decide whether it switches to Unit only (`basic[# Sales Units]`).
- **Date default on open:** Power BI Desktop does not apply the JSF Date slicer's saved 1 Jan 2025 selection when the file opens, so the page opens on all history; click **Reset Filters**. Check the behaviour in the Service after publishing.
- **Kit assumptions:** RainMaker 2.0 is treated as RM2S (1 panel). Climate Smart Direct kits (about 13,800 units) have no pump in the product name and are counted as 1 panel. Needs confirmation from aftersales.
- **First failure only on Product Failure Occurrences:** that page still counts every failure ticket (workload view). Decide whether it should switch to first failures.
- **Warranty Status:** proposed: donut shows Under vs Out only with a separate "Not covered" card; warranty expiry slicer open-ended; the status `"pending repossession on"` in `points[warranty_status]` looks like a typo for `"pending repossession"`.
- **Cash Release Reconciliation:** cost charts are empty because the cost columns in the `jsf_mv` dataflow are all 0 (already so before this revamp). Raise with the dataflow owner.
- **`jsf_mv`:** `account_id` changed type upstream; confirm with the dataflow owner.
- **`engineers`:** typed-in data; move to a dataflow or HR source so changes do not need a republish.
