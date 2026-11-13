# Operations Dashboard – Kenya: what every number means

Covers the **ALL JSFs** page and the two aftersales ticket pages (**All Aftersale Tickets** and **Testing & Repairs Aftersale Tickets**).

Terms used throughout:

- **JSF** – Job Sheet Form: one engineer visit (installation or aftersale). Each JSF is counted once by its installation ID.
- **Account** – a customer's sale record in AMT.
- **Ticket** – a SunDesk aftersales ticket.
- **Kenya only** – every number on these pages is limited to Kenya accounts.

---

## 1. ALL JSFs

### What the page always includes

- Only JSFs that have a status. JSFs with a blank status are left out.
- Only Kenya accounts.
- Only real products. These lines are left out: AfterSale, Agronomy, Extra Items, Installation, test, Training and Transport / Shipping.

### Filters at the top

| Filter | What it does |
|---|---|
| JSF Date | Visit date of the JSF. Saved to start on 1 Jan 2025 with no end date, so new JSFs appear automatically. For Tables 1–3 the same range is applied to the **sale date** (see below). Clear Filters removes the default, and the tables then show all sales since 2018. |
| Region | Customer's region from the account. |
| JSF Engineer | Engineer on the JSF. |
| JSF Type | INSTALLATION (includes add-on installations) or AFTERSALE. |
| Engineer Recom. | What the engineer recorded: Installed, Not Installed, and so on. |
| Account ref | Search one account. |
| Number in the top-right corner | How many filters are switched on right now. |

### Headline cards

| Card | What it counts | How it is worked out |
|---|---|---|
| **# JSFs Completed** | Every JSF in the date range, in any status | Distinct JSFs (installation ID). In-progress JSFs are included on purpose, so this matches the total JSFs raised. |
| **JSFs Syncing Rate** | Share of JSFs that have fully synced as completed | JSFs with status COMPLETED ÷ all JSFs, over the same date range. |
| **Installation Failure Rate** | Share of installation visits that failed because the product was faulty | Not Installed visits whose reason is Product Fault or Pump Fault ÷ all completed installation visits (Installed + Not Installed). |

### Engineering cards (turnaround)

| Card | Meaning | How it is worked out |
|---|---|---|
| **Installation TAT** | Average days from assignment to visit | Average of (JSF date − assignment date) in days, for the JSFs in the filters. |
| **Installation TAT (Median)** | Typical days from assignment to visit | Median of the same days, for visits marked Installed. A few very long jobs do not pull this number up the way they do the average. |
| **Installation Within SLA** | Installation JSFs done on time | Installation JSFs visited within **5 days** of assignment. |
| **Installation Out of SLA** | Installation JSFs done late | Installation JSFs that took **more than 5 days**. |

The SLA target is 5 days for installations and 2 days for aftersale visits.

### Charts

| Chart | What it shows |
|---|---|
| **Chart 1: MoM JSFs Distribution** | For each month of the JSF date: completed JSFs (status not In Progress or N/A), and installations marked Installed, split into PAYG and Cash accounts. |
| **Chart 2: JSFs by Region** | JSFs per customer region, coloured by JSF status. |
| **Chart 3: Top 10 Engineers by JSFs** | The 10 engineers with the most JSFs in the filters. |
| **Chart 4: JSFs by Engineer Recommendation** | JSFs per outcome recorded by the engineer (Installed, Not Installed, and so on). |
| **Chart 5: In-progress JSFs by Engineer** | JSFs not yet COMPLETED, per engineer. This is the open workload. |

### Tables 1–3: units sold, dispatched and installed

These three tables follow units from **sale** to **dispatch** to **installation**.

- Each row is the **month the unit was sold**, not the JSF month.
- The page **Date** filter applies to the sale date: only sales made inside the chosen date range are counted.
- Figures are units (product quantity, at least 1 per account), not accounts.
- **Same definition as the Sales Funnel:** only Category = **Unit** (pump kits). Add-ons (TV, drip, batteries), financial services (Farm Products, tanks, insurance) and services (TSR) are not installed, so they are left out. Refunded and Rejected accounts are left out.

| Number | Meaning | How it is worked out |
|---|---|---|
| **Total / Total sales** | Units sold that month | Units on Unit-category accounts with a sale date, not Refunded or Rejected. |
| **Shipped / Dispatches** | Units that have left the warehouse | Sold units that have a dispatch date **or** whose account already has an Installed JSF. An installed unit must have been shipped, even if nobody recorded the dispatch date. |
| **Pending dispatch** | Units sold but not yet shipped | Total − Shipped. |
| **Installed / Installations** | Units installed | Sold units whose account has a completed Kenya installation JSF marked Installed. |
| **Pending installation** | Units sold but not yet installed | Total − Installed. |
| **Pending Installation After Dispatch** | Units shipped but not yet installed | Shipped − Installed. |
| **% dispatched vs sales** | | Shipped ÷ Total. |
| **% installed vs dispatch** | | Installed ÷ Shipped. |
| **% Installed vs Sales** | | Installed ÷ Total. |
| **% pending installation vs dispatches** | | Pending installation after dispatch ÷ Shipped. |
| **% pending installation vs sales** | | Pending installation ÷ Total. |

Good to know:

- **Checked against the Sales Funnel on 13 Nov 2026:** 31 units sold on 12 Nov, 339 in November to date and 26,896 since 1 Jan 2025, the same in both. The Sales Dashboard's Sales Overview counts every category except TSR, so it shows more (369 in November).
- **Checked against ClickHouse on 12 Nov 2026** (before the Unit-only rule): for all sales from Jan 2025, all regions and products, the model and ClickHouse agreed exactly (37,020 sold, 30,966 shipped).
- **Distributor bulk orders inflate Pending dispatch.** About 2,960 of the 4,715 pending units are five distributor orders with no dispatch date in AMT (March 2025 and June 2026). This is why those two months stand out.
- **Installation is checked per account, not per unit.** If one unit on an account is installed, every unit on that account counts as installed.
- **Refunded and Rejected sales are removed**, so they never show as pending.

### Tables 4 and 5

| Table | Contents |
|---|---|
| **Table 4: Completed JSFs** | One row per JSF, with dates, account, outcome reason, engineer, device ID, location and department. |
| **Table 5: In-progress JSFs** | JSFs whose status is not COMPLETED, with engineer, type and location. Use it to chase open jobs. |

---

## 2. All Aftersale Tickets

### What the page always includes

SunDesk tickets of these types only:

- AfterSales
- Aftersales (Field)
- Battery Issue
- controller
- Controller & Device Fault
- Controller Issue
- Faulty sensor
- Locked Device
- Pump failure
- Pump Fault

### Filters at the top

| Filter | What it does |
|---|---|
| Ticket Period | Date range. **Raised** counts use the date the ticket was created. **Closed** counts use the date it was closed. |
| Ticket Creator, Customer Name, Product Type, JSF Engineer | Self-explanatory. |
| Status | Ticket state (open, closed, and so on). |
| Department | Ticket group. |
| Refurb / Non-Refurb | Taken from the product linked to the ticket's account: **Refurb** if the product name contains "refurb" or ends in "- R"; otherwise Non-Refurb. Tickets with no linked product are "Unlinked". |
| Reason | Ticket title. |
| Ticket number, JSF ID | Search one ticket or one JSF. |

### Cards

| Card | Meaning | How it is worked out |
|---|---|---|
| **Tickets Raised** | New tickets in the period | Distinct tickets created in the Ticket Period. |
| **Tickets Closed** | Work finished in the period | Tickets whose closed date falls in the period, whenever they were raised. |
| **Open at Period End** | Backlog | Tickets raised up to the last day of the period that were not closed by that day. |
| **Closed Within SLA** | Closed on time | Tickets closed in the period within the target days for their type (see below). |
| **Closed Beyond SLA** | Closed late | Tickets closed in the period after the target days. |
| **SLA Adherence** | On-time share | Closed Within SLA ÷ (Within + Beyond). Only closed tickets count, so open tickets do not drag it down. |
| **#JSFs** | Tickets that led to a visit | Tickets that have a JSF ID. |
| **#Assignments** | Tickets assigned to an engineer | Tickets that have a JSF assignment ID. |

**SLA target days by ticket type:**

| Ticket type | Target |
|---|---|
| Aftersales (Field) | 3 days |
| Installation | 5 days |
| Repossession | 14 days |
| Refund Request | 3 days |
| AMT Update, Customer Request, Locked Device | 2 days |
| Customer Request / Inquiry, Delayed Dispatch, Finance, Incident / Complaint, Problem / Failure | 1 day |
| Any other type | 2 days |

**Days to close** = closed date − created date, in whole days.

### Charts and table

| Visual | What it shows |
|---|---|
| **Chart 1: by Status** | Tickets per status. |
| **Chart 2: by Type** | Tickets per ticket type. |
| **Chart 3: by Ticket Group** | Tickets per department. |
| **Chart 4: MoM Trend** | For each month: tickets raised, closed and open at month end. |
| **Table 1: AfterSales SunDesk Tickets** | One row per ticket, with owner, creator, type, status, priority, reason, dates, days to close and SLA status. |

---

## 3. Testing & Repairs Aftersale Tickets

This page works the same way as All Aftersale Tickets, except it only includes tickets in the **Testing & Repairs (Kenya)** group.

The cards (Raised, Closed, Open at Period End, Within / Beyond SLA, SLA Adherence) have the same meaning as above.

Extra filters:

| Filter | Meaning |
|---|---|
| Failure Cause | The cause recorded by Testing & Repairs, e.g. "24V_OUT socket" or "No fault found". |
| Towns | Town on the linked account. |

| Visual | What it shows |
|---|---|
| **Chart 1: by Status** | Tickets per status. |
| **Chart 2: by Type** | Tickets per ticket type. |
| **Chart 3: Refurb / Non-Refurb** | Tickets split by whether the linked product is refurbished. Only tickets linked to an account with a region. |
| **Chart 4: by Product** | Tickets per linked product. Same rule as Chart 3. |
| **Chart 5: MoM Trend** | Raised, closed and open at month end, by month. |
| **Table 1: Ticket Type × Failure Cause** | Tickets raised for each type and failure cause. **%Tickets** is each row's share of all tickets in the table. Tickets with no failure cause are left out. |
| **Table 2: T&R SunDesk Tickets** | One row per ticket, including the failure cause. |

**Why the units-with-faults number is lower than the tickets number:** one unit can have several tickets. Ticket counts on these pages are tickets, not units.

---

*Data refreshed* (bottom-left card on each page) shows when the data was last loaded, in East Africa Time.
