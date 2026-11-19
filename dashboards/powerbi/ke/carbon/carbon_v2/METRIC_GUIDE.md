# Carbon Dashboard – Kenya: what every number means

Terms used throughout:

- **Unit account** – a customer account for a pump kit (Category = Unit). Add-ons (batteries, TVs, drip), financial services (Farm Products, tanks, insurance) and services (TSR) are not Unit accounts.
- **Installation JSF** – an engineer visit of type INSTALLATION with status COMPLETED where the engineer recorded **Installed**.
- **Contract** – the signed carbon contract for one installed pump. **One pump = one contract**: a 10-pump account needs 10.
- **Kenya only** – every page shows Kenya.

**Filters.** Click **Filter Here →** to open the filters, **Hide Slicer Pane** to close them. The number next to Filter Here is how many filters are on. **Reset Filters** puts them back to the default (Country = Kenya on Contracts Overview; everything else cleared). When the file opens in Power BI Desktop, click Reset Filters once.

---

## Contracts Overview

| Card | What it counts |
|---|---|
| **Units Sold** | Pumps sold (product quantity, at least 1 per account) on Unit accounts with a sale date, not Refunded or Rejected. Same number as *Total sales* on the Operations dashboard and the Sales Funnel. |
| **Installations** | Installed pumps: every Unit account with an installation JSF counts once, at its latest installation, and adds its number of pumps. Dated by the installation date. |
| **Contracts Submitted** | Installed pumps whose signed contract has been received (contract register, Received = Yes). |
| **Contracts Outstanding** | Installations − Contracts Submitted. These are the contracts still to collect. |
| **Submission Rate** | Contracts Submitted ÷ Installations. |

| Visual | What it shows |
|---|---|
| **Chart 1: MoM Installations x Contracts Submitted** | Installations and contracts submitted per installation month, with the submission rate; the latest 18 months with activity. |
| **Chart 2: Submission Rate x Region** | Submission rate per customer region. |
| **Table 1: Sales Cohorts → Installations → Contracts** | For each sale year: pumps sold, how many are installed, submitted and outstanding, and the rate. |
| **Table 2: Outstanding Contracts x JSF Engineer** | Installed, submitted and outstanding per engineer who did the installation, most outstanding first. Use it to chase contracts. |

Filters: Country, Contract Source, Account Type, Contract Received, JSF Year, JSF Month (installation date).

## Customer Details

One row per installed account, with its contract status: account ref, customer, account type, product, dispatch date, status, device, region, county, JSF engineer, sale date, JSF date and type. **Contract Received** is Yes / No (green / red); blank means the account is not in the contract register yet.

The cards are the same as on Contracts Overview. Filters: Account Ref, Contract Received, Account Type, Contract Source, Dispatch Month, JSF Year, JSF Month.

## Carbon Credit CDS

| Number / visual | What it counts |
|---|---|
| **Number of Customers** | Customer Data Survey (CDS) responses linked to a Kenya account. |
| **Chart 1: Customers by Pump Type** | CDS responses by the pump the customer used before (diesel, electric, petrol). |
| **Table 1: Carbon Credit Customer Summary** | One row per CDS response: customer, account ref, previous pump, town, payment method, weekly water use, water source, CDS code, CDS 2 date. Customer details come from the customer's account (blank when the response has no lead). |

Filters: Country, Pump type, Owned a pump before, CDS 2 Month, CDS Code.

## Training (safeguards)

| Card | What it counts |
|---|---|
| **Teams Total** | Teams on the carbon training register. |
| **Teams Completed** | Teams whose training is Completed. |
| **Training Completion %** | Teams Completed ÷ Teams Total. |
| **Teams Remaining** | Teams Total − Teams Completed. |
| **Divisions Covered** | Divisions with at least one team on the register. |
| **Ambassadors Onboarded** | Carbon ambassadors on the roster. |

Chart 1 shows training status by division, Chart 2 the overall status split; Table 1 is the team-level register (status coloured green / amber / red), Table 2 the ambassador roster. Filters: Division, Department / Team, Training Status, Session Type.

## LSC & Feedback (safeguards)

| Card | What it counts |
|---|---|
| **Consultations Held** | Local stakeholder consultations (LSC) held. |
| **Total Participants** | People who attended. |
| **Female %** | Women as a share of participants. |
| **Counties Reached** | Counties with at least one consultation. |
| **County Coverage %** | Counties reached ÷ 47. |
| **Counties Not Reached** | 47 − counties reached. |

Chart 1 maps consultations by county; Chart 2 shows participants per consultation round; Chart 3 the issues communities raised (count of consultations raising each theme). Filters: Consultation round, County, Year.

---

*Data refreshed* (under the sidebar) is when the data was last loaded, in East Africa Time. *Contracts synced* is when the contract register last updated; contracts received after that do not show yet.
