# Changelog

## 2026-11-19

- **Changed** every chart and table title to bold, italic and underlined, and every score-card label to bold.
- **Fixed** the refresh failing with "Column 'accountRef' in Table 'accounts_mv' contains a duplicate value '3483116'": a new account (18 Nov) reused an older account's ref. In `accounts_mv` the oldest account keeps the plain ref and later accounts sharing it get `<ref>-<account_id>`, so the contract link stays one-to-one and no account is dropped.

## 2026-11-16

- **Added** `Carbon Dashboard - KE.pbip`: Customer Contracts, Carbon credit CDS and Carbon Safeguards in one report and one model (originals unchanged in `../carbon_v1/`).
- **Changed** Installations: taken from the installation JSF itself (completed INSTALLATION, recommendation Installed, Kenya, Category Unit, not ADDON), one pump = one contract (each account once, at its latest such JSF, adding its pumps). Previously it used the account's latest-JSF summary and required a device ID on the account, which dropped completed installations (111 of the 405 reported missing on 10 Nov).
- **Changed** Units Sold to the Operations definition (Category Unit, not Refunded / Rejected): 82,261 → 76,653 all time.
- **Changed** the Calendar to date installations by the JSF date; JSF Year / Month slicers use the Calendar.
- **Fixed** the Contracts Overview sales-cohort table (it referenced measures that did not exist) and Pending Contracts (blank, not 0, without installations).
- **Fixed** the CDS customer table: customer columns are looked up once per lead on the survey table instead of joining every account (the table did not load).
- **Changed** the layout to the SunCulture house style: sidebar (`nav.json`), header, filter pane over the header with Show / Hide and Reset Filters bookmarks, active-filter count, score cards in one row, SunCulture theme with per-visual colours and fonts removed, Data refreshed / Contracts synced under the sidebar. The CDS page is 1280 wide; its two "Number of Customers by Response" slicers are now *Pump type* and *Owned a pump before*.
- **Changed** model size 189 → 141 MB: auto date/time off (Dispatch Month, Sale Year and CDS2 Month columns replace the date hierarchies); only used columns loaded; `jsf_mv` only completed installation JSFs marked Installed; `accounts_mv` Kenya only; raw Safeguards tables removed; `Has Received Contract` reads the contracts relationship.
- **Removed** customer phone numbers (CDS page) and a CDS page filter on a column that does not exist.
- **Added** descriptions to every measure.
