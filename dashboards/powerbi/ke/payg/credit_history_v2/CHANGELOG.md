# Changelog

## 2026-10-29 (later)

- **Changed** the 7 retention/attrition/reactivation pages to the SunCulture look (`_templates/BRAND.md`): header with logo, title and data-refreshed card; 1280-wide canvas; slicers and 30/60/90 tabs in one filter row (current tab highlighted); a one-line definition of the rate; rate and count matrices stacked full width, with a light green heatmap on the rate. Formatting is set on these pages only; the report theme and the other pages are unchanged.

## 2026-10-29

- **Added** `Credit History - Ke.pbip`, converted from `Credit History -New (Ke).pbix` (report and model unchanged).
- **Added** 7 pages from PAYG Overview: Retention Rates (30/60/90), Attrition Rates (30/60/90), Reactivation Rates. They now run on `schedule` (snapshot, Kenya) instead of PAYG Overview's `Schedules`; the left-hand menu was removed and the 30/60/90 tab buttons open the pages directly.
- **Added** to `schedule`: columns `months_from_installment_to_current` and `is_within_plan`; 15 measures in the *Transitions* folder, with PAYG Overview's definitions.
