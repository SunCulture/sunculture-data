---
id: collection-rate
title: Collection Rate
sidebar_label: Collection Rate
---

# 💸 KPI: Collection Rate

| Attribute            | Description |
|----------------------|-------------|
| **Business Question** | How well are we collecting scheduled payments from active PAYG clients? |
| **Definition**        | Percentage of expected payments that were actually collected from active PAYG customers in a given period (typically monthly). Focuses only on **active** accounts—excludes paid-off or repossessed ones. |
| **Formula**           | `(Total Amount Collected ÷ Total Amount Due) × 100` |
| **Data Source(s)**    | `payment_schedule`, `payment_transactions` |
| **Owner**             | Credit Team |
| **Reporting Period**  | Monthly |
| **Tags**              | `Credit`, `Portfolio Quality`, `Repayment` |
| **Notes**             | Includes only active PAYG clients scheduled to pay during the reporting window. |

### 💡 Example

If PAYG clients were expected to pay **KES 1,000,000** in June, and the actual collections were **KES 850,000**, the Collection Rate would be:

**`(850,000 ÷ 1,000,000) × 100 = 85%`**

