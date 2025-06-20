---
id: suncultureDashboardGlobal
title: SunCulture Dashboard - Global
sidebar_label: Global
---

# 🌍 SunCulture Dashboard – Global

Welcome to the **Global Dashboard**, which aggregates key KPIs across SunCulture’s operations in **Uganda**, **Kenya**, and **Côte d’Ivoire (CIV)** to provide a unified performance view.

---

## 🎯 Objective

The **Global Dashboard** aims to:

- Consolidate key KPIs from **Kenya**, **Uganda**, and **CIV** at a global level.
- Enable cross-country comparisons for decision-making and strategic alignment.

---

## 📈 Key Performance Indicators (KPIs)

Below are the KPIs tracked globally, including their business relevance, calculation logic, and caveats.

---

### 1. 💼 Sales (YTD)

- **Purpose**: Displays total **unit sales year-to-date**, compares performance to **budgets/targets**, and breaks down sales by country and department (Field Sales, SSC, Telesales, BD).
- **Calculation**:
  - **Kenya & CIV**:
    - _Cash clients_: Use `Full Payment Date`
    - _PAYG clients_: Use `Full Deposit Date`
  - **Uganda**:
    - _Cash clients_: Use `CDS1 Completion Date`
    - _PAYG clients_: Use `CDS2 Completion Date` **plus** a **positive credit check**
- **Exclusions**:
  - Add-ons like **TVs**, **Direct Drip (DD)** units, and **promotional items**
- **Benchmark/Targets**:
  - Based on targets shared by the **FP&A team**

---

### 2. 🔁 Leads Conversion Rate

- **Purpose**: Measures the percentage of leads generated **year-to-date (YTD)** that have been successfully converted into sales, based on the sales definitions provided above.
- **Calculation**:  
  `(Number of Leads Converted to Sales YTD) / (Total Leads Generated YTD) × 100%`
- **Notes**:
  - “Converted” is based on the **country-specific sales logic** described in the Sales (YTD) section.
  - Helps assess the **effectiveness of the lead-to-sale pipeline** across countries.

---

### 3. 🕒 PAR30 (%)

- **Purpose**: Measures the percentage of active clients whose outstanding balance is **30 days or more past due**. Indicates portfolio risk and repayment delays.
- **Calculation**:  
  `(Outstanding Balance of Accounts >30 Days Past Due) / (Total Portfolio Balance of Active Clients) × 100%`
- **Notes**:
  - Only includes **Active Accounts** (excluding **Arrears**, **written-off** accounts).

---

### 4. 🚫 FPD Rate (%)

- **Purpose**: Tracks the portion of the portfolio that is under **First Payment Default (FPD)** — accounts that failed to make the first scheduled payment after activation.
- **Calculation**:  
  `(Balance of Clients Under FPD) / (Total Portfolio Balance) × 100%`

---

### 5. 💵 Collection Rate (%)

- **Purpose**: Measures the effectiveness of collections by comparing how much was **collected** vs. how much was **expected**.
- **Calculation**:  
  `(Total Amount Collected) / (Total Amount Expected) × 100%`

---

### 6. 🔄 Pre-Installation Churn Rate (%)

- **Purpose**: Measures the percentage of accounts that were **fully refunded before installation**, indicating early customer churn or cancellation.
- **Calculation**:  
  `(Number of Accounts Fully Refunded Before Installation YTD) / (Total Sales YTD) × 100%`
- **Notes**:
  - Excludes refunds that occurred **after installation**.
  - Helps monitor pre-delivery customer dropout and operational inefficiencies.

---

### 7. 🛠️ Average Installation Wait Days

- **Purpose**: Measures the average number of days taken from the **date of sale** to **successful installation**, reflecting operational efficiency and customer onboarding experience.
- **Calculation**:  
  `Average(Date of Installation − Date of Sale)`
- **Notes**:
  - For PAYG clients, use **Full Deposit Date** as the Sale Date.
  - For Cash clients, use **Full Payment Date**.
  - Installation Date is based on the JSF Date.

---

_Last updated: 2025-05-21_  
_Maintainer: Data Team_

> For custom reports or access issues, contact the Data Team via the [Data Request Form](https://sunculture.jotform.com/250342132233037). 🧠
