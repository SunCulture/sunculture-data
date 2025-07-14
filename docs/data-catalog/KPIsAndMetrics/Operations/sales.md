---
id: sales
title: Sales
sidebar_label: 1. Sales
---

# 💼 Sales Metrics

> 📌 <span id="dri">**Directly Responsible Individual (DRI)**: `Jon Saunders` (`COO`)</span>  
> These metric definitions have been set and approved by the DRI above.  
> For suggested changes, please email the DRI for review and approval.

---

### Customer

- **Description**: An individual or organization that has completed a purchase of at least one SunCulture product. Customers are typically created after a successful sale—either through cash or PAYG—and may span across different regions
- **Metric Type**: Count
- **Applicable Time Horizons**: Daily, Weekly, Monthly, Quarterly, Yearly
- **Feeds Into KPIs**
  1. `Net Promoter Score (NPS)`: Measures customer satisfaction and likelihood to refer

---

### Department

- **Description**: A functional unit within **SunCulture**, organized around a core business process such as Sales, Credit, Operations, or Product. Each department is responsible for specific KPIs, workflows, and outcomes.
- **Classification**:
  1. Sales
  2. Credit
  3. Operations
  4. Supply Chain
  5. After-Sales
  6. Product
  7. Marketing
  8. Finance
  9. People & Culture
- **Metric Type**: Count
- **Applicable Time Horizons**: Daily, Weekly, Monthly, Quarterly, Yearly
- **Feeds Into KPIs**

---

### Agent

- **Description**: An individual onboarded to promote and sell **SunCulture** products. Agents may operate across various channels (Field Sales, Telesales, SSC).
- **Metric Type**: Count
- **Applicable Time Horizons**: Daily, Weekly, Monthly, Quarterly, Yearly
- **Feeds Into KPIs**
  1.  `Agents Recruited`: Count of newly activated agents
  2.  `Selling Agent`: Agent with at least one `Sale`
  3.  `Working Agent`: Agent with at least one `Lead`
  4.  `Selling Agents Productivity`: (Total Sales / Selling Agent) x 100%
  5.  `Working Agent Productivity`: (Total Leads / Working Agents) x 100%

---

### Sales Supervisor

- **Description**: Oversees a group of **Sales Agents** within a specific **region**. They are responsible for driving performance, managing recruitment.
- **Metric Type**: Count
- **Applicable Time Horizons**: Daily, Weekly, Monthly, Quarterly, Yearly
- **Feeds Into KPIs**
  1. `Active Supervisors`: Count of supervisors with at least one active selling agent

---

### Regional Sales Manager

- **Description**:
- **Metric Type**: Count
- **Applicable Time Horizons**: Daily, Weekly, Monthly, Quarterly, Yearly
- **Feeds Into KPIs**

---

### Region

- **Description**: A Regional Sales Manager (**RSM**) is responsible for overseeing Sales Supervisors within a specific geographic region. They ensure regional sales targets are met, coach supervisors.
- **Classification**:
  1. Customer Region
  2. Sales Region
- **Metric Type**: Count
- **Applicable Time Horizons**: Daily, Weekly, Monthly, Quarterly, Yearly
- **Feeds Into KPIs**
  1. `Regional Sales Performance`: Aggregated sales across all supervisors and agents under the RSM

---

### Lead

- **Description**: A lead represents a potential customer who has expressed interest in SunCulture products or services.
- **Sources**:
- **Metric Type**: Count
- **Applicable Time Horizons**: Daily, Weekly, Monthly, Quarterly, Yearly
- **Feeds Into KPIs**
  1. `Lead Conversion Rate`: % of leads converted into `Sales`

---

### Sales

- **Description**: Total number of units sold across all channels and payment methods.
- **Classification**:
  1. Pump
  2. Add-On
- **Metric Type**: Count, Sum
- **Applicable Time Horizons**: Daily, Weekly, Monthly, Quarterly, Yearly
- **Feeds Into KPIs**
  1. `Total Unit Sales`:
  2. `PAYG Sales Volume (%)`:
  3. `Refer & Earn Performance`: (Refer & Earn Unit Sales / Total Unit Sales) × 100

---

### Account

- **Description**: An Account represents a unique product sale record in the **Account Management Tool** (**AMT**).
- **Classification**:
  1. Cash: Fully paid upfront
  2. PAYG (Pay-As-You-Go) – Purchased on credit with installment payments
  3. Add-On – Additional units or accessories purchased after the initial sale
- **Metric Type**: Count, Sum
- **Applicable Time Horizons**: Daily, Weekly, Monthly, Quarterly, Yearly
- **Feeds Into KPIs**
  1. `PAR30 Rate` : % of active PAYG accounts 30+ days past due
  2. `FPD Rate`: % of accounts that fail to make their first payment after activation

---

### Refunds

- **Description**: A Refund represents a return of funds to a customer for a canceled or incomplete account.
- **Classification**:
  1. Full Refund – Entire amount refunded (typically for pre-installation cancellations)
  2. Partial Refund – Portion of amount refunded
- **Metric Type**: Count, Sum
- **Applicable Time Horizons**: Daily, Weekly, Monthly, Quarterly, Yearly
- **Feeds Into KPIs**
  1. `Pre-Installation Churn Rate` – % of accounts fully refunded before installation
  2. `Refund Volume` – Total number of refund cases\
  3. `Net Revenue` – Revenue adjusted for refunds

---

> 🔄 Got a suggestion or correction? Reach out to the [**DRI listed above**](#dri) to propose a change. All updates must be reviewed and approved by the **DRI**.
