### 💰 KPI: Sales

**Business Question**  
When do we count a customer as a confirmed sale?

**Definition**  
A customer is recorded as a sale when the full deposit is paid.  
- **Cash Clients**: Based on full payment date.  
- **PAYG Clients**: Based on full deposit payment date.

**Calculation**  
`COUNT(Clients WHERE full_deposit_paid_date IS NOT NULL)`

**Reporting Period**  
YTD

**Data Source**  
`sales_orders`, `payment_transactions`

**Owner**  
Sales Operations

**Notes**  
- Excludes test/demo clients  
- Resales handled under "Reactivation Sales"
