---
id: fpd
title: FPD (First Payment Default)
sidebar_label: FPD
---

## Definition

**FPD (First Payment Default)** refers to loans where the client fails to make their first scheduled payment within a specified grace period (e.g., 30 days after the due date).

## Business Logic

This metric is critical for assessing early risk behavior among new PAYG accounts. A high FPD rate signals onboarding issues or customer mismatch and can be an early warning of poor credit quality. The focus is on accounts that have been activated but defaulted immediately.

## Formula

**FPD Rate (%) = (Number of Accounts with Missed First Payment ÷ Total Number of Activated Accounts) × 100**

- **Number of Accounts with Missed First Payment**: PAYG accounts that missed their first scheduled payment within the grace period.  
- **Total Number of Activated Accounts**: Accounts that were successfully activated during the period.
