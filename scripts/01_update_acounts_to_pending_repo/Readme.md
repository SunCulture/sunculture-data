# About

- Update Accounts to `Pending Repo`

# Requirements

1. Text Ediotr (Vs Code)
2. Access to MySQL DB
3. Python

# Steps

1. List of accounts to be updated are shared by Simon Mulinge (Head of PAYG)
2. For the provided accounts, check on `repossession` table in FMA DB if the accounts exists
   1. For missing `account_id` in `repossessions` table, create an insert
   2. If `status = PAID` then create a new one
   3. If `status = CANCELLED` then create a new one
   4. If `status = NEW` then skip the creation and update the `accounts` table to **Pending Repossession**
3. Run the `update_accounts_to_pending_repossession.py` script from the above by:
   ```py
     py update_account_status_to_pending_repossession.py
   ```

# Run the Script

## Dry Run

```py
  py update_account_status_to_pending_repossession.py --dry-run
```

## Run (Production)
```py
  py update_account_status_to_pending_repossession.py --execute
```