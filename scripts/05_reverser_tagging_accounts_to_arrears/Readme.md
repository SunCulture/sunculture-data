# Reverse Tagging Accounts to Arrears

# Step 1: Update `repossessions` Table as follows:

```sql
    update public.repossessions
    set status = 'PAID'
    where is_active = TRUE
    and account_id in ()
```

## Step 2: Update `repossessions` Table as follows:

```sql
    update public.repossessions
    set is_active = FALSE
    where account_id in ()
```
