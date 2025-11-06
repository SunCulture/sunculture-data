# Migrate Data From `data-migration-staging` db to staging or prod

# Requirements

# Steps

1. ETL the following objects from Salesforce to `data-migration-staging` db
   1. `Leads`
   2. `Agents__c`
   3. `Customer_Data_Survey_c`
   4. `User`

# Leads

## Test with small batch

```sh
    python migrate_leads.py --dry-run --limit 100 --batch-size 50
```

## Run live migration

```sh
    python migrate_leads.py --limit 1000 --batch-size 500
```

## Full migration when confident

```sh
    python migrate_leads.py --batch-size 5000
```

# How to Resume Migration

```sh
    # The script will resume from leadId ~286000
    python migrate_leads.py --resume --batch-size 5000
```

# `kyc_requests`

## Dry run with 100 records

python %(prog)s --dry-run --limit 100

## Resume interrupted migration

python %(prog)s --resume --batch-size 5000

## Large migration with progress tracking

python %(prog)s --batch-size 10000 --limit 31000

# `next_of_kin_details`

## Dry run

py migrate_next_of_kin_details_to_prod.py --dry-run --limit 100

## Run in prod

py migrate_next_of_kin_details_to_dev.py --batch-size 5000
