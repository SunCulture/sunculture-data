# Analyze Schema Script

# Versions

## `v6`

- The version:

  - Hanldes table mapping dictionary such that it handles cases where source and target table names might differ. i.e.,
    - Only add tables where the name changed
    - All other tables assumed to be 1:1
  - Handles table split i.e., instances where one source table is being kigrated into two target tables
  - This is `v5` with additional table filters

- How to run the script:
  ```sh
    py analyze_schema.py
  ```
