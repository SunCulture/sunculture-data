# Compare Table Script

# Versions

## v1

- For all table comaprison, assmuning a one to one comparison between source table name and a target table name.
- Run the script from the command line by:
  ```sh
    py compare_tables.py --source-table <source_table_name> --target-table <target_table_name>
  ```

## `v2`

- This version handles Table Filters
- To run the script:
  ```sh
    python compare_tables.py --source-table cashReleaseExpenses --target-table cashReleaseExpenses
  ```
