import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import logging
from datetime import datetime

import warnings
warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy connectable')

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# TABLE NAME MAPPING: Source table name -> Target table name(s)
# Only include tables where the name has changed during migration
# Tables not in this mapping are assumed to have the same name in both databases
# 
# For one-to-one mapping: "source_table": "target_table"
# For one-to-many mapping (table split): "source_table": ["target_table1", "target_table2"]
TABLE_NAME_MAPPING = {
    "user": "users",
    "expenseApproval": "expense_approvals",
    "accountability": ["accountability", "accountabilityItems"]  # Table split: one source -> two targets
}

# TABLE-SPECIFIC FILTER CONDITIONS
# Define WHERE clauses for tables that need filtered row count comparisons
# Format: {
#     "table_name": {
#         "source_filter": 'column_name = value',
#         "target_filter": 'column_name = value'
#     }
# }
# If not specified, entire table will be counted
TABLE_FILTERS = {
    "cashReleaseExpenses": {
        "source_filter": '"expenseOrigin" = 1',
        "target_filter": '"expenseOrigin" = 6'
    },
    "accountability": {
        "source_filter": '"advanceRegion" = 1',
        "target_filter": '"advanceRegion" = 6'
    }
}

# Database configurations
DB_CONFIGS = {
    "cash_release": {
        "host": os.getenv("PG_SC_CASH_RELEASE_DB_HOST"),
        "user": os.getenv("PG_SC_CASH_RELEASE_DB_USER"),
        "password": os.getenv("PG_SC_CASH_RELEASE_DB_PASSWORD"),
        "database": os.getenv("PG_SC_CASH_RELEASE_DB_NAME"),
        "port": 5432
    },
    "mopesa_staging": {
        "host": os.getenv("PG_SC_MOPESA_STAGING_DB_HOST"),
        "user": os.getenv("PG_SC_MOPESA_STAGING_DB_USER"),
        "password": os.getenv("PG_SC_MOPESA_STAGING_DB_PASSWORD"),
        "database": os.getenv("PG_SC_MOPESA_STAGING_DB_NAME"),
        "port": 5432
    }
}

def get_source_db_connection():
    """Establish connection to the source Cash Release PostgreSQL database."""
    try:
        config = DB_CONFIGS["cash_release"]
        conn = psycopg2.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            port=config["port"]
        )
        print("✅ Connected to source database (cash_release)")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to source database: {e}")
        raise

def get_target_db_connection():
    """Establish connection to the target Mopesa Staging PostgreSQL database."""
    try:
        config = DB_CONFIGS["mopesa_staging"]
        conn = psycopg2.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            port=config["port"]
        )
        print("✅ Connected to target database (mopesa_staging)")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to target database: {e}")
        raise

def get_non_null_counts(conn, table_name, column_name):
    """Get count of non-null values for a specific column in a table."""
    try:
        cursor = conn.cursor()
        # Handle case sensitivity and special characters in PostgreSQL
        quoted_table = f'"{table_name}"'
        quoted_column = f'"{column_name}"'
        query = f"SELECT COUNT({quoted_column}) FROM {quoted_table} WHERE {quoted_column} IS NOT NULL"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Exception as e:
        # Rollback the transaction to continue with other queries
        conn.rollback()
        return None

def get_table_row_count(conn, table_name, filter_clause=None):
    """Get the total row count for a specific table, optionally with a WHERE clause filter."""
    try:
        cursor = conn.cursor()
        quoted_table = f'"{table_name}"'
        
        if filter_clause:
            query = f"SELECT COUNT(*) FROM {quoted_table} WHERE {filter_clause}"
        else:
            query = f"SELECT COUNT(*) FROM {quoted_table}"
        
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Exception as e:
        # Rollback the transaction to continue with other queries
        conn.rollback()
        return None

def fetch_source_db_information_schema():
    """Fetch table and column information from the source database's information schema."""
    query = """
    SELECT 
        table_catalog as db_name,
        table_name, 
        column_name, 
        data_type, 
        column_default, 
        is_nullable,
        ordinal_position
    FROM information_schema.columns
    WHERE table_schema = 'public'
    ORDER BY table_name, ordinal_position;
    """
    source_conn = None
    try:
        source_conn = get_source_db_connection()
        df = pd.read_sql(query, source_conn)
        
        print(f"📊 Fetching non-null counts for {len(df)} columns...")
        # Add non-null counts with progress indicator
        non_null_counts = []
        for i, (_, row) in enumerate(df.iterrows()):
            if i % 20 == 0:  # Progress indicator every 20 columns
                print(f"   Progress: {i}/{len(df)} columns processed...")
            count = get_non_null_counts(source_conn, row['table_name'], row['column_name'])
            non_null_counts.append(count)
        
        df['non_null_count'] = non_null_counts
        print(f"✅ Source schema: {len(set(df['table_name']))} tables, {len(df)} columns")
        return df
    except Exception as e:
        logger.error(f"Error fetching source information schema: {e}")
        return pd.DataFrame()
    finally:
        if source_conn:
            source_conn.close()

def fetch_target_db_information_schema():
    """Fetch table and column information from the target database's information schema."""
    query = """
    SELECT 
        table_catalog as db_name,
        table_name, 
        column_name, 
        data_type, 
        column_default, 
        is_nullable,
        ordinal_position
    FROM information_schema.columns
    WHERE table_schema = 'public'
    ORDER BY table_name, ordinal_position;
    """
    target_conn = None
    try:
        target_conn = get_target_db_connection()
        df = pd.read_sql(query, target_conn)
        
        print(f"📊 Fetching non-null counts for {len(df)} columns...")
        # Add non-null counts with progress indicator
        non_null_counts = []
        for i, (_, row) in enumerate(df.iterrows()):
            if i % 20 == 0:  # Progress indicator every 20 columns
                print(f"   Progress: {i}/{len(df)} columns processed...")
            count = get_non_null_counts(target_conn, row['table_name'], row['column_name'])
            non_null_counts.append(count)
        
        df['non_null_count'] = non_null_counts
        print(f"✅ Target schema: {len(set(df['table_name']))} tables, {len(df)} columns")
        return df
    except Exception as e:
        logger.error(f"Error fetching target information schema: {e}")
        return pd.DataFrame()
    finally:
        if target_conn:
            target_conn.close()

def get_table_row_counts_comparison(source_df, target_df):
    """Compare row counts between source and target databases for matching tables."""
    # Get unique tables from both databases
    source_tables = set(source_df['table_name'].unique())
    target_tables = set(target_df['table_name'].unique())
    
    # Apply table name mapping to find matches
    # For each source table, check if it maps to one or more target tables
    matched_pairs = []
    
    for source_table in source_tables:
        # Check if there's a mapping for this source table
        mapped_target = TABLE_NAME_MAPPING.get(source_table)
        
        if mapped_target:
            # Handle both single target and list of targets (table split)
            if isinstance(mapped_target, list):
                # Table split: one source -> multiple targets
                for target_table in mapped_target:
                    if target_table in target_tables:
                        matched_pairs.append((source_table, target_table, True))  # True = part of split
            else:
                # Single target
                if mapped_target in target_tables:
                    matched_pairs.append((source_table, mapped_target, False))  # False = not a split
        else:
            # No mapping, check if same name exists in target
            if source_table in target_tables:
                matched_pairs.append((source_table, source_table, False))
    
    if not matched_pairs:
        return pd.DataFrame()
    
    print(f"📊 Comparing row counts for {len(matched_pairs)} table pair(s)...")
    splits = [p for p in matched_pairs if p[2]]
    if splits:
        print(f"   ℹ️  Detected {len(splits)} table split(s)")
    if TABLE_FILTERS:
        filtered_tables = [source for source, _, _ in matched_pairs if source in TABLE_FILTERS]
        if filtered_tables:
            print(f"   🔍 Applying filters to {len(filtered_tables)} table(s): {', '.join(filtered_tables)}")
    if any(source != target for source, target, _ in matched_pairs if source in TABLE_NAME_MAPPING):
        print(f"   ℹ️  Using table name mappings from TABLE_NAME_MAPPING")
    
    row_count_comparison = []
    source_conn = None
    target_conn = None
    
    try:
        source_conn = get_source_db_connection()
        target_conn = get_target_db_connection()
        
        for i, (source_table, target_table, is_split) in enumerate(matched_pairs):
            if i % 10 == 0:  # Progress indicator every 10 tables
                print(f"   Progress: {i}/{len(matched_pairs)} pairs processed...")
            
            # Check if there are filters for this table
            table_filter = TABLE_FILTERS.get(source_table, {})
            source_filter = table_filter.get('source_filter')
            target_filter = table_filter.get('target_filter')
            
            # Get row counts with optional filters
            source_count = get_table_row_count(source_conn, source_table, source_filter)
            target_count = get_table_row_count(target_conn, target_table, target_filter)
            
            # Calculate differences
            if source_count is not None and target_count is not None:
                difference = target_count - source_count
                if source_count > 0:
                    percentage_diff = (difference / source_count) * 100
                else:
                    percentage_diff = 0 if target_count == 0 else 100
                
                # Determine status - be lenient for split tables
                if is_split:
                    # For split tables, we expect target to have fewer rows than source
                    status = "✅ Split table (partial data)"
                elif source_count == target_count:
                    status = "✅ Match"
                elif target_count > source_count:
                    status = "⬆️ Target has more"
                elif target_count < source_count:
                    status = "⬇️ Target has less"
                else:
                    status = "❓ Unknown"
            else:
                difference = None
                percentage_diff = None
                status = "❌ Error counting"
            
            # Check if table name was mapped or has filters
            table_name_changed = source_table != target_table
            has_filter = source_table in TABLE_FILTERS
            
            row_count_comparison.append({
                'source_table_name': source_table,
                'target_table_name': target_table,
                'is_table_split': '🔀 Yes' if is_split else 'No',
                'table_name_changed': '🔄 Yes' if table_name_changed else 'No',
                'filter_applied': '🔍 Yes' if has_filter else 'No',
                'source_filter': source_filter if source_filter else 'None',
                'target_filter': target_filter if target_filter else 'None',
                'source_row_count': source_count,
                'target_row_count': target_count,
                'difference': difference,
                'percentage_diff': round(percentage_diff, 2) if percentage_diff is not None else None,
                'status': status
            })
    
    except Exception as e:
        print(f"❌ Error during row count comparison: {e}")
        return pd.DataFrame()
    
    finally:
        if source_conn:
            source_conn.close()
        if target_conn:
            target_conn.close()
    
    comparison_df = pd.DataFrame(row_count_comparison)
    print(f"✅ Row count comparison complete for {len(comparison_df)} table pair(s)")
    return comparison_df

def compare_information_schemas(source_df, target_df):
    """Compare the information schemas of source and target databases."""
    comparison_results = {}
    
    # Get unique tables from both databases
    source_tables = set(source_df['table_name'].unique())
    target_tables = set(target_df['table_name'].unique())
    
    # Apply table name mapping to find matches
    matched_source_tables = set()
    matched_target_tables = set()
    table_pairs = []  # List of (source, target, is_split) tuples
    
    for source_table in source_tables:
        # Check if there's a mapping for this source table
        mapped_target = TABLE_NAME_MAPPING.get(source_table)
        
        if mapped_target:
            # Handle both single target and list of targets (table split)
            if isinstance(mapped_target, list):
                # Table split: one source -> multiple targets
                for target_table in mapped_target:
                    if target_table in target_tables:
                        matched_source_tables.add(source_table)
                        matched_target_tables.add(target_table)
                        table_pairs.append((source_table, target_table, True))  # True = split
            else:
                # Single target
                if mapped_target in target_tables:
                    matched_source_tables.add(source_table)
                    matched_target_tables.add(mapped_target)
                    table_pairs.append((source_table, mapped_target, False))  # False = not split
        else:
            # No mapping, check if same name exists in target
            if source_table in target_tables:
                matched_source_tables.add(source_table)
                matched_target_tables.add(source_table)
                table_pairs.append((source_table, source_table, False))
    
    # Tables only in source (not migrated or not mapped)
    source_only_tables = source_tables - matched_source_tables
    
    # Tables only in target (new tables or not mapped)
    target_only_tables = target_tables - matched_target_tables
    
    splits = [p for p in table_pairs if p[2]]
    print(f"📊 Analysis: {len(table_pairs)} matched pair(s), {len(source_only_tables)} source-only, {len(target_only_tables)} target-only tables")
    if splits:
        print(f"   ℹ️  Detected {len(splits)} table split(s)")
    if TABLE_NAME_MAPPING:
        print(f"   ℹ️  Applied table name mappings from TABLE_NAME_MAPPING")
    
    # Detailed comparison for matched tables
    matching_tables_comparison = []
    
    for source_table, target_table, is_split in table_pairs:
        source_table_df = source_df[source_df['table_name'] == source_table].copy()
        target_table_df = target_df[target_df['table_name'] == target_table].copy()
        
        # Get columns for each table
        source_columns = set(source_table_df['column_name'])
        target_columns = set(target_table_df['column_name'])
        
        # Common columns
        common_columns = source_columns.intersection(target_columns)
        
        # Missing columns
        missing_in_target = source_columns - target_columns
        missing_in_source = target_columns - source_columns
        
        # Compare data types for common columns
        data_type_mismatches = []
        for col in common_columns:
            source_col = source_table_df[source_table_df['column_name'] == col].iloc[0]
            target_col = target_table_df[target_table_df['column_name'] == col].iloc[0]
            
            if source_col['data_type'] != target_col['data_type']:
                data_type_mismatches.append({
                    'column_name': col,
                    'source_data_type': source_col['data_type'],
                    'target_data_type': target_col['data_type']
                })
        
        # Check if table name was changed or split
        table_name_changed = source_table != target_table
        
        matching_tables_comparison.append({
            'source_table_name': source_table,
            'target_table_name': target_table,
            'is_table_split': is_split,
            'table_name_changed': table_name_changed,
            'total_columns_source': len(source_columns),
            'total_columns_target': len(target_columns),
            'common_columns': len(common_columns),
            'missing_in_target': list(missing_in_target),
            'missing_in_source': list(missing_in_source),
            'data_type_mismatches': data_type_mismatches
        })
    
    comparison_results = {
        'matched_pairs': matching_tables_comparison,
        'source_only_tables': list(source_only_tables),
        'target_only_tables': list(target_only_tables),
        'table_name_mapping': TABLE_NAME_MAPPING
    }
    
    return comparison_results

def save_results_to_excel(source_df, target_df, comparison_results, row_count_comparison_df=None):
    """Save the information schemas and comparison results to an Excel file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"db_migration_analysis_{timestamp}.xlsx"
    
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Sheet 1: Source DB Schema
            if not source_df.empty:
                source_df.to_excel(writer, sheet_name='Source_DB_Schema', index=False)
                print(f"   ✅ Sheet 1: Source DB Schema ({len(source_df)} rows)")
            
            # Sheet 2: Target DB Schema
            if not target_df.empty:
                target_df.to_excel(writer, sheet_name='Target_DB_Schema', index=False)
                print(f"   ✅ Sheet 2: Target DB Schema ({len(target_df)} rows)")
            
            # Sheet 3: Matching Tables Analysis
            if comparison_results['matched_pairs']:
                matching_df = pd.DataFrame(comparison_results['matched_pairs'])
                # Expand complex columns for better readability
                expanded_rows = []
                for _, row in matching_df.iterrows():
                    base_info = {
                        'source_table_name': row['source_table_name'],
                        'target_table_name': row['target_table_name'],
                        'is_table_split': '🔀 Yes' if row['is_table_split'] else 'No',
                        'table_renamed': '🔄 Yes' if row['table_name_changed'] else 'No',
                        'total_columns_source': row['total_columns_source'],
                        'total_columns_target': row['total_columns_target'],
                        'common_columns': row['common_columns']
                    }
                    
                    # Add missing columns info
                    if row['missing_in_target']:
                        for col in row['missing_in_target']:
                            expanded_rows.append({**base_info, 'issue_type': 'Missing in Target', 'column_name': col})
                    
                    if row['missing_in_source']:
                        for col in row['missing_in_source']:
                            expanded_rows.append({**base_info, 'issue_type': 'Missing in Source', 'column_name': col})
                    
                    # Add data type mismatches
                    if row['data_type_mismatches']:
                        for mismatch in row['data_type_mismatches']:
                            expanded_rows.append({
                                **base_info, 
                                'issue_type': 'Data Type Mismatch', 
                                'column_name': mismatch['column_name'],
                                'source_data_type': mismatch['source_data_type'],
                                'target_data_type': mismatch['target_data_type']
                            })
                    
                    # If no issues, add a summary row
                    if not row['missing_in_target'] and not row['missing_in_source'] and not row['data_type_mismatches']:
                        expanded_rows.append({**base_info, 'issue_type': 'Perfect Match'})
                
                if expanded_rows:
                    expanded_df = pd.DataFrame(expanded_rows)
                    expanded_df.to_excel(writer, sheet_name='Matching_Tables_Analysis', index=False)
                    print(f"   ✅ Sheet 3: Matching Tables Analysis ({len(expanded_df)} rows)")
            
            # Sheet 4: Tables Only in Source
            if comparison_results['source_only_tables']:
                source_only_df = pd.DataFrame({
                    'table_name': comparison_results['source_only_tables'],
                    'status': 'Exists only in Source DB'
                })
                source_only_df.to_excel(writer, sheet_name='Source_Only_Tables', index=False)
                print(f"   ✅ Sheet 4: Source Only Tables ({len(source_only_df)} rows)")
            
            # Sheet 5: Tables Only in Target
            if comparison_results['target_only_tables']:
                target_only_df = pd.DataFrame({
                    'table_name': comparison_results['target_only_tables'],
                    'status': 'Exists only in Target DB'
                })
                target_only_df.to_excel(writer, sheet_name='Target_Only_Tables', index=False)
                print(f"   ✅ Sheet 5: Target Only Tables ({len(target_only_df)} rows)")
            
            # Sheet 6: Row Count Comparison
            if row_count_comparison_df is not None and not row_count_comparison_df.empty:
                row_count_comparison_df.to_excel(writer, sheet_name='Row_Count_Comparison', index=False)
                print(f"   ✅ Sheet 6: Row Count Comparison ({len(row_count_comparison_df)} rows)")
            
            # Summary Sheet
            summary_data = {
                'Metric': [
                    'Total Tables in Source',
                    'Total Tables in Target', 
                    'Common Tables',
                    'Tables Only in Source',
                    'Tables Only in Target',
                    'Total Columns in Source',
                    'Total Columns in Target',
                    'Tables with Row Count Matches',
                    'Tables with Row Count Differences'
                ],
                'Count': [
                    len(comparison_results['source_only_tables']) + len(comparison_results['matched_pairs']),
                    len(comparison_results['target_only_tables']) + len(comparison_results['matched_pairs']),
                    len(comparison_results['matched_pairs']),
                    len(comparison_results['source_only_tables']),
                    len(comparison_results['target_only_tables']),
                    len(source_df),
                    len(target_df),
                    len(row_count_comparison_df[row_count_comparison_df['status'] == '✅ Match']) if row_count_comparison_df is not None and not row_count_comparison_df.empty else 0,
                    len(row_count_comparison_df[row_count_comparison_df['status'] != '✅ Match']) if row_count_comparison_df is not None and not row_count_comparison_df.empty else 0
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            print(f"   ✅ Sheet 7: Summary")
            
        print(f"🎉 Analysis complete! Results saved to: {filename}")
        return filename
        
    except Exception as e:
        logger.error(f"Error saving to Excel: {e}")
        raise

def main():
    try:
        print("🚀 Starting database migration analysis...")
        
        # Fetch information schemas
        print("📥 Fetching source database schema...")
        source_df = fetch_source_db_information_schema()
        
        print("📥 Fetching target database schema...")
        target_df = fetch_target_db_information_schema()
        
        if source_df.empty or target_df.empty:
            print("❌ Failed to fetch database schemas. Please check your database connections.")
            return
        
        # Compare schemas
        print("🔍 Comparing database schemas...")
        comparison_results = compare_information_schemas(source_df, target_df)
        
        # Compare row counts for matching tables
        print("🔢 Comparing row counts for matching tables...")
        row_count_comparison_df = get_table_row_counts_comparison(source_df, target_df)
        
        # Save results to Excel
        print("💾 Saving results to Excel...")
        filename = save_results_to_excel(source_df, target_df, comparison_results, row_count_comparison_df)
        
        print("\n" + "="*60)
        print("📈 MIGRATION ANALYSIS SUMMARY")
        print("="*60)
        print(f"📊 Source DB Tables: {len(set(source_df['table_name']))}")
        print(f"📊 Target DB Tables: {len(set(target_df['table_name']))}")
        print(f"🤝 Matched Table Pairs: {len(comparison_results['matched_pairs'])}")
        print(f"📤 Source-only Tables: {len(comparison_results['source_only_tables'])}")
        print(f"📥 Target-only Tables: {len(comparison_results['target_only_tables'])}")
        if TABLE_NAME_MAPPING:
            print(f"🔄 Table Name Mappings Applied: {len(TABLE_NAME_MAPPING)}")
        print(f"📄 Results saved to: {filename}")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error in main execution: {e}")
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()