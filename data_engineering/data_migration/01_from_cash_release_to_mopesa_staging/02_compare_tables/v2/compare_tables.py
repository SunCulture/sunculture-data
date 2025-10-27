import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import logging
from datetime import datetime
import argparse

import warnings
warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy connectable')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# TABLE-SPECIFIC FILTER CONDITIONS
# Define WHERE clauses for tables that need filtered comparisons
# Format: {
#     "table_name": {
#         "source_filter": 'column_name = value',
#         "target_filter": 'column_name = value'
#     }
# }
# If not specified, entire table will be compared
TABLE_FILTERS = {
    "cashReleaseExpenses": {
        "source_filter": '"expenseOrigin" = 1',
        "target_filter": '"expenseOrigin" = 6'
    },
    "accountability": {
        "source_filter": '"advanceRegion" = 1',
        "target_filter": '"advanceRegion" = 6'
    }
    # Add more filtered tables as needed
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

def get_table_primary_key(conn, table_name):
    """Get the primary key column(s) for a table."""
    query = """
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = %s::regclass AND i.indisprimary;
    """
    try:
        cursor = conn.cursor()
        # Quote the table name to handle case sensitivity
        quoted_table = f'"{table_name}"'
        cursor.execute(query, (quoted_table,))
        result = cursor.fetchall()
        cursor.close()
        
        if result:
            pk_columns = [row[0] for row in result]
            print(f"   Primary key(s) for {table_name}: {', '.join(pk_columns)}")
            return pk_columns
        else:
            print(f"   ⚠️  No primary key found for {table_name}")
            return None
    except Exception as e:
        # Rollback the transaction to recover from the error
        conn.rollback()
        print(f"   ⚠️  Could not detect primary key for {table_name}")
        return None

def get_source_table_data(source_conn, source_table):
    """Fetch all data from the source table."""
    try:
        quoted_table = f'"{source_table}"'
        
        # Check if there's a filter for this table
        table_filter = TABLE_FILTERS.get(source_table, {})
        source_filter = table_filter.get('source_filter', '')
        
        if source_filter:
            query = f"SELECT * FROM {quoted_table} WHERE {source_filter}"
            print(f"📥 Fetching FILTERED data from source table: {source_table}")
            print(f"   🔍 Filter: WHERE {source_filter}")
        else:
            query = f"SELECT * FROM {quoted_table}"
            print(f"📥 Fetching ALL data from source table: {source_table}")
        
        df = pd.read_sql(query, source_conn)
        print(f"   ✅ Fetched {len(df)} rows from source")
        return df
    except Exception as e:
        logger.error(f"Error fetching source table data: {e}")
        raise

def get_target_table_data(target_conn, target_table, source_table=None):
    """Fetch all data from the target table."""
    try:
        quoted_table = f'"{target_table}"'
        
        # Check if there's a filter for this table (using source table name as key)
        lookup_table = source_table if source_table else target_table
        table_filter = TABLE_FILTERS.get(lookup_table, {})
        target_filter = table_filter.get('target_filter', '')
        
        if target_filter:
            query = f"SELECT * FROM {quoted_table} WHERE {target_filter}"
            print(f"📥 Fetching FILTERED data from target table: {target_table}")
            print(f"   🔍 Filter: WHERE {target_filter}")
        else:
            query = f"SELECT * FROM {quoted_table}"
            print(f"📥 Fetching ALL data from target table: {target_table}")
        
        df = pd.read_sql(query, target_conn)
        print(f"   ✅ Fetched {len(df)} rows from target")
        return df
    except Exception as e:
        logger.error(f"Error fetching target table data: {e}")
        raise

def compare_table_data(source_df, target_df, primary_keys=None, source_table="source", target_table="target"):
    """Compare data between source and target tables."""
    print(f"🔍 Comparing data between {source_table} and {target_table}...")
    
    # If no primary keys specified, try to find common unique identifier columns
    if primary_keys is None:
        # Common ID column names to check
        common_id_columns = ['id', 'uuid', 'user_id', 'email', 'username']
        primary_keys = []
        for col in common_id_columns:
            if col in source_df.columns and col in target_df.columns:
                primary_keys.append(col)
                break
        
        if not primary_keys:
            print("   ⚠️  No primary key specified and no common ID column found.")
            print("   ℹ️  Will compare entire rows (slower and may not be accurate)")
            # Use all columns as composite key
            primary_keys = list(source_df.columns)
    
    print(f"   Using key column(s): {', '.join(primary_keys)}")
    
    # Ensure primary key columns exist in both dataframes
    missing_in_source = [pk for pk in primary_keys if pk not in source_df.columns]
    missing_in_target = [pk for pk in primary_keys if pk not in target_df.columns]
    
    if missing_in_source or missing_in_target:
        error_msg = []
        if missing_in_source:
            error_msg.append(f"Missing in source: {', '.join(missing_in_source)}")
        if missing_in_target:
            error_msg.append(f"Missing in target: {', '.join(missing_in_target)}")
        raise ValueError(f"Primary key columns not found. {' | '.join(error_msg)}")
    
    # Ensure both dataframes have the same columns
    common_columns = list(set(source_df.columns) & set(target_df.columns))
    source_only_columns = list(set(source_df.columns) - set(target_df.columns))
    target_only_columns = list(set(target_df.columns) - set(source_df.columns))
    
    if source_only_columns:
        print(f"   ℹ️  Columns only in source: {', '.join(source_only_columns)}")
    if target_only_columns:
        print(f"   ℹ️  Columns only in target: {', '.join(target_only_columns)}")
    
    # Work with common columns only
    source_df_common = source_df[common_columns].copy()
    target_df_common = target_df[common_columns].copy()
    
    # Convert DataFrames to string to handle different data types
    source_df_common = source_df_common.astype(str)
    target_df_common = target_df_common.astype(str)
    
    # Create composite keys
    source_df_common['_composite_key'] = source_df_common[primary_keys].apply(
        lambda row: '||'.join(row.values.astype(str)), axis=1
    )
    target_df_common['_composite_key'] = target_df_common[primary_keys].apply(
        lambda row: '||'.join(row.values.astype(str)), axis=1
    )
    
    # Find records only in source
    source_keys = set(source_df_common['_composite_key'])
    target_keys = set(target_df_common['_composite_key'])
    
    only_in_source_keys = source_keys - target_keys
    only_in_target_keys = target_keys - source_keys
    common_keys = source_keys & target_keys
    
    print(f"   📊 Records only in source: {len(only_in_source_keys)}")
    print(f"   📊 Records only in target: {len(only_in_target_keys)}")
    print(f"   📊 Common records: {len(common_keys)}")
    
    # Get records only in source
    records_only_in_source = source_df[source_df_common['_composite_key'].isin(only_in_source_keys)].copy()
    
    # Get records only in target
    records_only_in_target = target_df[target_df_common['_composite_key'].isin(only_in_target_keys)].copy()
    
    # Find records with differences (same key but different data)
    differing_records = []
    if len(common_keys) > 0:
        print(f"   🔎 Checking for data differences in {len(common_keys)} common records...")
        
        for key in common_keys:
            source_row = source_df_common[source_df_common['_composite_key'] == key]
            target_row = target_df_common[target_df_common['_composite_key'] == key]
            
            # Compare all columns except the composite key
            cols_to_compare = [col for col in common_columns if col in primary_keys or col != '_composite_key']
            
            source_values = source_row[cols_to_compare].iloc[0].to_dict()
            target_values = target_row[cols_to_compare].iloc[0].to_dict()
            
            differences = {}
            for col in cols_to_compare:
                if col not in primary_keys:  # Don't compare primary key columns
                    if source_values[col] != target_values[col]:
                        differences[col] = {
                            'source_value': source_values[col],
                            'target_value': target_values[col]
                        }
            
            if differences:
                diff_record = {pk: source_values[pk] for pk in primary_keys}
                for col, vals in differences.items():
                    diff_record[f'{col}_source'] = vals['source_value']
                    diff_record[f'{col}_target'] = vals['target_value']
                differing_records.append(diff_record)
    
    differing_records_df = pd.DataFrame(differing_records) if differing_records else pd.DataFrame()
    print(f"   📊 Records with differences: {len(differing_records_df)}")
    
    return {
        'only_in_source': records_only_in_source,
        'only_in_target': records_only_in_target,
        'differing_records': differing_records_df,
        'summary': {
            'total_source': len(source_df),
            'total_target': len(target_df),
            'only_in_source_count': len(only_in_source_keys),
            'only_in_target_count': len(only_in_target_keys),
            'common_count': len(common_keys),
            'differing_count': len(differing_records_df)
        }
    }

def save_comparison_to_excel(comparison_results, source_table, target_table):
    """Save comparison results to an Excel file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"table_comparison_{source_table}_vs_{target_table}_{timestamp}.xlsx"
    
    try:
        print(f"💾 Saving comparison results to Excel...")
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Sheet 1: Summary
            summary_data = {
                'Metric': [
                    'Source Table',
                    'Target Table',
                    'Source Filter Applied',
                    'Target Filter Applied',
                    'Total Records in Source',
                    'Total Records in Target',
                    'Records Only in Source',
                    'Records Only in Target',
                    'Common Records',
                    'Records with Differences'
                ],
                'Value': [
                    source_table,
                    target_table,
                    TABLE_FILTERS.get(source_table, {}).get('source_filter', 'None'),
                    TABLE_FILTERS.get(source_table, {}).get('target_filter', 'None'),
                    comparison_results['summary']['total_source'],
                    comparison_results['summary']['total_target'],
                    comparison_results['summary']['only_in_source_count'],
                    comparison_results['summary']['only_in_target_count'],
                    comparison_results['summary']['common_count'],
                    comparison_results['summary']['differing_count']
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            print(f"   ✅ Sheet 1: Summary")
            
            # Sheet 2: Records Only in Source
            if not comparison_results['only_in_source'].empty:
                comparison_results['only_in_source'].to_excel(writer, sheet_name='Only_in_Source', index=False)
                print(f"   ✅ Sheet 2: Only in Source ({len(comparison_results['only_in_source'])} records)")
            else:
                pd.DataFrame({'Message': ['No records found only in source']}).to_excel(
                    writer, sheet_name='Only_in_Source', index=False
                )
                print(f"   ✅ Sheet 2: Only in Source (0 records)")
            
            # Sheet 3: Records Only in Target
            if not comparison_results['only_in_target'].empty:
                comparison_results['only_in_target'].to_excel(writer, sheet_name='Only_in_Target', index=False)
                print(f"   ✅ Sheet 3: Only in Target ({len(comparison_results['only_in_target'])} records)")
            else:
                pd.DataFrame({'Message': ['No records found only in target']}).to_excel(
                    writer, sheet_name='Only_in_Target', index=False
                )
                print(f"   ✅ Sheet 3: Only in Target (0 records)")
            
            # Sheet 4: Records with Differences
            if not comparison_results['differing_records'].empty:
                comparison_results['differing_records'].to_excel(writer, sheet_name='Differing_Records', index=False)
                print(f"   ✅ Sheet 4: Differing Records ({len(comparison_results['differing_records'])} records)")
            else:
                pd.DataFrame({'Message': ['No differing records found']}).to_excel(
                    writer, sheet_name='Differing_Records', index=False
                )
                print(f"   ✅ Sheet 4: Differing Records (0 records)")
        
        print(f"🎉 Comparison complete! Results saved to: {filename}")
        return filename
        
    except Exception as e:
        logger.error(f"Error saving to Excel: {e}")
        raise

def main():
    """Main function to compare specific tables between source and target databases."""
    parser = argparse.ArgumentParser(
        description='Compare row-level data between source and target database tables'
    )
    parser.add_argument(
        '--source-table',
        required=True,
        help='Name of the source table (e.g., user, users)'
    )
    parser.add_argument(
        '--target-table',
        required=True,
        help='Name of the target table (e.g., users, user)'
    )
    parser.add_argument(
        '--primary-key',
        nargs='+',
        help='Primary key column(s) to use for comparison (e.g., id or email username)'
    )
    
    args = parser.parse_args()
    
    source_table = args.source_table
    target_table = args.target_table
    primary_keys = args.primary_key
    
    try:
        print("🚀 Starting table-level comparison...")
        print(f"   Source table: {source_table}")
        print(f"   Target table: {target_table}")
        
        # Connect to databases
        source_conn = get_source_db_connection()
        target_conn = get_target_db_connection()
        
        # If primary keys not specified, try to detect them
        if not primary_keys:
            print("🔑 Detecting primary keys...")
            source_pk = get_table_primary_key(source_conn, source_table)
            target_pk = get_table_primary_key(target_conn, target_table)
            
            if source_pk and target_pk and source_pk == target_pk:
                primary_keys = source_pk
                print(f"   ✅ Using detected primary key(s): {', '.join(primary_keys)}")
            else:
                print("   ⚠️  Could not detect matching primary keys")
                primary_keys = None
        
        # Fetch table data
        source_df = get_source_table_data(source_conn, source_table)
        target_df = get_target_table_data(target_conn, target_table, source_table)
        
        # Compare data
        comparison_results = compare_table_data(
            source_df, target_df, primary_keys, source_table, target_table
        )
        
        # Save results
        filename = save_comparison_to_excel(comparison_results, source_table, target_table)
        
        # Print summary
        print("\n" + "="*60)
        print("📈 COMPARISON SUMMARY")
        print("="*60)
        print(f"📊 Source: {source_table} ({comparison_results['summary']['total_source']} records)")
        print(f"📊 Target: {target_table} ({comparison_results['summary']['total_target']} records)")
        print(f"📤 Only in Source: {comparison_results['summary']['only_in_source_count']} records")
        print(f"📥 Only in Target: {comparison_results['summary']['only_in_target_count']} records")
        print(f"🤝 Common: {comparison_results['summary']['common_count']} records")
        print(f"⚠️  Differing: {comparison_results['summary']['differing_count']} records")
        print(f"📄 Results saved to: {filename}")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error during comparison: {e}")
        logger.error(f"Error during comparison: {e}")
        raise
    finally:
        if 'source_conn' in locals() and source_conn:
            source_conn.close()
            print("✅ Closed source database connection")
        if 'target_conn' in locals() and target_conn:
            target_conn.close()
            print("✅ Closed target database connection")

if __name__ == "__main__":
    main()