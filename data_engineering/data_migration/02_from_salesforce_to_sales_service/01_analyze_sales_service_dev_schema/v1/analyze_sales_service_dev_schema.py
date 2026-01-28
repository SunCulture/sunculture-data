import os
import pandas as pd
from dotenv import load_dotenv
import mysql.connector
import logging
from datetime import datetime

import warnings
warnings.filterwarnings('ignore', category=UserWarning, message='pandas only supports SQLAlchemy connectable')

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

DB_CONFIGS = {
    "sales_service_dev": {
        "host": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_HOST"),
        "database": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_NAME"),
        "user": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_USER"),
        "password": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_PASSWORD"),
        "port": int(os.getenv("MYSQL_DB_PORT", 3306)),  # MySQL default port is 3306
    }
}

def get_db_connection(db_key):
    """Establish a database connection using the provided configuration key."""
    try:
        config = DB_CONFIGS[db_key]
        conn = mysql.connector.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            port=config["port"]
        )
        print(f"✅ Connected to database: {config['database']}")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database {db_key}: {e}")
        raise

def fetch_database_schema(db_key):
    """Fetch comprehensive table and column information from MySQL database."""
    query = """
    SELECT 
        TABLE_SCHEMA as db_name,
        TABLE_NAME as table_name,
        COLUMN_NAME as column_name,
        ORDINAL_POSITION as ordinal_position,
        CAST(COLUMN_DEFAULT AS CHAR) as column_default,
        IS_NULLABLE as is_nullable,
        DATA_TYPE as data_type,
        COLUMN_TYPE as column_type,
        CHARACTER_MAXIMUM_LENGTH as char_max_length,
        NUMERIC_PRECISION as numeric_precision,
        NUMERIC_SCALE as numeric_scale,
        COLUMN_KEY as column_key,
        EXTRA as extra,
        COLUMN_COMMENT as column_comment,
        CHARACTER_SET_NAME as character_set,
        COLLATION_NAME as collation
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
    ORDER BY TABLE_NAME, ORDINAL_POSITION;
    """
    
    conn = None
    try:
        conn = get_db_connection(db_key)
        # Fetch data with cursor to handle binary data properly
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        
        # Convert to DataFrame, handling binary data
        rows_clean = []
        for row in rows:
            clean_row = []
            for val in row:
                if isinstance(val, bytearray):
                    clean_row.append(val.decode('utf-8', errors='ignore'))
                elif isinstance(val, bytes):
                    clean_row.append(val.decode('utf-8', errors='ignore'))
                else:
                    clean_row.append(val)
            rows_clean.append(clean_row)
        
        df = pd.DataFrame(rows_clean, columns=columns)
        
        print(f"📊 Fetching non-null counts for {len(df)} columns...")
        
        # Add non-null counts with progress indicator
        non_null_counts = []
        for i, (_, row) in enumerate(df.iterrows()):
            if i % 20 == 0:  # Progress indicator every 20 columns
                print(f"   Progress: {i}/{len(df)} columns processed...")
            count = get_non_null_count(conn, row['table_name'], row['column_name'])
            non_null_counts.append(count)
        
        df['non_null_count'] = non_null_counts
        
        print(f"✅ Schema fetched: {len(set(df['table_name']))} tables, {len(df)} columns")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching database schema: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_non_null_count(conn, table_name, column_name):
    """Get count of non-null values for a specific column in a table."""
    try:
        cursor = conn.cursor()
        # Use backticks for MySQL identifiers
        query = f"SELECT COUNT(`{column_name}`) FROM `{table_name}` WHERE `{column_name}` IS NOT NULL"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Exception as e:
        # Return None if error occurs
        return None

def get_table_row_counts(conn, db_name):
    """Get row counts for all tables in the database."""
    try:
        cursor = conn.cursor()
        
        # Get all table names
        cursor.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_TYPE = 'BASE TABLE'")
        tables_raw = cursor.fetchall()
        
        # Convert table names, handling bytearray
        tables = []
        for row in tables_raw:
            table_name = row[0]
            if isinstance(table_name, bytearray):
                table_name = table_name.decode('utf-8', errors='ignore')
            elif isinstance(table_name, bytes):
                table_name = table_name.decode('utf-8', errors='ignore')
            tables.append(table_name)
        
        print(f"📊 Fetching row counts for {len(tables)} tables...")
        
        row_counts = []
        for i, table_name in enumerate(tables):
            if i % 10 == 0:  # Progress indicator every 10 tables
                print(f"   Progress: {i}/{len(tables)} tables processed...")
            
            try:
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                count = cursor.fetchone()[0]
                row_counts.append({
                    'table_name': table_name,
                    'row_count': count
                })
            except Exception as e:
                logger.warning(f"Could not get row count for table {table_name}: {e}")
                row_counts.append({
                    'table_name': table_name,
                    'row_count': None
                })
        
        cursor.close()
        return pd.DataFrame(row_counts)
        
    except Exception as e:
        logger.error(f"Error fetching row counts: {e}")
        return pd.DataFrame()

def get_table_indexes(conn, db_name):
    """Get index information for all tables."""
    query = """
    SELECT 
        TABLE_NAME as table_name,
        INDEX_NAME as index_name,
        COLUMN_NAME as column_name,
        NON_UNIQUE as non_unique,
        SEQ_IN_INDEX as seq_in_index,
        INDEX_TYPE as index_type
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
    ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        
        # Convert to DataFrame, handling binary data
        rows_clean = []
        for row in rows:
            clean_row = []
            for val in row:
                if isinstance(val, bytearray):
                    clean_row.append(val.decode('utf-8', errors='ignore'))
                elif isinstance(val, bytes):
                    clean_row.append(val.decode('utf-8', errors='ignore'))
                else:
                    clean_row.append(val)
            rows_clean.append(clean_row)
        
        df = pd.DataFrame(rows_clean, columns=columns)
        print(f"✅ Fetched index information for {len(set(df['table_name']))} tables")
        return df
    except Exception as e:
        logger.error(f"Error fetching index information: {e}")
        return pd.DataFrame()

def get_table_constraints(conn, db_name):
    """Get foreign key constraints information."""
    query = """
    SELECT 
        TABLE_NAME as table_name,
        CONSTRAINT_NAME as constraint_name,
        COLUMN_NAME as column_name,
        REFERENCED_TABLE_NAME as referenced_table,
        REFERENCED_COLUMN_NAME as referenced_column
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = DATABASE()
    AND REFERENCED_TABLE_NAME IS NOT NULL
    ORDER BY TABLE_NAME, CONSTRAINT_NAME;
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        
        # Convert to DataFrame, handling binary data
        rows_clean = []
        for row in rows:
            clean_row = []
            for val in row:
                if isinstance(val, bytearray):
                    clean_row.append(val.decode('utf-8', errors='ignore'))
                elif isinstance(val, bytes):
                    clean_row.append(val.decode('utf-8', errors='ignore'))
                else:
                    clean_row.append(val)
            rows_clean.append(clean_row)
        
        df = pd.DataFrame(rows_clean, columns=columns)
        print(f"✅ Fetched constraint information")
        return df
    except Exception as e:
        logger.error(f"Error fetching constraint information: {e}")
        return pd.DataFrame()

def generate_summary_statistics(schema_df, row_counts_df):
    """Generate summary statistics about the database."""
    summary = {
        'Metric': [],
        'Value': []
    }
    
    if not schema_df.empty:
        # Basic counts
        total_tables = len(set(schema_df['table_name']))
        total_columns = len(schema_df)
        
        summary['Metric'].extend([
            'Database Name',
            'Total Tables',
            'Total Columns'
        ])
        summary['Value'].extend([
            schema_df['db_name'].iloc[0] if not schema_df.empty else 'N/A',
            total_tables,
            total_columns
        ])
        
        # Row count statistics
        if not row_counts_df.empty and row_counts_df['row_count'].notna().any():
            max_rows = row_counts_df['row_count'].max()
            max_table = row_counts_df.loc[row_counts_df['row_count'].idxmax(), 'table_name']
            
            summary['Metric'].extend([
                'Largest Table',
                'Largest Table Row Count'
            ])
            summary['Value'].extend([
                max_table,
                int(max_rows)
            ])
        
        # Column type distribution
        data_type_counts = schema_df['data_type'].value_counts()
        summary['Metric'].append('Most Common Data Type')
        summary['Value'].append(f"{data_type_counts.index[0]} ({data_type_counts.iloc[0]} columns)")
        
        # Key columns
        key_columns = schema_df[schema_df['column_key'].notna()]
        summary['Metric'].extend([
            'Primary Key Columns',
            'Indexed Columns (with keys)'
        ])
        summary['Value'].extend([
            len(key_columns[key_columns['column_key'] == 'PRI']),
            len(key_columns)
        ])
    
    return pd.DataFrame(summary)

def save_results_to_excel(schema_df, row_counts_df, indexes_df, constraints_df, db_name):
    """Save the database schema analysis to an Excel file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{db_name}_schema_analysis_{timestamp}.xlsx"
    
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            
            # Sheet 1: Summary Statistics
            summary_df = generate_summary_statistics(schema_df, row_counts_df)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            print(f"   ✅ Sheet 1: Summary")
            
            # Sheet 2: Complete Schema (sorted by table, then column position)
            if not schema_df.empty:
                # Sort by table name and ordinal position
                sorted_schema = schema_df.sort_values(['table_name', 'ordinal_position'])
                # Select only relevant columns
                relevant_columns = [
                    'db_name', 'table_name', 'column_name', 'ordinal_position',
                    'column_default', 'is_nullable', 'data_type', 'column_type',
                    'char_max_length', 'extra', 'column_comment', 'non_null_count'
                ]
                sorted_schema = sorted_schema[relevant_columns]
                sorted_schema.to_excel(writer, sheet_name='Complete_Schema', index=False)
                print(f"   ✅ Sheet 2: Complete Schema ({len(sorted_schema)} columns)")
            
            # Sheet 3: Table Overview
            if not schema_df.empty:
                table_overview = schema_df.groupby('table_name').agg({
                    'column_name': 'count',
                    'db_name': 'first'
                }).reset_index()
                table_overview.columns = ['table_name', 'column_count', 'db_name']
                
                # Merge with row counts if available
                if not row_counts_df.empty:
                    table_overview = table_overview.merge(row_counts_df, on='table_name', how='left')
                
                table_overview = table_overview.sort_values('table_name')
                table_overview.to_excel(writer, sheet_name='Table_Overview', index=False)
                print(f"   ✅ Sheet 3: Table Overview ({len(table_overview)} tables)")
            
            # Sheet 4: Indexes
            if not indexes_df.empty:
                indexes_df.to_excel(writer, sheet_name='Indexes', index=False)
                print(f"   ✅ Sheet 4: Indexes ({len(indexes_df)} index columns)")
            
            # Sheet 5: Foreign Keys
            if not constraints_df.empty:
                constraints_df.to_excel(writer, sheet_name='Foreign_Keys', index=False)
                print(f"   ✅ Sheet 5: Foreign Keys ({len(constraints_df)} constraints)")
            
            # Sheet 6: Nullable Columns Analysis
            if not schema_df.empty:
                nullable_analysis = schema_df.groupby('table_name').agg({
                    'is_nullable': lambda x: (x == 'YES').sum()
                }).reset_index()
                nullable_analysis.columns = ['table_name', 'nullable_columns_count']
                nullable_analysis = nullable_analysis.sort_values('nullable_columns_count', ascending=False)
                nullable_analysis.to_excel(writer, sheet_name='Nullable_Columns', index=False)
                print(f"   ✅ Sheet 6: Nullable Columns Analysis")
        
        print(f"🎉 Analysis complete! Results saved to: {filename}")
        return filename
        
    except Exception as e:
        logger.error(f"Error saving to Excel: {e}")
        raise

def main():
    try:
        print("🚀 Starting MySQL database schema analysis...")
        
        db_key = "sales_service_dev"
        db_name = DB_CONFIGS[db_key]["database"]
        
        # Fetch database schema
        print("📥 Fetching database schema...")
        schema_df = fetch_database_schema(db_key)
        
        if schema_df.empty:
            print("❌ Failed to fetch database schema. Please check your database connection.")
            return
        
        # Get connection for additional queries
        conn = get_db_connection(db_key)
        
        # Fetch row counts
        print("🔢 Fetching table row counts...")
        row_counts_df = get_table_row_counts(conn, db_name)
        
        # Fetch indexes
        print("🔍 Fetching index information...")
        indexes_df = get_table_indexes(conn, db_name)
        
        # Fetch foreign keys
        print("🔗 Fetching foreign key constraints...")
        constraints_df = get_table_constraints(conn, db_name)
        
        conn.close()
        
        # Save results to Excel
        print("💾 Saving results to Excel...")
        filename = save_results_to_excel(schema_df, row_counts_df, indexes_df, constraints_df, db_name)
        
        print("\n" + "="*60)
        print("📈 DATABASE SCHEMA ANALYSIS SUMMARY")
        print("="*60)
        print(f"📊 Database: {db_name}")
        print(f"📊 Total Tables: {len(set(schema_df['table_name']))}")
        print(f"📊 Total Columns: {len(schema_df)}")
        if not row_counts_df.empty:
            total_rows = row_counts_df['row_count'].sum()
            print(f"📊 Total Rows: {int(total_rows):,}")
        print(f"📄 Results saved to: {filename}")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error in main execution: {e}")
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()