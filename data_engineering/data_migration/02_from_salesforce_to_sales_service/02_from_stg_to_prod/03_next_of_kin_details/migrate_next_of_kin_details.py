#!/usr/bin/env python3
"""
Next of Kin Details Migration Script - PERFORMANCE OPTIMIZED
Adapted from KYC Migration: Cursor pagination on leadId, batch inserts, pre-loaded dups, checkpointing.
Optimized for ~51k records: Fast batches, minimal roundtrips.
With FK pre-validation: Checks leadId exists in 'leads' table, logs missing ones to CSV.
"""
import argparse
import logging
import sys
import csv
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
import pymysql
from pymysql.cursors import DictCursor
import os
from dotenv import load_dotenv
import json
from pathlib import Path
# Load environment variables
load_dotenv()
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'nok_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
# Database Configuration
MYSQL_CONFIGS = {
    "staging_db": {
        "host": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_HOST"),
        "port": int(os.getenv("MYSQL_DB_PORT", 3306)),
        "user": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_USER"),
        "password": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_PASSWORD"),
        "database": "data-migration-staging",
        "table_name": "migrate_next_of_kin_details_v4"
    },
    "destination_db": {
        "host": os.getenv("SC_SALES_SERVICE_MYSQL_DB_HOST"),
        "port": int(os.getenv("MYSQL_DB_PORT", 3306)),
        "user": os.getenv("SC_SALES_SERVICE_MYSQL_DB_USER"),
        "password": os.getenv("SC_SALES_SERVICE_MYSQL_DB_PASSWORD"),
        "database": "sales-service",
        "table_name": "next_of_kin_details"
    },
    "leads_db": {
        "host": os.getenv("SC_SALES_SERVICE_MYSQL_DB_HOST"),
        "port": int(os.getenv("MYSQL_DB_PORT", 3306)),
        "user": os.getenv("SC_SALES_SERVICE_MYSQL_DB_USER"),
        "password": os.getenv("SC_SALES_SERVICE_MYSQL_DB_PASSWORD"),
        "database": "sales-service",
        "table_name": "leads"
    }
}
class NextOfKinMigration:
    def __init__(self, staging_config: Dict, destination_config: Dict, leads_config: Dict,
                 dry_run: bool = False, batch_size: int = 5000, limit: Optional[int] = None,
                 disable_fk_checks: bool = False, checkpoint_file: str = "nok_migration_checkpoint.json",
                 resume: bool = False):
        """
        Initialize migration manager with performance optimizations
        """
        self.staging_config = staging_config
        self.destination_config = destination_config
        self.leads_config = leads_config
        self.staging_table = staging_config.get('table_name', 'migrate_next_of_kin_details_v3')
        self.destination_table = destination_config.get('table_name', 'next_of_kin_details')
        self.leads_table = leads_config.get('table_name', 'leads')
        self.dry_run = dry_run
        self.batch_size = batch_size
        self.limit = limit
        self.disable_fk_checks = disable_fk_checks
        self.checkpoint_file = checkpoint_file
        self.resume = resume
       
        # Last processed leadId for cursor-based pagination
        self.last_lead_id = None
       
        self.stats = {
            'total_fetched': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'validation_failed': 0,
            'duplicates': 0,
            'fk_failed': 0
        }
       
        # Load checkpoint if resuming
        if self.resume:
            self._load_checkpoint()
       
        # CSV for failed records
        self.failed_csv_path = f'failed_nok_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        self.failed_file = None
        self.failed_writer = None
        self._init_failed_csv()
       
        # Pre-load existing (leadId, phoneNumber) pairs, and ALL leadIds for FK
        self.existing_source_system_ids = set()
        self.existing_lead_phone_pairs = set()
        self.existing_lead_ids = set()
        #if not dry_run:
        ## Always preload - we need this data even for dry-run validation
        self._preload_existing_data()
       
        # Validate configuration
        self._validate_config()
        self._check_tables()
   
    def _load_checkpoint(self):
        """Load checkpoint from previous run"""
        if Path(self.checkpoint_file).exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    self.last_lead_id = checkpoint.get('last_lead_id')
                    self.stats = checkpoint.get('stats', self.stats)
                    logger.info(f"[RESUME] Loaded checkpoint - Last leadId: {self.last_lead_id}")
                    logger.info(f"[RESUME] Previous stats: {self.stats}")
            except Exception as e:
                logger.error(f"Failed to load checkpoint: {e}")
                logger.info("Starting fresh migration")
   
    def _save_checkpoint(self):
        """Save current progress to checkpoint file"""
        try:
            checkpoint = {
                'last_lead_id': self.last_lead_id,
                'stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f, indent=2)
            logger.debug(f"Checkpoint saved: leadId={self.last_lead_id}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
   
    def _preload_existing_data(self):
        """Pre-load existing leadId+phoneNumber pairs and ALL leadIds from leads table"""
        logger.info("[INIT] Pre-loading existing data from destination...")
        try:
            # Load from next_of_kin_details
            dest_conn = self.connect_destination()
            with dest_conn.cursor() as cursor:
                # leadId + phoneNumber pairs (the real duplicate check)
                cursor.execute(f"SELECT leadId, phoneNumber FROM {self.destination_table} WHERE phoneNumber IS NOT NULL")
                pair_count = 0
                for row in cursor:
                    self.existing_lead_phone_pairs.add((row['leadId'], row['phoneNumber']))
                    pair_count += 1
                    if pair_count % 10000 == 0:
                        logger.info(f" Loaded {pair_count:,} leadId+phoneNumber pairs...")
                logger.info(f"[OK] Loaded {len(self.existing_lead_phone_pairs):,} existing leadId+phoneNumber pairs")
            dest_conn.close()
            
            # Load ALL leadIds from leads table (for FK checks)
            leads_conn = self.connect_leads()
            with leads_conn.cursor() as cursor:
                cursor.execute(f"SELECT leadId FROM {self.leads_table}")
                lead_count = 0
                for row in cursor:
                    self.existing_lead_ids.add(row['leadId'])
                    lead_count += 1
                    if lead_count % 50000 == 0:
                        logger.info(f" Loaded {lead_count:,} existing leadIds...")
                logger.info(f"[OK] Loaded {len(self.existing_lead_ids):,} existing leadIds for FK validation")
            leads_conn.close()
        except Exception as e:
            logger.error(f"Failed to preload existing data: {e}")
            raise
   
    def _init_failed_csv(self):
        """Initialize CSV writer for failed records"""
        try:
            self.failed_file = open(self.failed_csv_path, 'w', newline='', encoding='utf-8')
            self.failed_writer = csv.DictWriter(self.failed_file, fieldnames=None)
            logger.info(f"Failed records CSV initialized: {self.failed_csv_path}")
        except Exception as e:
            logger.error(f"Failed to initialize CSV: {str(e)}")
   
    def _log_failed_record(self, record: Dict, reason: str):
        """Log a failed record to CSV"""
        if self.failed_writer and self.failed_file:
            if self.failed_writer.fieldnames is None:
                all_fields = list(record.keys()) + ['failure_reason']
                self.failed_writer.fieldnames = all_fields
                self.failed_writer.writeheader()
           
            record_copy = record.copy()
            record_copy['failure_reason'] = reason
            self.failed_writer.writerow(record_copy)
            self.failed_file.flush()
   
    def _validate_config(self):
        """Validate that all required config values are present"""
        required_staging = ['host', 'user', 'password', 'database']
        required_dest = ['host', 'user', 'password', 'database']
       
        for key in required_staging:
            if not self.staging_config.get(key):
                raise ValueError(f"Missing staging config: {key}")
       
        for key in required_dest:
            if not self.destination_config.get(key):
                raise ValueError(f"Missing destination config: {key}")
       
        logger.info(f"Staging DB: {self.staging_config['database']}")
        logger.info(f"Staging Table: {self.staging_table}")
        logger.info(f"Destination DB: {self.destination_config['database']}")
        logger.info(f"Destination Table: {self.destination_table}")
        logger.info(f"Leads Table: {self.leads_table}")
   
    def _check_tables(self):
        """Verify tables exist and check indexes"""
        try:
            dest_conn = self.connect_destination()
            with dest_conn.cursor() as cursor:
                # Check next_of_kin_details
                cursor.execute(f"DESCRIBE {self.destination_table}")
                rows = cursor.fetchall()
                id_info = next((row for row in rows if row['Field'] == 'id'), None)
                if id_info and 'auto_increment' in (id_info.get('Extra') or '').lower():
                    logger.info(f"[OK] Destination table '{self.destination_table}' has auto-increment 'id'")
               
                # Check indexes
                cursor.execute(f"SHOW INDEX FROM {self.destination_table}")
                indexes = cursor.fetchall()
                lead_id_indexed = any(idx['Column_name'] == 'leadId' for idx in indexes)
               
                if lead_id_indexed:
                    logger.info("[OK] leadId column is indexed")
                else:
                    logger.warning("[WARNING] leadId column is NOT indexed - performance will be poor!")
               
                # Check current state
                cursor.execute(f"SHOW TABLE STATUS LIKE '{self.destination_table}'")
                status_row = cursor.fetchone()
                row_count = status_row.get('Rows', 'N/A') if status_row else 'N/A'
                logger.info(f" Current next_of_kin_details rows: ~{row_count:,}")
            dest_conn.close()
           
            # Check leads table
            leads_conn = self.connect_leads()
            with leads_conn.cursor() as cursor:
                cursor.execute(f"SHOW TABLE STATUS LIKE '{self.leads_table}'")
                status_row = cursor.fetchone()
                lead_count = status_row.get('Rows', 'N/A') if status_row else 'N/A'
                logger.info(f" Current leads rows: ~{lead_count:,}")
            leads_conn.close()
        except Exception as e:
            logger.error(f"Cannot access tables: {str(e)}")
            raise ValueError(f"Table issue: {str(e)}")
   
    def connect_staging(self) -> pymysql.Connection:
        """Create connection to staging database with optimized settings"""
        config = {k: v for k, v in self.staging_config.items() if k != 'table_name'}
        config['charset'] = 'utf8mb4'
        config['cursorclass'] = DictCursor
        config['connect_timeout'] = 30
        config['read_timeout'] = 600
        config['write_timeout'] = 600
        config['autocommit'] = False
        config['init_command'] = "SET SESSION sql_mode='NO_ENGINE_SUBSTITUTION'"
        return pymysql.connect(**config)
   
    def connect_destination(self) -> pymysql.Connection:
        """Create connection to destination database with optimized settings"""
        config = {k: v for k, v in self.destination_config.items() if k != 'table_name'}
        config['charset'] = 'utf8mb4'
        config['cursorclass'] = DictCursor
        config['connect_timeout'] = 30
        config['read_timeout'] = 600
        config['write_timeout'] = 600
        config['autocommit'] = False
        return pymysql.connect(**config)
   
    def connect_leads(self) -> pymysql.Connection:
        """Connection to leads table"""
        config = {k: v for k, v in self.leads_config.items() if k != 'table_name'}
        config['charset'] = 'utf8mb4'
        config['cursorclass'] = DictCursor
        config['connect_timeout'] = 30
        config['read_timeout'] = 600
        config['write_timeout'] = 600
        config['autocommit'] = False
        return pymysql.connect(**config)
   
    def fetch_pending_records(self, conn: pymysql.Connection, fetch_limit: Optional[int] = None) -> List[Dict]:
        """
        Fetch records using cursor-based pagination (WHERE leadId > last_lead_id)
        """
        limit_val = fetch_limit or self.batch_size
       
        with conn.cursor() as cursor:
            if self.last_lead_id is None:
                # First batch
                query = f"""
                    SELECT * FROM {self.staging_table}
                    ORDER BY leadId
                    LIMIT %s
                """
                cursor.execute(query, (limit_val,))
            else:
                # Subsequent batches
                query = f"""
                    SELECT * FROM {self.staging_table}
                    WHERE leadId > %s
                    ORDER BY leadId
                    LIMIT %s
                """
                cursor.execute(query, (self.last_lead_id, limit_val))
           
            return cursor.fetchall()

    def _process_batch(self, records: List[Dict]) -> int:
        """Process a batch of records: validate, check dups/FK, prepare for insert"""
        batch_to_insert = []
        batch_skipped = 0
        batch_duplicates = 0
        batch_fk_failed = 0
        batch_failed = 0

        for record in records:
            self.stats['total_fetched'] += 1

            lead_id = record.get('leadId')
            phone = record.get('phoneNumber', '')
            if not lead_id:
                batch_failed += 1
                self.stats['failed'] += 1
                self._log_failed_record(record, "Missing leadId")
                continue

            # Real duplicate check: leadId + phoneNumber combination
            if (lead_id, phone) in self.existing_lead_phone_pairs:
                batch_duplicates += 1
                self.stats['duplicates'] += 1
                self._log_failed_record(record, "Duplicate leadId+phoneNumber")
                continue

            # FK validation
            if lead_id not in self.existing_lead_ids:
                batch_fk_failed += 1
                self.stats['fk_failed'] += 1
                self._log_failed_record(record, f"leadId {lead_id} not found in leads table")
                continue

            # Valid record - prepare for insert
            record_copy = record.copy()
            if 'id' in record_copy:
                del record_copy['id']
            record_copy['is_migrated'] = 1
            record_copy['createdBy'] = record_copy.get('createdBy', 'migration_script')
            batch_to_insert.append(record_copy)

        logger.info(f"Batch validation: {len(records)} fetched | {len(batch_to_insert)} valid | "
                    f"{batch_skipped} skipped | {batch_duplicates} dups | {batch_fk_failed} FK fails | "
                    f"{batch_failed} other fails")

        if batch_to_insert:
            if self.dry_run:
                logger.info(f"[DRY-RUN] Would insert {len(batch_to_insert)} records (leadIds: {', '.join(r['leadId'] for r in batch_to_insert[:5])}...)")
            else:
                self._insert_batch(batch_to_insert)
                self.stats['successful'] += len(batch_to_insert)
                # Update cache with newly inserted lead+phone pairs
                for rec in batch_to_insert:
                    phone = rec.get('phoneNumber', '')
                    self.existing_lead_phone_pairs.add((rec['leadId'], phone))
                logger.info(f"[INSERTED] {len(batch_to_insert)} records")

        return len(records)

    def _insert_batch(self, batch: List[Dict]):
        """Perform batch insert into destination table"""
        if not batch:
            return

        conn = self.connect_destination()
        try:
            with conn.cursor() as cursor:
                # Assume all records have same structure; use first for columns
                columns = list(batch[0].keys())
                col_str = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))
                query = f"INSERT INTO {self.destination_table} ({col_str}) VALUES ({placeholders})"

                values = [tuple(rec[col] for col in columns) for rec in batch]

                if self.disable_fk_checks:
                    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

                cursor.executemany(query, values)
                conn.commit()

                if self.disable_fk_checks:
                    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

            logger.info(f"Batch insert successful: {len(batch)} rows")
        except Exception as e:
            conn.rollback()
            logger.error(f"Batch insert failed: {e}")
            for rec in batch:
                self._log_failed_record(rec, f"Insert error: {str(e)}")
                self.stats['failed'] += 1
            raise
        finally:
            conn.close()

    def run(self):
        """Main migration loop"""
        logger.info(f"Starting Next of Kin Migration - Dry Run: {self.dry_run}, Limit: {self.limit}, Batch Size: {self.batch_size}")
        if self.resume:
            logger.info(f"Resuming from leadId: {self.last_lead_id}")
        staging_conn = None
        total_processed = 0
        try:
            staging_conn = self.connect_staging()
            while True:
                remaining_limit = self.limit - total_processed if self.limit else self.batch_size
                if self.limit and remaining_limit <= 0:
                    logger.info(f"Limit {self.limit} reached. Stopping.")
                    break

                records = self.fetch_pending_records(staging_conn, remaining_limit)
                if not records:
                    logger.info("No more records to fetch. Migration complete.")
                    break

                logger.info(f"Fetched batch of {len(records)} records (starting from leadId > {self.last_lead_id or 'beginning'})")
                processed_in_batch = self._process_batch(records)
                total_processed += processed_in_batch

                if records:
                    self.last_lead_id = records[-1]['leadId']
                    self._save_checkpoint()

                if self.limit and total_processed >= self.limit:
                    logger.info(f"Processed {total_processed} records (limit hit). Stopping.")
                    break

            logger.info(f"Migration finished. Final stats: {self.stats}")
            logger.info(f"Check failed records CSV: {self.failed_csv_path}")
        except KeyboardInterrupt:
            logger.info("Migration interrupted by user.")
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise
        finally:
            if staging_conn:
                staging_conn.close()
            if self.failed_file:
                self.failed_file.close()

def main():
    parser = argparse.ArgumentParser(description="Migrate Next of Kin Details to dev")
    parser.add_argument('--dry-run', action='store_true', help='Simulate without inserting')
    parser.add_argument('--limit', type=int, help='Max records to process')
    parser.add_argument('--batch-size', type=int, default=5000, help='Records per batch')
    parser.add_argument('--disable-fk-checks', action='store_true', help='Disable FK checks during insert')
    parser.add_argument('--checkpoint', default='nok_migration_checkpoint.json', help='Checkpoint file path')
    parser.add_argument('--resume', action='store_true', help='Resume from last checkpoint')
    args = parser.parse_args()

    staging_config = MYSQL_CONFIGS['staging_db']
    destination_config = MYSQL_CONFIGS['destination_db']
    leads_config = MYSQL_CONFIGS['leads_db']

    migration = NextOfKinMigration(
        staging_config=staging_config,
        destination_config=destination_config,
        leads_config=leads_config,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        limit=args.limit,
        disable_fk_checks=args.disable_fk_checks,
        checkpoint_file=args.checkpoint,
        resume=args.resume
    )

    migration.run()

if __name__ == "__main__":
    main()