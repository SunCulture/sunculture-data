#!/usr/bin/env python3
"""
KYC Requests Migration Script - PERFORMANCE OPTIMIZED
Adapted from Lead Migration: Cursor pagination, batch inserts, pre-loaded dups, checkpointing.
Optimized for ~31k records: Fast batches, minimal roundtrips.
Now with FK pre-validation: Checks leadId exists in 'leads' table, logs missing ones to CSV.
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
        logging.FileHandler(f'kyc_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
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
        "table_name": "migrate_kyc_requests_v4"
    },
    "destination_db": {
        "host": os.getenv("SC_SALES_SERVICE_MYSQL_DB_HOST"),
        "port": int(os.getenv("MYSQL_DB_PORT", 3306)),
        "user": os.getenv("SC_SALES_SERVICE_MYSQL_DB_USER"),
        "password": os.getenv("SC_SALES_SERVICE_MYSQL_DB_PASSWORD"),
        "database": "sales-service",
        "table_name": "kyc_requests"
    },
    "leads_db": {  # Shared with destination
        "host": os.getenv("SC_SALES_SERVICE_MYSQL_DB_HOST"),
        "port": int(os.getenv("MYSQL_DB_PORT", 3306)),
        "user": os.getenv("SC_SALES_SERVICE_MYSQL_DB_USER"),
        "password": os.getenv("SC_SALES_SERVICE_MYSQL_DB_PASSWORD"),
        "database": "sales-service",
        "table_name": "leads"
    }
}


class KycMigration:
    def __init__(self, staging_config: Dict, destination_config: Dict, leads_config: Dict, dry_run: bool = False, 
                 batch_size: int = 5000, limit: Optional[int] = None, disable_fk_checks: bool = False,
                 checkpoint_file: str = "kyc_migration_checkpoint.json", resume: bool = False):
        """
        Initialize migration manager with performance optimizations
        
        Args:
            ... (mirrors LeadMigration)
        """
        self.staging_config = staging_config
        self.destination_config = destination_config
        self.leads_config = leads_config
        self.staging_table = staging_config.get('table_name', 'migrate_kyc_requests_v2')
        self.destination_table = destination_config.get('table_name', 'kyc_requests')
        self.leads_table = leads_config.get('table_name', 'leads')
        self.dry_run = dry_run
        self.batch_size = batch_size
        self.limit = limit
        self.disable_fk_checks = disable_fk_checks
        self.checkpoint_file = checkpoint_file
        self.resume = resume
        
        # Last processed externalRefId for cursor-based pagination
        self.last_external_ref_id = None
        
        self.stats = {
            'total_fetched': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'validation_failed': 0,
            'duplicates': 0,
            'fk_failed': 0  # NEW: Track FK validation fails
        }
        
        # Load checkpoint if resuming
        if self.resume:
            self._load_checkpoint()
        
        # CSV for failed records
        self.failed_csv_path = f'failed_kyc_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        self.failed_file = None
        self.failed_writer = None
        self._init_failed_csv()
        
        # Pre-load existing externalRefIds, (leadId, idNumber) pairs, and ALL leadIds for FK checks
        self.existing_external_refs = set()
        self.existing_lead_id_pairs = set()
        self.existing_lead_ids = set()  # NEW: For FK pre-validation
        #if not dry_run:
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
                    self.last_external_ref_id = checkpoint.get('last_external_ref_id')
                    self.stats = checkpoint.get('stats', self.stats)
                    logger.info(f"[RESUME] Loaded checkpoint - Last externalRefId: {self.last_external_ref_id}")
                    logger.info(f"[RESUME] Previous stats: {self.stats}")
            except Exception as e:
                logger.error(f"Failed to load checkpoint: {e}")
                logger.info("Starting fresh migration")
    
    def _save_checkpoint(self):
        """Save current progress to checkpoint file"""
        try:
            checkpoint = {
                'last_external_ref_id': self.last_external_ref_id,
                'stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f, indent=2)
            logger.debug(f"Checkpoint saved: externalRefId={self.last_external_ref_id}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def _preload_existing_data(self):
        """Pre-load existing externalRefIds, leadId+idNumber, and ALL leadIds from leads table"""
        logger.info("[INIT] Pre-loading existing data from destination...")
        try:
            # Load from kyc_requests
            dest_conn = self.connect_destination()
            with dest_conn.cursor() as cursor:
                # externalRefIds
                cursor.execute(f"SELECT externalRefId FROM {self.destination_table}")
                count = 0
                for row in cursor:
                    self.existing_external_refs.add(row['externalRefId'])
                    count += 1
                    if count % 10000 == 0:
                        logger.info(f"  Loaded {count:,} existing externalRefIds...")
                logger.info(f"[OK] Loaded {len(self.existing_external_refs):,} existing externalRefIds")
                
                # leadId + idNumber pairs
                cursor.execute(f"SELECT leadId, idNumber FROM {self.destination_table} WHERE idNumber IS NOT NULL")
                pair_count = 0
                for row in cursor:
                    self.existing_lead_id_pairs.add((row['leadId'], row['idNumber']))
                    pair_count += 1
                    if pair_count % 10000 == 0:
                        logger.info(f"  Loaded {pair_count:,} leadId+idNumber pairs...")
                logger.info(f"[OK] Loaded {len(self.existing_lead_id_pairs):,} leadId+idNumber pairs")
                
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
                        logger.info(f"  Loaded {lead_count:,} existing leadIds...")
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
                # Check kyc_requests
                cursor.execute(f"DESCRIBE {self.destination_table}")
                rows = cursor.fetchall()
                id_info = next((row for row in rows if row['Field'] == 'id'), None)
                if id_info and 'auto_increment' in (id_info.get('Extra') or '').lower():
                    logger.info(f"[OK] Destination table '{self.destination_table}' has auto-increment 'id'")
                
                # Check indexes
                cursor.execute(f"SHOW INDEX FROM {self.destination_table}")
                indexes = cursor.fetchall()
                external_ref_indexed = any(idx['Column_name'] == 'externalRefId' for idx in indexes)
                
                if external_ref_indexed:
                    logger.info("[OK] externalRefId column is indexed")
                else:
                    logger.warning("[WARNING] externalRefId column is NOT indexed - performance will be poor!")
                    logger.warning("  Run: CREATE INDEX idx_externalRefId ON kyc_requests(externalRefId);")
                
                # Check current state
                cursor.execute(f"SHOW TABLE STATUS LIKE '{self.destination_table}'")
                status_row = cursor.fetchone()
                row_count = status_row.get('Rows', 'N/A') if status_row else 'N/A'
                logger.info(f"  Current kyc_requests rows: ~{row_count:,}")
            dest_conn.close()
            
            # Quick check for leads table
            leads_conn = self.connect_leads()
            with leads_conn.cursor() as cursor:
                cursor.execute(f"SHOW TABLE STATUS LIKE '{self.leads_table}'")
                status_row = cursor.fetchone()
                lead_count = status_row.get('Rows', 'N/A') if status_row else 'N/A'
                logger.info(f"  Current leads rows: ~{lead_count:,}")
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
        """Connection to leads table (shared with destination)"""
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
        Fetch records using cursor-based pagination (WHERE externalRefId > last_external_ref_id)
        """
        limit_val = fetch_limit or self.batch_size
        
        with conn.cursor() as cursor:
            if self.last_external_ref_id is None:
                # First batch
                query = f"""
                    SELECT * FROM {self.staging_table} 
                    ORDER BY externalRefId
                    LIMIT %s
                """
                cursor.execute(query, (limit_val,))
            else:
                # Subsequent batches
                query = f"""
                    SELECT * FROM {self.staging_table} 
                    WHERE externalRefId > %s
                    ORDER BY externalRefId
                    LIMIT %s
                """
                cursor.execute(query, (self.last_external_ref_id, limit_val))
            
            return cursor.fetchall()
    
    def validate_record(self, record: Dict) -> Tuple[bool, Optional[str]]:
        """Validate KYC record (mirrors original)"""
        required_fields = ['externalRefId', 'leadId', 'idNumber', 'dob', 'status', 'documentType']
        
        for field in required_fields:
            if not record.get(field):
                return False, f"Missing required field: {field}"
        
        # Validate documentType enum
        valid_doc_types = [
            'NATIONAL_ID', 'NATIONAL_ID_NO_PHOTO', 'ALIEN_CARD', 'PASSPORT', 
            'KRA_PIN', 'TAX_INFORMATION', 'GHANA_CARD', 'GHANA_CARD_NO_PHOTO', 'VOTER_ID'
        ]
        if record.get('documentType') not in valid_doc_types:
            return False, f"Invalid documentType: {record.get('documentType')}"
        
        # Uganda constraint
        if record.get('companyRegionId') == 3:
            if not record.get('serialNumber') or record.get('serialNumber') == '':
                return False, "serialNumber is required for Uganda (companyRegionId=3)"
        
        return True, None
    
    def check_duplicates_batch_fast(self, records: List[Dict]) -> Tuple[Set[str], Dict[str, str]]:
        """
        Fast duplicate checking using pre-loaded sets
        Returns: (duplicate_external_ref_ids, dup_reasons)
        """
        if not records:
            return set(), {}
        
        duplicate_external_refs = set()
        dup_reasons = {}
        
        for record in records:
            external_ref = record['externalRefId']
            lead_id = record['leadId']
            id_num = record['idNumber']
            
            if external_ref in self.existing_external_refs:
                duplicate_external_refs.add(external_ref)
                dup_reasons[external_ref] = "Duplicate externalRefId"
            
            if (lead_id, id_num) in self.existing_lead_id_pairs:
                duplicate_external_refs.add(external_ref)
                dup_reasons[external_ref] = "Duplicate leadId+idNumber"
        
        return duplicate_external_refs, dup_reasons
    
    def check_fk_batch_fast(self, records: List[Dict]) -> Tuple[Set[str], Dict[str, str]]:
        """
        NEW: Fast FK checking for leadId using pre-loaded set
        Returns: (fk_fail_external_refs, fk_reasons)
        """
        if not records:
            return set(), {}
        
        fk_fail_external_refs = set()
        fk_reasons = {}
        
        for record in records:
            lead_id = record['leadId']
            if lead_id not in self.existing_lead_ids:
                external_ref = record['externalRefId']
                fk_fail_external_refs.add(external_ref)
                fk_reasons[external_ref] = f"Missing leadId FK: {lead_id} not in leads table"
        
        return fk_fail_external_refs, fk_reasons
    
    def prepare_record_for_insert(self, record: Dict) -> Dict:
        """Prepare a single record for insertion"""
        clean_record = {k: v for k, v in record.items() if k != 'id'}  # Exclude id for auto-increment
        
        # Convert empty strings to None
        for key, value in clean_record.items():
            if value == '':
                clean_record[key] = None
        
        # Ensure is_migrated=1
        clean_record['is_migrated'] = 1
        
        return clean_record
    
    def insert_records_batch(self, conn: pymysql.Connection, records: List[Dict]) -> int:
        """
        Batch insert multiple records
        """
        if not records:
            return 0
        
        prepared_records = [self.prepare_record_for_insert(r) for r in records]
        
        columns = list(prepared_records[0].keys())
        column_str = ', '.join(f'`{col}`' for col in columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        values_list = [tuple(r[col] for col in columns) for r in prepared_records]
        
        query = f"INSERT INTO {self.destination_table} ({column_str}) VALUES ({placeholders})"
        
        try:
            with conn.cursor() as cursor:
                if self.disable_fk_checks:
                    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
                
                rows_affected = cursor.executemany(query, values_list)
                
                if self.disable_fk_checks:
                    cursor.execute("SET FOREIGN_KEY_CHECKS=1")
                
                # Update in-memory sets with new data
                for r in records:
                    self.existing_external_refs.add(r['externalRefId'])
                    lead_id = r['leadId']
                    id_num = r['idNumber']
                    if id_num:
                        self.existing_lead_id_pairs.add((lead_id, id_num))
                
                return rows_affected if rows_affected else len(values_list)
        except Exception as e:
            logger.error(f"Batch INSERT failed: {str(e)}")
            if self.disable_fk_checks:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
                except:
                    pass
            raise
    
    def process_batch(self, staging_conn: pymysql.Connection, 
                     dest_conn: pymysql.Connection, records: List[Dict]):
        """Process a batch of records (optimized)"""
        
        if not records:
            return
        
        # Update cursor position
        self.last_external_ref_id = records[-1]['externalRefId']
        
        # Step 1: Deduplicate within batch (externalRefId)
        seen_external_refs = set()
        unique_records = []
        batch_dups = 0
        
        for record in records:
            external_ref = record['externalRefId']
            if external_ref in seen_external_refs:
                logger.debug(f"[SKIP] Duplicate externalRefId within batch: {external_ref}")
                self._log_failed_record(record.copy(), "Duplicate externalRefId within staging batch")
                batch_dups += 1
            else:
                seen_external_refs.add(external_ref)
                unique_records.append(record)
        
        if batch_dups > 0:
            logger.info(f"[INFO] Removed {batch_dups} duplicates within this batch")
            self.stats['duplicates'] += batch_dups
        
        # Step 2: Validate all unique records
        valid_records = []
        
        for record in unique_records:
            is_valid, error_msg = self.validate_record(record)
            if not is_valid:
                logger.warning(f"Record {record['externalRefId']} validation failed: {error_msg}")
                self._log_failed_record(record.copy(), f"Validation failed: {error_msg}")
                self.stats['validation_failed'] += 1
            else:
                valid_records.append(record)
        
        if not valid_records:
            logger.info("[INFO] No valid records in this batch")
            return
        
        # Step 3: Fast duplicate checking using in-memory sets
        duplicate_external_refs, dup_reasons = self.check_duplicates_batch_fast(valid_records)
        
        # Step 3.5: NEW - Fast FK checking for leadId
        fk_fail_external_refs, fk_reasons = self.check_fk_batch_fast(valid_records)
        
        insertable_records = []
        
        for record in valid_records:
            external_ref = record['externalRefId']
            
            # Check dups
            if external_ref in duplicate_external_refs:
                reason = dup_reasons.get(external_ref, "Duplicate")
                logger.debug(f"Record {external_ref} is a duplicate: {reason}")
                self._log_failed_record(record.copy(), reason)
                self.stats['duplicates'] += 1
                continue
            
            # Check FK
            if external_ref in fk_fail_external_refs:
                reason = fk_reasons.get(external_ref, "Missing leadId FK")
                logger.warning(f"[FK FAIL] {external_ref}: {reason}")
                self._log_failed_record(record.copy(), reason)
                self.stats['fk_failed'] += 1
                continue
            
            insertable_records.append(record)
        
        if not insertable_records:
            logger.info("[INFO] No insertable records in this batch (all duplicates/invalid/FK fails)")
            return
        
        # Step 4: Batch insert
        if not self.dry_run:
            try:
                rows_inserted = self.insert_records_batch(dest_conn, insertable_records)
                dest_conn.commit()
                self.stats['successful'] += len(insertable_records)
                logger.info(f"[OK] Successfully inserted {rows_inserted} records in batch")
                if self.stats['fk_failed'] > 0:
                    logger.warning(f"[WARNING] {self.stats['fk_failed']} FK fails logged to CSV in this batch")
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Batch insert failed: {error_msg}")
                try:
                    dest_conn.rollback()
                except:
                    logger.warning("Could not rollback - connection may be lost")
                
                for record in insertable_records:
                    self._log_failed_record(record.copy(), f"Batch insert error: {error_msg}")
                
                self.stats['failed'] += len(insertable_records)
                raise
        else:
            logger.info(f"[DRY RUN] Would insert {len(insertable_records)} records")
            self.stats['successful'] += len(insertable_records)
    
    def run(self):
        """Execute the migration with optimizations"""
        mode = "DRY RUN" if self.dry_run else "LIVE MIGRATION"
        logger.info("="*80)
        logger.info(f"Starting KYC Migration - {mode}")
        logger.info(f"Batch Size: {self.batch_size}, Limit: {self.limit or 'No limit'}")
        if self.resume:
            logger.info(f"[RESUME] Continuing from externalRefId: {self.last_external_ref_id}")
        if self.disable_fk_checks:
            logger.info("[WARNING] Foreign key checks will be DISABLED during insert")
        logger.info("="*80)
        
        staging_conn = None
        dest_conn = None
        
        try:
            staging_conn = self.connect_staging()
            dest_conn = self.connect_destination()
            
            logger.info("[OK] Database connections established")
            
            total_processed = 0
            batch_num = 1
            start_time = datetime.now()
            
            while True:
                # Check limit
                if self.limit and total_processed >= self.limit:
                    logger.info(f"Reached limit of {self.limit} records")
                    break
                
                # Adjust batch size for limit
                fetch_size = self.batch_size
                if self.limit:
                    fetch_size = min(self.batch_size, self.limit - total_processed)
                
                # Fetch batch using cursor-based pagination
                batch_start = datetime.now()
                records = self.fetch_pending_records(staging_conn, fetch_size)
                
                if not records:
                    logger.info("No more pending records to process")
                    break
                
                self.stats['total_fetched'] += len(records)
                logger.info(f"\n--- Batch {batch_num}: {len(records)} records (last_id: {self.last_external_ref_id or 'start'}) ---")
                
                # Process batch
                self.process_batch(staging_conn, dest_conn, records)
                
                # Save checkpoint every batch
                self._save_checkpoint()
                
                batch_duration = (datetime.now() - batch_start).total_seconds()
                records_per_sec = len(records) / batch_duration if batch_duration > 0 else 0
                
                total_processed += len(records)
                batch_num += 1
                
                # Log progress
                elapsed = (datetime.now() - start_time).total_seconds()
                overall_rate = total_processed / elapsed if elapsed > 0 else 0
                
                logger.info(f"Batch completed in {batch_duration:.2f}s ({records_per_sec:.1f} records/sec)")
                logger.info(f"Progress: {total_processed} processed | "
                          f"[OK] {self.stats['successful']} successful | "
                          f"[FAIL] {self.stats['failed']} failed | "
                          f"[SKIP] {self.stats['skipped'] + self.stats['duplicates'] + self.stats['fk_failed']} skipped")
                logger.info(f"Overall rate: {overall_rate:.1f} records/sec")
                
                # Estimate remaining time
                if self.limit and overall_rate > 0:
                    remaining = self.limit - total_processed
                    eta_seconds = remaining / overall_rate
                    eta_mins = eta_seconds / 60
                    logger.info(f"ETA: {eta_mins:.1f} minutes")
            
            # Final summary
            total_duration = (datetime.now() - start_time).total_seconds()
            logger.info("\n" + "="*80)
            logger.info(f"MIGRATION COMPLETE - {mode}")
            logger.info("="*80)
            logger.info(f"Total Fetched:      {self.stats['total_fetched']}")
            logger.info(f"[OK] Successful:    {self.stats['successful']}")
            logger.info(f"[FAIL] Failed:      {self.stats['failed']}")
            logger.info(f"  - Validation:     {self.stats['validation_failed']}")
            logger.info(f"  - Duplicates:     {self.stats['duplicates']}")
            logger.info(f"  - FK Fails:       {self.stats['fk_failed']}")
            logger.info(f"[SKIP] Skipped:     {self.stats['skipped']}")
            logger.info(f"\nTotal Duration:     {total_duration/60:.2f} minutes")
            logger.info(f"Average Rate:       {self.stats['total_fetched']/total_duration:.1f} records/sec")
            logger.info("="*80)
            
            if self.stats['failed'] > 0 or self.stats['fk_failed'] > 0:
                logger.warning(f"\n[WARNING] {self.stats['failed'] + self.stats['fk_failed']} records failed/skipped.")
                logger.warning(f"  Failed records exported to: {self.failed_csv_path}")
            
            # Clean up checkpoint on success
            if Path(self.checkpoint_file).exists():
                Path(self.checkpoint_file).unlink()
                logger.info("Checkpoint file removed (migration complete)")
            
        except KeyboardInterrupt:
            logger.warning("\n[INTERRUPTED] Migration stopped by user")
            logger.info(f"Resume with --resume flag. Last processed externalRefId: {self.last_external_ref_id}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"\nMigration failed with error: {str(e)}", exc_info=True)
            logger.info(f"Resume with --resume flag. Last processed externalRefId: {self.last_external_ref_id}")
            sys.exit(1)
        finally:
            if staging_conn:
                staging_conn.close()
            if dest_conn:
                dest_conn.close()
            if self.failed_file:
                self.failed_file.close()
            logger.info("\nDatabase connections closed")


def main():
    parser = argparse.ArgumentParser(
        description='Migrate KYC requests from staging to production database (PERFORMANCE OPTIMIZED)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run with 100 records
  python %(prog)s --dry-run --limit 100

  # Full migration with optimal settings (for ~31k records)
  python %(prog)s --batch-size 5000

  # Resume interrupted migration
  python %(prog)s --resume --batch-size 5000

  # Large migration with progress tracking
  python %(prog)s --batch-size 10000 --limit 31000
        """
    )
    
    parser.add_argument('--dry-run', action='store_true', 
                       help='Run without writing to destination (test mode)')
    parser.add_argument('--batch-size', type=int, default=5000, 
                       help='Number of records per batch (default: 5000, recommended: 5000-10000 for 31k total)')
    parser.add_argument('--limit', type=int, 
                       help='Maximum number of records to migrate (default: all)')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from last checkpoint')
    parser.add_argument('--checkpoint-file', default='kyc_migration_checkpoint.json',
                       help='Path to checkpoint file (default: kyc_migration_checkpoint.json)')
    parser.add_argument('--staging-table', 
                       help='Override staging table name')
    parser.add_argument('--destination-table', 
                       help='Override destination table name')
    parser.add_argument('--debug', action='store_true',
                       help='Enable DEBUG logging')
    parser.add_argument('--disable-fk-checks', action='store_true',
                       help='Disable foreign key checks during insert (use if pre-validation misses)')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Build configs
    staging_config = MYSQL_CONFIGS['staging_db'].copy()
    destination_config = MYSQL_CONFIGS['destination_db'].copy()
    leads_config = MYSQL_CONFIGS['leads_db'].copy()
    
    if args.staging_table:
        staging_config['table_name'] = args.staging_table
    if args.destination_table:
        destination_config['table_name'] = args.destination_table
    
    # Validate env vars
    if not staging_config['host']:
        logger.error("ERROR: Database host not configured in .env file")
        sys.exit(1)
    
    # Run migration
    migration = KycMigration(
        staging_config=staging_config,
        destination_config=destination_config,
        leads_config=leads_config,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        limit=args.limit,
        disable_fk_checks=args.disable_fk_checks,
        checkpoint_file=args.checkpoint_file,
        resume=args.resume
    )
    
    migration.run()


if __name__ == '__main__':
    main()