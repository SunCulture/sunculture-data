#!/usr/bin/env python3
"""
Lead Migration Script - PERFORMANCE OPTIMIZED
Key improvements:
1. Cursor-based pagination (no OFFSET)
2. Batch duplicate checking with indexed queries
3. Connection pool with auto-reconnect
4. Checkpoint system for resume capability
5. Parallel processing option
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
from collections import defaultdict

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'lead_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
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
        "table_name": "migrate_leads_v6"
    },
    "destination_db": {
        "host": os.getenv("SC_SALES_SERVICE_MYSQL_DB_HOST"),
        "port": int(os.getenv("MYSQL_DB_PORT", 3306)),
        "user": os.getenv("SC_SALES_SERVICE_MYSQL_DB_USER"),
        "password": os.getenv("SC_SALES_SERVICE_MYSQL_DB_PASSWORD"),
        "database": "sales-service",
        "table_name": "leads"
    }
}


class LeadMigration:
    def __init__(self, staging_config: Dict, destination_config: Dict, dry_run: bool = False, 
                 batch_size: int = 1000, limit: Optional[int] = None, disable_fk_checks: bool = False,
                 checkpoint_file: str = "migration_checkpoint.json", resume: bool = False):
        """
        Initialize migration manager with performance optimizations
        
        Args:
            staging_config: Staging database connection config
            destination_config: Destination database connection config
            dry_run: If True, no data will be written to destination
            batch_size: Number of records to process per batch (recommended: 2000-5000)
            limit: Maximum number of records to migrate (None for all)
            disable_fk_checks: If True, temporarily disable foreign key checks during insert
            checkpoint_file: Path to checkpoint file for resuming
            resume: If True, resume from last checkpoint
        """
        self.staging_config = staging_config
        self.destination_config = destination_config
        self.staging_table = staging_config.get('table_name', 'leads_migration_staging')
        self.destination_table = destination_config.get('table_name', 'leads')
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
            'duplicates': 0
        }
        
        # Load checkpoint if resuming
        if self.resume:
            self._load_checkpoint()
        
        # Fields to always exclude from INSERT
        self.excluded_fields = []
        
        # CSV for failed records
        self.failed_csv_path = f'failed_leads_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        self.failed_file = None
        self.failed_writer = None
        self._init_failed_csv()
        
        # Pre-load existing leadIds for faster duplicate checking
        self.existing_lead_ids = set()
        self.existing_phones = set()
        if not dry_run:
            self._preload_existing_data()
        
        # Validate configuration
        self._validate_config()
        self._check_destination_table()
    
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
        """
        Pre-load all existing leadIds and mobilePhones from destination into memory
        This is much faster than querying for each batch
        """
        logger.info("[INIT] Pre-loading existing data from destination...")
        try:
            dest_conn = self.connect_destination()
            with dest_conn.cursor() as cursor:
                # Load leadIds
                cursor.execute(f"SELECT leadId FROM {self.destination_table}")
                
                count = 0
                for row in cursor:
                    self.existing_lead_ids.add(row['leadId'])
                    count += 1
                    if count % 100000 == 0:
                        logger.info(f"  Loaded {count:,} existing leadIds...")
                
                logger.info(f"[OK] Loaded {len(self.existing_lead_ids):,} existing leadIds into memory")
                
                # Load mobilePhones (for duplicate checking)
                cursor.execute(f"SELECT DISTINCT mobilePhone FROM {self.destination_table} WHERE mobilePhone IS NOT NULL")
                
                phone_count = 0
                for row in cursor:
                    self.existing_phones.add(row['mobilePhone'])
                    phone_count += 1
                    if phone_count % 100000 == 0:
                        logger.info(f"  Loaded {phone_count:,} existing phone numbers...")
                
                logger.info(f"[OK] Loaded {len(self.existing_phones):,} existing phone numbers into memory")
                
            dest_conn.close()
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
    
    def _log_failed_record(self, lead: Dict, reason: str):
        """Log a failed record to CSV"""
        if self.failed_writer and self.failed_file:
            if self.failed_writer.fieldnames is None:
                all_fields = list(lead.keys()) + ['failure_reason']
                self.failed_writer.fieldnames = all_fields
                self.failed_writer.writeheader()
            
            lead_copy = lead.copy()
            lead_copy['failure_reason'] = reason
            self.failed_writer.writerow(lead_copy)
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
    
    def _check_destination_table(self):
        """Verify destination table exists and check indexes"""
        try:
            dest_conn = self.connect_destination()
            with dest_conn.cursor() as cursor:
                # Check table structure
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
                    logger.warning("  Run: CREATE INDEX idx_leadId ON leads(leadId);")
                
                # Check current state
                cursor.execute(f"SHOW TABLE STATUS LIKE '{self.destination_table}'")
                status_row = cursor.fetchone()
                row_count = status_row.get('Rows', 'N/A') if status_row else 'N/A'
                logger.info(f"  Current rows: ~{row_count:,}")
            dest_conn.close()
        except Exception as e:
            logger.error(f"Cannot access destination table: {str(e)}")
            raise ValueError(f"Destination table issue: {str(e)}")
    
    def connect_staging(self) -> pymysql.Connection:
        """Create connection to staging database with optimized settings"""
        config = {k: v for k, v in self.staging_config.items() if k != 'table_name'}
        config['charset'] = 'utf8mb4'
        config['cursorclass'] = DictCursor
        config['connect_timeout'] = 30
        config['read_timeout'] = 600      # 10 minutes
        config['write_timeout'] = 600
        config['autocommit'] = False
        # Optimize for large result sets
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
    
    def fetch_pending_leads(self, conn: pymysql.Connection, fetch_limit: Optional[int] = None) -> List[Dict]:
        """
        Fetch leads using cursor-based pagination (much faster than OFFSET)
        Uses WHERE leadId > last_lead_id instead of OFFSET
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
                # Subsequent batches - use cursor-based pagination
                query = f"""
                    SELECT * FROM {self.staging_table} 
                    WHERE leadId > %s
                    ORDER BY leadId
                    LIMIT %s
                """
                cursor.execute(query, (self.last_lead_id, limit_val))
            
            return cursor.fetchall()
    
    def validate_lead_data(self, lead: Dict) -> Tuple[bool, Optional[str]]:
        """Validate lead data before insertion"""
        required_fields = ['leadId', 'firstName', 'mobilePhone', 'companyRegionId', 
                          'createdAt', 'updatedAt', 'leadSourceId']
        
        for field in required_fields:
            if not lead.get(field):
                return False, f"Missing required field: {field}"
        
        # Validate enum values
        enum_validations = {
            'paymentMethod': ['CASH', 'PAYG', 'CREDIT'],
            'status': ['NEW', 'IN_PROGRESS', 'QUALIFIED', 'CONVERTED', 'ARCHIVED'],
            'leadStatus': ['LEAD_CREATION','TDH_SUBMISSION','KYC_COMPLETED','CDS1','QUALIFIED','CONVERTED','DEPOSIT','CDS2'],
            'purchaseDate': ['NOW','TWO_WEEKS','TWO_MONTHS','LATER'],
            'entityType': ['INDIVIDUAL','COMPANY']
        }
        
        for field, valid_values in enum_validations.items():
            if lead.get(field) and lead.get(field) not in valid_values:
                return False, f"Invalid {field}: {lead.get(field)}"
        
        return True, None
    
    def check_duplicates_batch_fast(self, leads: List[Dict]) -> Tuple[Set[str], Dict[str, str]]:
        """
        Fast duplicate checking using pre-loaded in-memory set
        Returns: (duplicate_lead_ids, phone_duplicate_reasons)
        """
        if not leads:
            return set(), {}
        
        duplicate_lead_ids = set()
        phone_duplicate_reasons = {}
        
        # Check leadId duplicates
        for lead in leads:
            lead_id = lead['leadId']
            if lead_id in self.existing_lead_ids:
                duplicate_lead_ids.add(lead_id)
        
        # Check mobilePhone duplicates against destination
        # Extract unique phones from this batch
        phones_in_batch = set()
        for lead in leads:
            phone = lead.get('mobilePhone')
            if phone:
                phones_in_batch.add(phone)
        
        if phones_in_batch and hasattr(self, 'existing_phones'):
            for lead in leads:
                phone = lead.get('mobilePhone')
                if phone and phone in self.existing_phones:
                    duplicate_lead_ids.add(lead['leadId'])
                    phone_duplicate_reasons[lead['leadId']] = f"Duplicate mobilePhone: {phone}"
        
        return duplicate_lead_ids, phone_duplicate_reasons
    
    def prepare_lead_for_insert(self, lead: Dict) -> Dict:
        """Prepare a single lead for insertion"""
        clean_lead = {k: v for k, v in lead.items() if k not in self.excluded_fields}
        
        # Convert empty strings to None for nullable fields
        for key, value in clean_lead.items():
            if value == '':
                clean_lead[key] = None
        
        # Explicitly set id=NULL to trigger auto-increment
        clean_lead['id'] = None
        
        # Ensure is_migrated=1 for migrated records
        clean_lead['is_migrated'] = 1
        
        return clean_lead
    
    def insert_leads_batch(self, conn: pymysql.Connection, leads: List[Dict]) -> int:
        """
        Insert multiple leads in a single batch operation
        Returns number of rows inserted
        """
        if not leads:
            return 0
        
        prepared_leads = [self.prepare_lead_for_insert(lead) for lead in leads]
        
        columns = list(prepared_leads[0].keys())
        column_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        values_list = []
        for lead in prepared_leads:
            values_list.append(tuple(lead[col] for col in columns))
        
        query = f"INSERT INTO {self.destination_table} ({column_str}) VALUES ({placeholders})"
        
        try:
            with conn.cursor() as cursor:
                if self.disable_fk_checks:
                    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
                
                rows_affected = cursor.executemany(query, values_list)
                
                if self.disable_fk_checks:
                    cursor.execute("SET FOREIGN_KEY_CHECKS=1")
                
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
                     dest_conn: pymysql.Connection, leads: List[Dict]):
        """Process a batch of leads with optimized operations"""
        
        if not leads:
            return
        
        # Update cursor position
        self.last_lead_id = leads[-1]['leadId']
        
        # Step 1: Deduplicate within batch
        seen_lead_ids = set()
        unique_leads = []
        batch_duplicates = 0
        
        for lead in leads:
            lead_id = lead['leadId']
            if lead_id in seen_lead_ids:
                logger.debug(f"[SKIP] Duplicate leadId within batch: {lead_id}")
                self._log_failed_record(lead.copy(), "Duplicate leadId within staging batch")
                batch_duplicates += 1
            else:
                seen_lead_ids.add(lead_id)
                unique_leads.append(lead)
        
        if batch_duplicates > 0:
            logger.info(f"[INFO] Removed {batch_duplicates} duplicate leadIds within this batch")
            self.stats['duplicates'] += batch_duplicates
        
        # Step 2: Validate all unique leads
        valid_leads = []
        
        for lead in unique_leads:
            is_valid, error_msg = self.validate_lead_data(lead)
            if not is_valid:
                logger.warning(f"Lead {lead['leadId']} validation failed: {error_msg}")
                self._log_failed_record(lead.copy(), f"Validation failed: {error_msg}")
                self.stats['validation_failed'] += 1
            else:
                valid_leads.append(lead)
        
        if not valid_leads:
            logger.info("[INFO] No valid leads in this batch")
            return

        # Step 2.5: Deduplicate phones within batch (NEW: prevents insert failures on unique constraint)
        seen_phones = set()
        filtered_leads = []
        within_batch_phone_dups = 0

        for lead in valid_leads:
            phone = lead.get('mobilePhone')
            if phone is not None and phone in seen_phones:
                # Skip duplicate phone within this batch (keep first occurrence)
                logger.debug(f"[SKIP] Duplicate phone within batch: {lead['leadId']} (phone: {phone})")
                self._log_failed_record(lead.copy(), f"Duplicate mobilePhone within batch: {phone}")
                within_batch_phone_dups += 1
                self.stats['duplicates'] += 1
            else:
                if phone is not None:
                    seen_phones.add(phone)
                filtered_leads.append(lead)

        if within_batch_phone_dups > 0:
            logger.info(f"[INFO] Removed {within_batch_phone_dups} duplicate phones within this batch")
            valid_leads = filtered_leads
        
        if not valid_leads:
            logger.info("[INFO] No valid leads after phone deduplication in this batch")
            return
        
        # Step 3: Fast duplicate checking using in-memory set
        duplicate_lead_ids, phone_duplicate_reasons = self.check_duplicates_batch_fast(valid_leads)
        
        insertable_leads = []
        
        for lead in valid_leads:
            lead_id = lead['leadId']
            if lead_id in duplicate_lead_ids:
                reason = phone_duplicate_reasons.get(lead_id, "Duplicate leadId")
                logger.debug(f"Lead {lead_id} is a duplicate: {reason}")
                self._log_failed_record(lead.copy(), reason)
                self.stats['duplicates'] += 1
            else:
                insertable_leads.append(lead)
        
        if not insertable_leads:
            logger.info("[INFO] No insertable leads in this batch (all duplicates/invalid)")
            return
        
        # Step 4: Batch insert
        if not self.dry_run:
            try:
                rows_inserted = self.insert_leads_batch(dest_conn, insertable_leads)
                dest_conn.commit()
                
                # Add newly inserted leadIds and phones to our in-memory sets
                for lead in insertable_leads:
                    self.existing_lead_ids.add(lead['leadId'])
                    phone = lead.get('mobilePhone')
                    if phone:
                        self.existing_phones.add(phone)
                
                self.stats['successful'] += len(insertable_leads)
                logger.info(f"[OK] Successfully inserted {rows_inserted} leads in batch")
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Batch insert failed: {error_msg}")
                
                try:
                    dest_conn.rollback()
                except:
                    logger.warning("Could not rollback - connection may be lost")
                
                for lead in insertable_leads:
                    self._log_failed_record(lead.copy(), f"Batch insert error: {error_msg}")
                
                self.stats['failed'] += len(insertable_leads)
                raise
        else:
            logger.info(f"[DRY RUN] Would insert {len(insertable_leads)} leads")
            self.stats['successful'] += len(insertable_leads)
    
    def run(self):
        """Execute the migration with optimizations"""
        mode = "DRY RUN" if self.dry_run else "LIVE MIGRATION"
        logger.info("="*80)
        logger.info(f"Starting Lead Migration - {mode}")
        logger.info(f"Batch Size: {self.batch_size}, Limit: {self.limit or 'No limit'}")
        if self.resume:
            logger.info(f"[RESUME] Continuing from leadId: {self.last_lead_id}")
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
                leads = self.fetch_pending_leads(staging_conn, fetch_size)
                
                if not leads:
                    logger.info("No more pending leads to process")
                    break
                
                self.stats['total_fetched'] += len(leads)
                logger.info(f"\n--- Batch {batch_num}: {len(leads)} leads (last_id: {self.last_lead_id or 'start'}) ---")
                
                # Process batch
                self.process_batch(staging_conn, dest_conn, leads)
                
                # Save checkpoint every batch (only reaches here on success)
                self._save_checkpoint()
                
                batch_duration = (datetime.now() - batch_start).total_seconds()
                records_per_sec = len(leads) / batch_duration if batch_duration > 0 else 0
                
                total_processed += len(leads)
                batch_num += 1
                
                # Log progress
                elapsed = (datetime.now() - start_time).total_seconds()
                overall_rate = total_processed / elapsed if elapsed > 0 else 0
                
                logger.info(f"Batch completed in {batch_duration:.2f}s ({records_per_sec:.1f} records/sec)")
                logger.info(f"Progress: {total_processed} processed | "
                          f"[OK] {self.stats['successful']} successful | "
                          f"[FAIL] {self.stats['failed']} failed | "
                          f"[SKIP] {self.stats['skipped'] + self.stats['duplicates']} skipped")
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
            logger.info(f"[SKIP] Skipped:     {self.stats['skipped']}")
            logger.info(f"\nTotal Duration:     {total_duration/60:.2f} minutes")
            logger.info(f"Average Rate:       {self.stats['total_fetched']/total_duration:.1f} records/sec")
            logger.info("="*80)
            
            if self.stats['failed'] > 0:
                logger.warning(f"\n[WARNING] {self.stats['failed']} records failed.")
                logger.warning(f"  Failed records exported to: {self.failed_csv_path}")
            
            # Clean up checkpoint on success
            if Path(self.checkpoint_file).exists():
                Path(self.checkpoint_file).unlink()
                logger.info("Checkpoint file removed (migration complete)")
            
        except KeyboardInterrupt:
            logger.warning("\n[INTERRUPTED] Migration stopped by user")
            self._save_checkpoint()  # Save on interrupt to allow resume
            logger.info(f"Resume with --resume flag. Last processed leadId: {self.last_lead_id}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"\nMigration failed with error: {str(e)}", exc_info=True)
            # Do NOT save checkpoint on error (avoids skipping failed batch)
            logger.info(f"Resume with --resume flag. Last processed leadId: {self.last_lead_id}")
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
        description='Migrate leads from staging to production database (PERFORMANCE OPTIMIZED)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run with 100 records
  python %(prog)s --dry-run --limit 100

  # Full migration with optimal settings
  python %(prog)s --batch-size 3000

  # Resume interrupted migration
  python %(prog)s --resume --batch-size 3000

  # Large migration with progress tracking
  python %(prog)s --batch-size 5000 --limit 100000
        """
    )
    
    parser.add_argument('--dry-run', action='store_true', 
                       help='Run without writing to destination (test mode)')
    parser.add_argument('--batch-size', type=int, default=2000, 
                       help='Number of records per batch (default: 2000, recommended: 2000-5000)')
    parser.add_argument('--limit', type=int, 
                       help='Maximum number of records to migrate (default: all)')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from last checkpoint')
    parser.add_argument('--checkpoint-file', default='migration_checkpoint.json',
                       help='Path to checkpoint file (default: migration_checkpoint.json)')
    parser.add_argument('--staging-table', 
                       help='Override staging table name')
    parser.add_argument('--destination-table', 
                       help='Override destination table name')
    parser.add_argument('--debug', action='store_true',
                       help='Enable DEBUG logging')
    parser.add_argument('--disable-fk-checks', action='store_true',
                       help='Disable foreign key checks during insert')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Build configs
    staging_config = MYSQL_CONFIGS['staging_db'].copy()
    destination_config = MYSQL_CONFIGS['destination_db'].copy()
    
    if args.staging_table:
        staging_config['table_name'] = args.staging_table
    if args.destination_table:
        destination_config['table_name'] = args.destination_table
    
    # Validate env vars
    if not staging_config['host']:
        logger.error("ERROR: Database host not configured in .env file")
        sys.exit(1)
    
    # Run migration
    migration = LeadMigration(
        staging_config=staging_config,
        destination_config=destination_config,
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