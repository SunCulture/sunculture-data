#!/usr/bin/env python3
"""
Lead Delta Sync Script - INCREMENTAL UPDATES
Purpose: Sync updates from legacy staging to destination based on updatedAt.
Assumes bulk migration is complete; only handles deltas (no inserts/deletes).
Key features:
1. Watermark-based fetching (WHERE updatedAt > last_sync_at)
2. Batch comparison and targeted UPDATEs
3. Resumable with checkpointing
4. Dry-run mode for safety
"""

import argparse
import logging
import sys
import csv
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
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
        logging.FileHandler(f'lead_delta_sync_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Database Configuration (same as migration script)
MYSQL_CONFIGS = {
    "staging_db": {
        "host": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_HOST"),
        "port": int(os.getenv("MYSQL_DB_PORT", 3306)),
        "user": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_USER"),
        "password": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_PASSWORD"),
        "database": "data-migration-staging",
        "table_name": "migrate_leads_v4"
    },
    "destination_db": {
        "host": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_HOST"),
        "port": int(os.getenv("MYSQL_DB_PORT", 3306)),
        "user": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_USER"),
        "password": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_PASSWORD"),
        "database": "sales-service-stage",
        "table_name": "leads"
    }
}

class LeadDeltaSync:
    def __init__(self, staging_config: Dict, destination_config: Dict, dry_run: bool = False,
                 batch_size: int = 1000, initial_cutoff: Optional[str] = None,
                 checkpoint_file: str = "delta_sync_checkpoint.json", resume: bool = False,
                 disable_fk_checks: bool = False):
        """
        Initialize delta sync manager.
        
        Args:
            ... (similar to migration)
            initial_cutoff: ISO datetime for first-run watermark (e.g., '2025-11-05T20:00:00Z')
        """
        self.staging_config = staging_config
        self.destination_config = destination_config
        self.staging_table = staging_config.get('table_name', 'migrate_leads_v4')
        self.destination_table = destination_config.get('table_name', 'leads')
        self.dry_run = dry_run
        self.batch_size = batch_size
        self.initial_cutoff = initial_cutoff
        self.disable_fk_checks = disable_fk_checks
        self.checkpoint_file = checkpoint_file
        self.resume = resume
        
        # Watermark for incremental sync
        self.last_sync_at = None
        
        self.stats = {
            'total_fetched': 0,
            'updated': 0,
            'skipped': 0,
            'failed': 0,
            'no_change': 0
        }
        
        # Load checkpoint if resuming
        if self.resume:
            self._load_checkpoint()
        elif self.initial_cutoff:
            self.last_sync_at = self.initial_cutoff
            logger.info(f"[INIT] Initial cutoff set to: {self.last_sync_at}")
        else:
            raise ValueError("First run requires --initial-cutoff (e.g., migration end time)")
        
        # CSV for updated records (with diffs)
        self.updated_csv_path = f'updated_leads_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        self.updated_file = None
        self.updated_writer = None
        self._init_updated_csv()
        
        # Validate config
        self._validate_config()
        self._check_tables()
    
    def _load_checkpoint(self):
        """Load checkpoint from previous run"""
        if Path(self.checkpoint_file).exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    self.last_sync_at = checkpoint.get('last_sync_at')
                    self.stats = checkpoint.get('stats', self.stats)
                    logger.info(f"[RESUME] Loaded checkpoint - Last sync at: {self.last_sync_at}")
                    logger.info(f"[RESUME] Previous stats: {self.stats}")
            except Exception as e:
                logger.error(f"Failed to load checkpoint: {e}")
                raise
        else:
            logger.warning("No checkpoint found; starting fresh with initial_cutoff")
    
    def _save_checkpoint(self):
        """Save current progress"""
        try:
            checkpoint = {
                'last_sync_at': self.last_sync_at,
                'stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f, indent=2)
            logger.debug(f"Checkpoint saved: sync_at={self.last_sync_at}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def _init_updated_csv(self):
        """Initialize CSV for updated records"""
        try:
            self.updated_file = open(self.updated_csv_path, 'w', newline='', encoding='utf-8')
            self.updated_writer = csv.DictWriter(self.updated_file, fieldnames=None)
            logger.info(f"Updated records CSV initialized: {self.updated_csv_path}")
        except Exception as e:
            logger.error(f"Failed to initialize CSV: {str(e)}")
    
    def _log_updated_record(self, lead_id: str, changes: Dict):
        """Log an update to CSV (leadId + changed fields, flattened)"""
        if self.updated_writer and self.updated_file:
            if self.updated_writer.fieldnames is None:
                # Dynamically build fieldnames
                flat_fields = ['leadId', 'sync_timestamp']
                for k in changes:
                    flat_fields += [f"{k}_old", f"{k}_new"]
                self.updated_writer.fieldnames = flat_fields
                self.updated_writer.writeheader()
            
            row = {'leadId': lead_id, 'sync_timestamp': datetime.now().isoformat()}
            for k, v in changes.items():
                if isinstance(v, dict) and 'old' in v and 'new' in v:
                    row[f"{k}_old"] = str(v['old']) if v['old'] is not None else ''
                    row[f"{k}_new"] = str(v['new']) if v['new'] is not None else ''
                else:
                    row[k] = str(v) if v is not None else ''
            
            self.updated_writer.writerow(row)
            self.updated_file.flush()
    
    def _validate_config(self):
        """Validate configs (copied from migration)"""
        required = ['host', 'user', 'password', 'database']
        for key in required:
            if not self.staging_config.get(key) or not self.destination_config.get(key):
                raise ValueError(f"Missing config: {key}")
        logger.info(f"Staging: {self.staging_table} | Dest: {self.destination_table}")
    
    def _check_tables(self):
        """Basic table checks"""
        try:
            dest_conn = self.connect_destination()
            with dest_conn.cursor() as cursor:
                cursor.execute(f"DESCRIBE {self.destination_table}")
                rows = cursor.fetchall()
                if not any(row['Field'] == 'updatedAt' for row in rows):
                    raise ValueError("Destination lacks 'updatedAt' column")
                cursor.execute(f"SHOW INDEX FROM {self.destination_table}")
                indexes = cursor.fetchall()
                if not any(idx['Column_name'] == 'leadId' for idx in indexes):
                    logger.warning("leadId not indexed on destination—consider adding for perf")
            dest_conn.close()
            logger.info("[OK] Tables validated")
        except Exception as e:
            raise ValueError(f"Table issue: {str(e)}")
    
    def connect_staging(self) -> pymysql.Connection:
        """Staging connection (copied)"""
        config = {k: v for k, v in self.staging_config.items() if k != 'table_name'}
        config.update({
            'charset': 'utf8mb4', 'cursorclass': DictCursor, 'connect_timeout': 30,
            'read_timeout': 600, 'write_timeout': 600, 'autocommit': False,
            'init_command': "SET SESSION sql_mode='NO_ENGINE_SUBSTITUTION'"
        })
        return pymysql.connect(**config)
    
    def connect_destination(self) -> pymysql.Connection:
        """Destination connection (copied)"""
        config = {k: v for k, v in self.destination_config.items() if k != 'table_name'}
        config.update({
            'charset': 'utf8mb4', 'cursorclass': DictCursor, 'connect_timeout': 30,
            'read_timeout': 600, 'write_timeout': 600, 'autocommit': False
        })
        return pymysql.connect(**config)
    
    def parse_timestamp(self, value) -> Optional[datetime]:
        """Robust timestamp parser: Handles datetime obj, str (ISO/MySQL/Unix), int/float (Unix)"""
        if value is None:
            return None
        try:
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=timezone.utc)
                return value
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
            if isinstance(value, str):
                # Normalize space to T
                normalized = value.strip().replace(' ', 'T')
                # Handle Z to +00:00
                if normalized.endswith('Z'):
                    normalized = normalized[:-1] + '+00:00'
                # Try ISO parse
                dt = datetime.fromisoformat(normalized)
                # Ensure UTC if naive
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            raise ValueError(f"Unsupported type: {type(value)}")
        except Exception as e:
            logger.error(f"Failed to parse timestamp '{value}' (type: {type(value)}): {e}")
            raise ValueError(f"Cannot parse timestamp: {value}")
    
    def fetch_updated_candidates(self, conn: pymysql.Connection, watermark: str) -> List[Dict]:
        """Fetch batch of updated records from staging"""
        query = f"""
            SELECT * FROM {self.staging_table}
            WHERE updatedAt > %s
            ORDER BY updatedAt
            LIMIT %s
        """
        with conn.cursor() as cursor:
            cursor.execute(query, (watermark, self.batch_size))
            return cursor.fetchall()
    
    def get_dest_snapshots(self, conn: pymysql.Connection, lead_ids: List[str]) -> Dict[str, Dict]:
        """Bulk fetch current state from destination by leadIds"""
        if not lead_ids:
            return {}
        placeholders = ', '.join(['%s'] * len(lead_ids))
        query = f"SELECT * FROM {self.destination_table} WHERE leadId IN ({placeholders})"
        with conn.cursor() as cursor:
            cursor.execute(query, lead_ids)
            rows = cursor.fetchall()
            return {row['leadId']: row for row in rows}
    
    def compute_deltas(self, staging_leads: List[Dict], dest_snapshots: Dict) -> List[Tuple[Dict, Dict]]:
        """
        Compute updates: Compare staging vs dest.
        Returns list of (staging_lead, changes_dict) for records needing UPDATE.
        """
        deltas = []
        for lead in staging_leads:
            lead_id = lead['leadId']
            dest_lead = dest_snapshots.get(lead_id)
            if not dest_lead:
                logger.warning(f"Lead {lead_id} not found in dest—skipping (consider INSERT)")
                self.stats['skipped'] += 1
                continue
            
            # Parse timestamps
            try:
                staging_ts = self.parse_timestamp(lead['updatedAt'])
                dest_ts = self.parse_timestamp(dest_lead['updatedAt'])
            except ValueError as e:
                logger.error(f"Skipping lead {lead_id} due to timestamp parse error: {e}")
                self.stats['failed'] += 1
                continue
            
            if staging_ts is None or dest_ts is None or staging_ts <= dest_ts:
                self.stats['skipped'] += 1  # No change or older
                continue
            
            # Compute field diffs (exclude id, leadId, updatedAt; include key fields)
            exclude_fields = {'id', 'leadId', 'updatedAt', 'is_migrated'}
            changes = {}
            for field, value in lead.items():
                if field not in exclude_fields and value != dest_lead.get(field, None):
                    old_val = dest_lead.get(field)
                    changes[field] = {'old': old_val, 'new': value}
            
            if changes:
                # Always update updatedAt and is_migrated
                changes['updatedAt'] = {'old': dest_lead['updatedAt'], 'new': lead['updatedAt']}
                changes['is_migrated'] = {'old': dest_lead.get('is_migrated'), 'new': 1}
                deltas.append((lead, changes))
                self.stats['updated'] += 1
                logger.debug(f"Delta for {lead_id}: {len(changes)} fields changed")
            else:
                self.stats['no_change'] += 1
        
        return deltas
    
    def apply_updates_batch(self, conn: pymysql.Connection, deltas: List[Tuple[Dict, Dict]]) -> int:
        """Batch UPDATE via executemany (optimized: collect all, but per-record for safety)"""
        if not deltas:
            return 0
        
        updated_count = 0
        with conn.cursor() as cursor:
            if self.disable_fk_checks:
                cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            
            for staging_lead, changes in deltas:
                # Build SET clause: only the new values
                set_parts = []
                values = []
                for k, v in changes.items():
                    if isinstance(v, dict):
                        set_parts.append(f"{k} = %s")
                        values.append(v['new'])
                    else:
                        set_parts.append(f"{k} = %s")
                        values.append(v)
                set_clause = ', '.join(set_parts)
                query = f"UPDATE {self.destination_table} SET {set_clause} WHERE leadId = %s"
                
                # Append leadId
                values.append(staging_lead['leadId'])
                
                try:
                    cursor.execute(query, values)
                    if cursor.rowcount > 0:
                        updated_count += 1
                        self._log_updated_record(staging_lead['leadId'], changes)
                except Exception as e:
                    logger.error(f"Update failed for {staging_lead['leadId']}: {e}")
                    self.stats['failed'] += 1
                    raise  # Re-raise to rollback batch
            
            if self.disable_fk_checks:
                cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        
        return updated_count
    
    def process_batch(self, staging_conn: pymysql.Connection, dest_conn: pymysql.Connection):
        """Process one batch of candidates"""
        candidates = self.fetch_updated_candidates(staging_conn, self.last_sync_at)
        if not candidates:
            return False  # No more
        
        self.stats['total_fetched'] += len(candidates)
        lead_ids = [lead['leadId'] for lead in candidates]
        
        dest_snapshots = self.get_dest_snapshots(dest_conn, lead_ids)
        deltas = self.compute_deltas(candidates, dest_snapshots)
        
        if not deltas:
            logger.info(f"[SKIP] Batch of {len(candidates)} candidates - no deltas")
            # Still advance watermark
            max_ts = max((self.parse_timestamp(lead['updatedAt']) for lead in candidates if lead['updatedAt']), default=None)
            if max_ts:
                self.last_sync_at = max_ts.isoformat().replace('+00:00', 'Z') if max_ts.tzinfo else max_ts.isoformat() + 'Z'
            return True
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would update {len(deltas)} records")
        else:
            try:
                applied = self.apply_updates_batch(dest_conn, deltas)
                dest_conn.commit()
                logger.info(f"[OK] Applied {applied} updates")
                if applied != len(deltas):
                    logger.warning(f"Applied {applied} out of {len(deltas)} expected")
            except Exception as e:
                dest_conn.rollback()
                logger.error(f"Batch update failed: {e}")
                raise
        
        # Update watermark to max updatedAt in this batch
        max_ts = max((self.parse_timestamp(lead['updatedAt']) for lead in candidates if lead['updatedAt']), default=None)
        if max_ts:
            self.last_sync_at = max_ts.isoformat().replace('+00:00', 'Z') if max_ts.tzinfo else max_ts.isoformat() + 'Z'
        
        return True
    
    def run(self):
        """Execute the sync"""
        mode = "DRY RUN" if self.dry_run else "LIVE SYNC"
        logger.info("="*80)
        logger.info(f"Starting Lead Delta Sync - {mode}")
        logger.info(f"Batch Size: {self.batch_size} | Starting from: {self.last_sync_at}")
        logger.info("="*80)
        
        staging_conn = None
        dest_conn = None
        
        try:
            staging_conn = self.connect_staging()
            dest_conn = self.connect_destination()
            logger.info("[OK] Connections established")
            
            batch_num = 1
            start_time = datetime.now()
            has_more = True
            
            while has_more:
                batch_start = datetime.now()
                has_more = self.process_batch(staging_conn, dest_conn)
                
                if has_more:
                    self._save_checkpoint()
                    batch_duration = (datetime.now() - batch_start).total_seconds()
                    logger.info(f"Batch {batch_num} completed in {batch_duration:.2f}s")
                    batch_num += 1
            
            # Final summary
            total_duration = (datetime.now() - start_time).total_seconds()
            if total_duration > 0:
                rate = self.stats['total_fetched'] / total_duration
            else:
                rate = 0
            logger.info("\n" + "="*80)
            logger.info(f"SYNC COMPLETE - {mode}")
            logger.info("="*80)
            logger.info(f"Fetched: {self.stats['total_fetched']} | Updated: {self.stats['updated']}")
            logger.info(f"Skipped: {self.stats['skipped']} | No Change: {self.stats['no_change']}")
            logger.info(f"Failed: {self.stats['failed']}")
            logger.info(f"Duration: {total_duration/60:.2f} min | Rate: {rate:.1f}/sec")
            logger.info("="*80)
            
            if self.stats['updated'] > 0:
                logger.info(f"Updated records logged to: {self.updated_csv_path}")
            
            # Cleanup checkpoint on full success
            if self.stats['failed'] == 0 and Path(self.checkpoint_file).exists():
                Path(self.checkpoint_file).unlink()
                logger.info("Checkpoint removed (sync complete)")
            
        except KeyboardInterrupt:
            logger.warning("\n[INTERRUPTED] Sync stopped")
            self._save_checkpoint()
            sys.exit(1)
        except Exception as e:
            logger.error(f"\nSync failed: {str(e)}", exc_info=True)
            sys.exit(1)
        finally:
            if staging_conn: staging_conn.close()
            if dest_conn: dest_conn.close()
            if self.updated_file: self.updated_file.close()
            logger.info("Connections closed")


def main():
    parser = argparse.ArgumentParser(
        description='Incremental sync of lead updates from staging to destination',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # First run: Dry-run with cutoff (migration end time)
  python lead_delta_sync.py --dry-run --initial-cutoff '2025-11-05T20:00:00Z' --batch-size 1000

  # Live sync (resumes if checkpoint exists)
  python lead_delta_sync.py --batch-size 2000

  # Resume interrupted sync
  python lead_delta_sync.py --resume --batch-size 2000
        """
    )
    
    parser.add_argument('--dry-run', action='store_true', help='Preview without updating')
    parser.add_argument('--batch-size', type=int, default=1000, help='Records per batch')
    parser.add_argument('--initial-cutoff', type=str, help='ISO datetime for first run watermark')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    parser.add_argument('--checkpoint-file', default='delta_sync_checkpoint.json', help='Checkpoint path')
    parser.add_argument('--staging-table', help='Override staging table')
    parser.add_argument('--destination-table', help='Override destination table')
    parser.add_argument('--debug', action='store_true', help='Debug logging')
    parser.add_argument('--disable-fk-checks', action='store_true', help='Disable FK checks')
    
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
    
    if not staging_config['host']:
        logger.error("Missing DB host in .env")
        sys.exit(1)
    
    # Run
    sync = LeadDeltaSync(
        staging_config=staging_config,
        destination_config=destination_config,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        initial_cutoff=args.initial_cutoff,
        checkpoint_file=args.checkpoint_file,
        resume=args.resume,
        disable_fk_checks=args.disable_fk_checks
    )
    
    sync.run()


if __name__ == '__main__':
    main()