#!/usr/bin/env python
"""
Database backup and data retention management script.
"""
import os
import subprocess
import logging
from datetime import datetime, timedelta
from pathlib import Path
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '3306')
DB_NAME = os.getenv('DB_NAME', 'learning')

BACKUP_DIR = Path('backups')
BACKUP_DIR.mkdir(exist_ok=True)

DATA_RETENTION_DAYS = int(os.getenv('DATA_RETENTION_DAYS', '90'))
LOG_RETENTION_DAYS = int(os.getenv('LOG_RETENTION_DAYS', '30'))


def backup_database():
    """
    Create a backup of the MySQL database.
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = BACKUP_DIR / f"backup_{DB_NAME}_{timestamp}.sql"
    
    try:
        logger.info(f"Starting database backup to {backup_file}")
        
        # MySQL dump command
        cmd = [
            'mysqldump',
            f'--user={DB_USER}',
            f'--password={DB_PASSWORD}',
            f'--host={DB_HOST}',
            f'--port={DB_PORT}',
            '--single-transaction',
            '--quick',
            '--lock-tables=false',
            DB_NAME
        ]
        
        with open(backup_file, 'w') as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)
        
        if result.returncode == 0:
            file_size = backup_file.stat().st_size / (1024 * 1024)  # MB
            logger.info(f"✓ Database backup completed: {backup_file} ({file_size:.2f} MB)")
            return True
        else:
            logger.error(f"✗ Backup failed: {result.stderr}")
            backup_file.unlink()
            return False
    
    except FileNotFoundError:
        logger.error("mysqldump not found. Make sure MySQL is installed.")
        return False
    except Exception as e:
        logger.error(f"Backup error: {str(e)}")
        if backup_file.exists():
            backup_file.unlink()
        return False


def backup_weaviate():
    """
    Create a backup of Weaviate vector database.
    """
    try:
        import weaviate
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f"weaviate_backup_{timestamp}"
        
        logger.info(f"Starting Weaviate backup: {backup_name}")
        
        # Connect to Weaviate
        client = weaviate.Client(os.getenv('WEAVIATE_URL', 'http://localhost:8080'))
        
        # Create backup
        result = client.backup.create(
            backup_name=backup_name,
            include_classes=['Course', 'Embedding']
        )
        
        if result:
            logger.info(f"✓ Weaviate backup completed: {backup_name}")
            return True
        else:
            logger.error("Weaviate backup failed")
            return False
    
    except Exception as e:
        logger.error(f"Weaviate backup error: {str(e)}")
        return False


def cleanup_old_backups(days: int = 30):
    """
    Delete backups older than specified days.
    """
    try:
        logger.info(f"Cleaning up backups older than {days} days")
        
        cutoff_time = datetime.now() - timedelta(days=days)
        deleted_count = 0
        
        for backup_file in BACKUP_DIR.glob('backup_*.sql'):
            if datetime.fromtimestamp(backup_file.stat().st_mtime) < cutoff_time:
                backup_file.unlink()
                deleted_count += 1
                logger.info(f"Deleted old backup: {backup_file}")
        
        logger.info(f"Cleanup completed: {deleted_count} backups deleted")
        return True
    
    except Exception as e:
        logger.error(f"Cleanup error: {str(e)}")
        return False


def cleanup_old_logs(days: int = LOG_RETENTION_DAYS):
    """
    Delete log files older than specified days.
    """
    try:
        logger.info(f"Cleaning up logs older than {days} days")
        
        cutoff_time = datetime.now() - timedelta(days=days)
        log_files = [
            'training_scheduler.log',
            'app.log',
            'error.log'
        ]
        
        deleted_count = 0
        
        for log_file in log_files:
            log_path = Path(log_file)
            if log_path.exists():
                if datetime.fromtimestamp(log_path.stat().st_mtime) < cutoff_time:
                    log_path.unlink()
                    deleted_count += 1
                    logger.info(f"Deleted old log: {log_file}")
        
        logger.info(f"Log cleanup completed: {deleted_count} logs deleted")
        return True
    
    except Exception as e:
        logger.error(f"Log cleanup error: {str(e)}")
        return False


def cleanup_old_data(days: int = DATA_RETENTION_DAYS):
    """
    Delete old interaction and log data from database.
    """
    try:
        from sqlalchemy import create_engine, text
        
        logger.info(f"Cleaning up data older than {days} days")
        
        # Create database connection
        connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            # Delete old click logs
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            result = conn.execute(text(
                f"DELETE FROM click_logs WHERE timestamp < '{cutoff_date}'"
            ))
            click_logs_deleted = result.rowcount
            logger.info(f"Deleted {click_logs_deleted} old click logs")
            
            # Delete old search logs
            result = conn.execute(text(
                f"DELETE FROM search_logs WHERE timestamp < '{cutoff_date}'"
            ))
            search_logs_deleted = result.rowcount
            logger.info(f"Deleted {search_logs_deleted} old search logs")
            
            conn.commit()
        
        logger.info(f"Data cleanup completed: {click_logs_deleted + search_logs_deleted} records deleted")
        return True
    
    except Exception as e:
        logger.error(f"Data cleanup error: {str(e)}")
        return False


def generate_backup_report():
    """
    Generate a report of all backups.
    """
    try:
        report = {
            "timestamp": datetime.now().isoformat(),
            "backups": []
        }
        
        for backup_file in sorted(BACKUP_DIR.glob('backup_*.sql'), reverse=True):
            stat = backup_file.stat()
            report["backups"].append({
                "filename": backup_file.name,
                "size_mb": stat.st_size / (1024 * 1024),
                "created": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "path": str(backup_file)
            })
        
        report_file = BACKUP_DIR / f"backup_report_{datetime.now().strftime('%Y%m%d')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Backup report generated: {report_file}")
        return report
    
    except Exception as e:
        logger.error(f"Report generation error: {str(e)}")
        return None


def restore_database(backup_file: str):
    """
    Restore database from a backup file.
    """
    try:
        backup_path = BACKUP_DIR / backup_file
        
        if not backup_path.exists():
            logger.error(f"Backup file not found: {backup_file}")
            return False
        
        logger.info(f"Starting database restore from {backup_file}")
        
        # MySQL restore command
        cmd = [
            'mysql',
            f'--user={DB_USER}',
            f'--password={DB_PASSWORD}',
            f'--host={DB_HOST}',
            f'--port={DB_PORT}',
            DB_NAME
        ]
        
        with open(backup_path, 'r') as f:
            result = subprocess.run(cmd, stdin=f, stderr=subprocess.PIPE, text=True)
        
        if result.returncode == 0:
            logger.info(f"✓ Database restore completed from {backup_file}")
            return True
        else:
            logger.error(f"✗ Restore failed: {result.stderr}")
            return False
    
    except Exception as e:
        logger.error(f"Restore error: {str(e)}")
        return False


def main():
    """
    Main backup and retention management routine.
    """
    logger.info("=" * 60)
    logger.info("Starting Database Backup & Data Retention Management")
    logger.info("=" * 60)
    
    # Backup database
    logger.info("\n[1] Creating database backup...")
    backup_success = backup_database()
    
    # Backup Weaviate
    logger.info("\n[2] Creating Weaviate backup...")
    weaviate_success = backup_weaviate()
    
    # Cleanup old backups
    logger.info("\n[3] Cleaning up old backups...")
    cleanup_old_backups(days=30)
    
    # Cleanup old logs
    logger.info("\n[4] Cleaning up old logs...")
    cleanup_old_logs(days=LOG_RETENTION_DAYS)
    
    # Cleanup old data
    logger.info("\n[5] Cleaning up old data...")
    cleanup_old_data(days=DATA_RETENTION_DAYS)
    
    # Generate report
    logger.info("\n[6] Generating backup report...")
    report = generate_backup_report()
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("BACKUP & RETENTION MANAGEMENT COMPLETED")
    logger.info("=" * 60)
    if backup_success:
        logger.info("✓ Database backup: SUCCESS")
    else:
        logger.info("✗ Database backup: FAILED")
    if report:
        logger.info(f"✓ Total backups: {len(report['backups'])}")
    
    return backup_success and weaviate_success


if __name__ == "__main__":
    main()
