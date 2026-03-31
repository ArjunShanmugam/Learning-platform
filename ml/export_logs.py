#!/usr/bin/env python3
"""
ml/export_logs.py

Usage (from project root or any dir):
    # default: export all tables to ml/data in csv+parquet
    python ml/export_logs.py

    # specify formats and output dir
    python ml/export_logs.py --out-dir ml/data --formats csv parquet --since-days 30

Notes:
- Run this using your backend venv so it picks up installed packages and backend/.env.
- Installs required: pandas, sqlalchemy, pymysql, python-dotenv. For parquet also pyarrow.
"""
import os
import argparse
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Defaults
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUT = SCRIPT_DIR / "data"

# Attempt to load backend/.env (common layout)
ENV_PATHS = [
    Path("backend") / ".env",
    Path(".env"),
    Path("../backend") / ".env",
]

def find_and_load_env():
    for p in ENV_PATHS:
        if p.exists():
            load_dotenv(dotenv_path=str(p))
            return p
    return None

def make_engine_from_env():
    env = find_and_load_env()
    if env is None:
        raise RuntimeError("Could not find backend/.env. Place .env in backend/.env or project root.")
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASS = os.getenv("DB_PASS", "pass123")
    DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
    DB_PORT = os.getenv("DB_PORT", "3306")
    DB_NAME = os.getenv("DB_NAME", "learning")
    uri = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(uri, pool_pre_ping=True)
    return engine

def export_table(engine, sql, out_dir: Path, base_name: str, formats=("csv", "parquet")):
    # Read into pandas
    print(f"Exporting {base_name} ...")
    df = pd.read_sql(sql, con=engine)
    out_files = []
    if "csv" in formats:
        csv_path = out_dir / f"{base_name}.csv"
        df.to_csv(csv_path, index=False)
        out_files.append(str(csv_path))
    if "parquet" in formats:
        try:
            pq_path = out_dir / f"{base_name}.parquet"
            df.to_parquet(pq_path, index=False)
            out_files.append(str(pq_path))
        except Exception as e:
            print(f"Warning: parquet export for {base_name} failed ({e}). Install pyarrow or fastparquet to enable parquet output.")
    print(f" -> exported {len(df)} rows to: {out_files}")
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT), help="Output directory (default: ml/data)")
    parser.add_argument("--formats", nargs="+", default=["csv", "parquet"], choices=["csv", "parquet"], help="Formats to export")
    parser.add_argument("--since-days", type=int, default=None, help="If set, only export logs newer than this many days (applies to logs tables)")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    formats = [f.lower() for f in args.formats]

    engine = make_engine_from_env()

    # build time filter for logs if requested
    time_filter = ""
    if args.since_days is not None:
        cutoff = datetime.utcnow() - timedelta(days=args.since_days)
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        time_filter = f" WHERE created_at >= '{cutoff_str}' "

    # Export core tables (full)
    export_table(engine, "SELECT id, email, role, created_at FROM users", out_dir, "users", formats)
    export_table(engine, "SELECT id, user_id, role, skill_level, career_path FROM user_profiles", out_dir, "user_profiles", formats)
    # Logs: apply time filter if present
    search_sql = "SELECT id, user_id, query, created_at FROM search_logs" + (time_filter if args.since_days is not None else "")
    click_sql = "SELECT id, user_id, course_id, event, created_at FROM click_logs" + (time_filter if args.since_days is not None else "")
    completed_sql = "SELECT id, user_id, course_id, completed_at FROM completed_courses" + (time_filter if args.since_days is not None else "")
    inprogress_sql = "SELECT id, user_id, course_id, started_at, last_seen_at FROM in_progress" + (time_filter if args.since_days is not None else "")

    export_table(engine, search_sql, out_dir, "search_logs", formats)
    export_table(engine, click_sql, out_dir, "click_logs", formats)
    export_table(engine, completed_sql, out_dir, "completed_courses", formats)
    export_table(engine, inprogress_sql, out_dir, "in_progress", formats)

    print("Export completed. Files are under:", out_dir)

if __name__ == "__main__":
    main()
