"""
Data cleaning script for interaction data.
"""
import os
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

def clean_interaction_data(input_path: str, output_path: str) -> None:
    """Clean and preprocess interaction data with proper encoding and path handling."""
    try:
        # Convert to Path objects and ensure they are absolute
        input_path = Path(input_path).absolute()
        output_path = Path(output_path).absolute()
        
        print(f"Looking for file at: {input_path}")
        print(f"File exists: {input_path.exists()}")
        
        # Ensure input file exists
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
            
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Read raw data with proper encoding for BOM
        print(f"Reading data from: {input_path}")
        df = pd.read_csv(input_path, encoding='utf-8-sig')
        
        # Basic data validation
        if df.empty:
            raise ValueError("Input file is empty")
            
        # Add cleaning steps here
        print(f"Processing {len(df)} records")
        
        # Example: Convert timestamps
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        
        # Ensure required columns exist
        required_columns = ['user_id', 'item_id', 'interaction_type']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
            
        # Basic data cleaning
        df = df.drop_duplicates()
        df = df.dropna(subset=['user_id', 'item_id', 'interaction_type'])
        
        # Convert data types
        df['user_id'] = df['user_id'].astype(int)
        df['item_id'] = df['item_id'].astype(int)
        
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        
        # Save cleaned data
        print(f"Saving cleaned data to: {output_path}")
        df.to_parquet(output_path, index=False)
        print(f"✅ Successfully processed {len(df)} records")
        print(f"✅ Cleaned data saved to: {output_path}")
        
    except Exception as e:
        print(f"❌ Error processing data: {str(e)}")
        raise
def main():
    """Main function to run the data cleaning process."""
    try:
        # Get the current working directory
        cwd = Path.cwd()
        print(f"Current working directory: {cwd}")
        
        # Try to find the data directory relative to the current working directory
        data_dir = cwd / 'data'
        if not data_dir.exists():
            # Try one level up (in case we're in backend directory)
            data_dir = cwd.parent / 'data'
            if not data_dir.exists():
                # Try the project root
                data_dir = cwd / 'backend' / 'data'
                if not data_dir.exists():
                    raise FileNotFoundError("Could not find data directory. Please run from project root or backend directory.")
        
        raw_file = data_dir / 'raw' / 'interactions.csv'
        output_file = data_dir / 'processed' / 'interactions_cleaned.parquet'
        
        # Ensure directories exist
        raw_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"Using data directory: {data_dir}")
        
        print(f"Starting data cleaning process...")
        print(f"Project root: {project_root}")
        print(f"Input file: {raw_file}")
        print(f"Output file: {output_file}")
        
        # Run the cleaning process
        clean_interaction_data(raw_file, output_file)
        
    except Exception as e:
        print(f"❌ Fatal error in main: {str(e)}")
        return 1
    return 0

if __name__ == "__main__":
    exit(main())
