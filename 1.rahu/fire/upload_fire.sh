import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime


class ParquetProcessor:
    def __init__(self, config_file="config.json"):
        self.config = self._load_config(config_file)
        self._setup_directories()
        self.schema = self._define_schema()

    def _load_config(self, config_file):
        with open(config_file, 'r') as f:
            return json.load(f)

    def _setup_directories(self):
        preprocessed_dir = self.config["directories"]["preprocessed_data"]
        Path(preprocessed_dir).mkdir(parents=True, exist_ok=True)

    def _define_schema(self):
        return pa.schema([
            ('latitude', pa.float64()),
            ('longitude', pa.float64()),
            ('brightness', pa.float64()),
            ('scan', pa.float64()),
            ('track', pa.float64()),
            ('acq_date', pa.string()),
            ('acq_time', pa.string()),
            ('satellite', pa.string()),
            ('instrument', pa.string()),
            ('confidence', pa.int64()),
            ('version', pa.string()),
            ('bright_t31', pa.float64()),
            ('frp', pa.float64()),
            ('daynight', pa.string()),
            ('type', pa.int64()),
            ('year', pa.int64())
        ])

    def _clean_dataframe(self, df):
        initial_rows = len(df)
        
        df = df.dropna()
        
        df = df[df != 'NA'].dropna()
        
        null_mask = df.isin(['null', 'NULL', 'Null', ''])
        df = df[~null_mask.any(axis=1)]
        
        cleaned_rows = len(df)
        print(f"Cleaned data: {initial_rows} -> {cleaned_rows} rows ({initial_rows - cleaned_rows} removed)")
        
        return df

    def _extract_year_from_date(self, df):
        df['acq_date'] = pd.to_datetime(df['acq_date'], errors='coerce')
        df['year'] = df['acq_date'].dt.year
        df['acq_date'] = df['acq_date'].dt.strftime('%Y-%m-%d')
        return df

    def _convert_data_types(self, df):
        type_mapping = {
            'latitude': 'float64',
            'longitude': 'float64',
            'brightness': 'float64',
            'scan': 'float64',
            'track': 'float64',
            'confidence': 'int64',
            'bright_t31': 'float64',
            'frp': 'float64',
            'type': 'int64',
            'year': 'int64'
        }
        
        for col, dtype in type_mapping.items():
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if dtype == 'int64':
                    df[col] = df[col].astype('Int64')
        
        string_cols = ['satellite', 'instrument', 'version', 'daynight', 'acq_date', 'acq_time']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype('string')
        
        return df

    def process_csv_file(self, csv_path):
        try:
            print(f"Processing {csv_path}...")
            
            df = pd.read_csv(csv_path, sep='\t')
            
            df = self._clean_dataframe(df)
            
            if df.empty:
                print(f"Warning: No data remaining after cleaning {csv_path}")
                return None
            
            df = self._extract_year_from_date(df)
            df = self._convert_data_types(df)
            
            df = df.dropna()
            
            return df
            
        except Exception as e:
            print(f"Error processing {csv_path}: {e}")
            return None

    def combine_and_save_parquet(self):
        raw_data_path = self.config["directories"]["fire_data"]
        preprocessed_path = self.config["directories"]["preprocessed_data"]
        
        all_dataframes = []
        
        for root, dirs, files in os.walk(raw_data_path):
            for file in files:
                if file.endswith('.csv'):
                    csv_path = os.path.join(root, file)
                    df = self.process_csv_file(csv_path)
                    if df is not None and not df.empty:
                        all_dataframes.append(df)
        
        if not all_dataframes:
            print("No valid data found to process")
            return
        
        print("Combining all data...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        combined_df = combined_df.sort_values(['year', 'acq_date', 'acq_time'])
        
        output_path = os.path.join(preprocessed_path, 'fire_data_combined.parquet')
        
        print(f"Saving combined data to {output_path}...")
        table = pa.Table.from_pandas(combined_df, schema=self.schema)
        pq.write_table(table, output_path, compression='snappy')
        
        print(f"Successfully saved {len(combined_df)} records to {output_path}")
        
        self._print_summary(combined_df)

    def process_year_parquet(self, year):
        raw_data_path = self.config["directories"]["fire_data"]
        preprocessed_path = self.config["directories"]["preprocessed_data"]
        
        year_dir = os.path.join(raw_data_path, str(year))
        if not os.path.exists(year_dir):
            print(f"No data found for year {year}")
            return
        
        year_dataframes = []
        
        for file in os.listdir(year_dir):
            if file.endswith('.csv'):
                csv_path = os.path.join(year_dir, file)
                df = self.process_csv_file(csv_path)
                if df is not None and not df.empty:
                    year_dataframes.append(df)
        
        if not year_dataframes:
            print(f"No valid data found for year {year}")
            return
        
        combined_df = pd.concat(year_dataframes, ignore_index=True)
        combined_df = combined_df.sort_values(['acq_date', 'acq_time'])
        
        output_path = os.path.join(preprocessed_path, f'fire_data_{year}.parquet')
        
        print(f"Saving {year} data to {output_path}...")
        table = pa.Table.from_pandas(combined_df, schema=self.schema)
        pq.write_table(table, output_path, compression='snappy')
        
        print(f"Successfully saved {len(combined_df)} records for year {year}")

    def _print_summary(self, df):
        print("\n=== Data Summary ===")
        print(f"Total records: {len(df)}")
        print(f"Year range: {df['year'].min()} - {df['year'].max()}")
        print(f"Countries: {df['satellite'].nunique()} unique satellites")
        print(f"Date range: {df['acq_date'].min()} to {df['acq_date'].max()}")
        print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        print("\nRecords per year:")
        year_counts = df['year'].value_counts().sort_index()
        for year, count in year_counts.items():
            print(f"  {year}: {count:,} records")

    def verify_parquet_file(self, parquet_path):
        try:
            table = pq.read_table(parquet_path)
            df = table.to_pandas()
            
            print(f"\n=== Verification of {parquet_path} ===")
            print(f"Schema: {table.schema}")
            print(f"Shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"Sample data:")
            print(df.head())
            
            return True
            
        except Exception as e:
            print(f"Error reading parquet file: {e}")
            return False


def main():
    processor = ParquetProcessor()
    
    print("Fire Data Parquet Processor")
    print("1. Process all years into single parquet file")
    print("2. Process specific year")
    print("3. Verify existing parquet file")
    
    choice = input("Enter choice (1-3): ").strip()
    
    if choice == '1':
        processor.combine_and_save_parquet()
        
    elif choice == '2':
        year = input("Enter year: ").strip()
        try:
            processor.process_year_parquet(int(year))
        except ValueError:
            print("Invalid year format")
            
    elif choice == '3':
        file_path = input("Enter parquet file path: ").strip()
        processor.verify_parquet_file(file_path)
        
    else:
        print("Invalid choice")


if __name__ == "__main__":
    main()
