import pandas as pd
import clickhouse_connect
from pathlib import Path
from typing import Dict
import warnings
warnings.filterwarnings("ignore")

def get_column_types_from_df(df: pd.DataFrame) -> Dict[str, str]:
    """Automatically detect and map pandas dtypes to ClickHouse types with compatibility"""
    type_mapping = {}
    for col in df.columns:
        dtype = df[col].dtype
        sample_values = df[col].dropna()
        if col in ['flight_id', 'primary_key'] or col.endswith('_id'):
            type_mapping[col] = 'String'
        elif col in ['startTime', 'endTime', 'timestamp']:
            type_mapping[col] = 'DateTime64(3)'
        elif col == 'flightTimeInSeconds':
            type_mapping[col] = 'Float64'
        elif 'time' in col.lower() and sample_values.dtype in ['int64', 'float64']:
            type_mapping[col] = 'UInt64'
        elif dtype == 'object' or dtype == 'string':
            type_mapping[col] = 'String'
        elif dtype == 'bool':
            type_mapping[col] = 'UInt8'
        elif dtype in ['int8', 'int16']:
            type_mapping[col] = 'Int16'
        elif dtype in ['int32', 'int64']:
            if not sample_values.empty and sample_values.min() >= 0 and sample_values.max() < 4294967295:
                type_mapping[col] = 'UInt32'
            else:
                type_mapping[col] = 'Int64'
        elif dtype in ['float32']:
            type_mapping[col] = 'Float32'
        elif dtype in ['float64']:
            type_mapping[col] = 'Float64'
        else:
            type_mapping[col] = 'String'
    return type_mapping

def create_database_and_tables(client, summary_df: pd.DataFrame, info_df: pd.DataFrame):
    """Create database and ClickHouse tables with proper data types"""
    print("Creating database autrik...")
    client.command("CREATE DATABASE IF NOT EXISTS autrik")
    print("Database \"autrik\" created/verified")
    summary_types = get_column_types_from_df(summary_df)
    summary_columns = [f"`{col}` {ch_type}" for col, ch_type in summary_types.items()]
    summary_ddl = f"""
    CREATE TABLE IF NOT EXISTS autrik.flight_summary (
        {', '.join(summary_columns)}
    ) ENGINE = MergeTree()
    ORDER BY flight_id
    """
    info_types = get_column_types_from_df(info_df)
    info_columns = [f"`{col}` {ch_type}" for col, ch_type in info_types.items()]
    info_ddl = f"""
    CREATE TABLE IF NOT EXISTS autrik.flight_info (
        {', '.join(info_columns)}
    ) ENGINE = MergeTree()
    ORDER BY (flight_id, timestamp)
    PARTITION BY flight_id
    """
    print("Creating flight_summary table...")
    client.command(summary_ddl)
    print("\"flight_summary\" table created")
    print("Creating flight_info table...")
    client.command(info_ddl)
    print("\"flight_info\" table created")
    return summary_types, info_types

def process_timestamp_columns(df: pd.DataFrame, timestamp_cols: list) -> pd.DataFrame:
    """Convert timestamp columns from milliseconds to datetime with millisecond precision"""
    df_copy = df.copy()
    for col in timestamp_cols:
        if col in df_copy.columns:
            try:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
                df_copy[col] = pd.to_datetime(df_copy[col], unit='s', errors='coerce')
                df_copy[col] = df_copy[col].dt.floor('ms')
            except Exception as e:
                print(f"Error converting column {col}: {e}")
                try:
                    df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
                except Exception as e2:
                    print(f"Fallback conversion failed for {col}: {e2}")
    return df_copy

def compute_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """Compute timestamp from startTime and flightTimeInSeconds"""
    if 'startTime' in df.columns and 'flightTimeInSeconds' in df.columns:
        # Ensure startTime is datetime
        if not pd.api.types.is_datetime64_any_dtype(df['startTime']):
            df['startTime'] = pd.to_datetime(df['startTime'], unit='s', errors='coerce')
        df['flightTimeInSeconds'] = pd.to_numeric(df['flightTimeInSeconds'], errors='coerce')
        df['timestamp'] = df['startTime'] + pd.to_timedelta(df['flightTimeInSeconds'], unit='s')
        df['timestamp'] = df['timestamp'].dt.floor('ms')
    return df

def load_and_prepare_data(csv_dir: str = "transformed_logs"):
    """Load all CSV files and prepare data for ClickHouse insertion"""
    csv_path = Path(csv_dir)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV directory '{csv_dir}' does not exist!")
    summary_file = csv_path / "summary.csv"
    if not summary_file.exists():
        raise FileNotFoundError(f"summary.csv not found in {csv_dir}")
    print(f"Loading summary data from {summary_file}")
    summary_df = pd.read_csv(summary_file)
    summary_df = process_timestamp_columns(summary_df, ['startTime', 'endTime'])
    print(f"✓ Loaded {len(summary_df)} summary records")
    info_files = list(csv_path.glob("*_info.csv"))
    if not info_files:
        raise FileNotFoundError(f"No *_info.csv files found in {csv_dir}")
    print(f"Found {len(info_files)} info files to process")
    all_info_dfs = []
    for info_file in info_files:
        print(f"Loading {info_file.name}...")
        info_df = pd.read_csv(info_file)
        info_df = process_timestamp_columns(info_df, ['startTime'])
        info_df = compute_timestamp(info_df)
        all_info_dfs.append(info_df)
    combined_info_df = pd.concat(all_info_dfs, ignore_index=True)
    print(f"Combined {len(combined_info_df)} total info records")
    return summary_df, combined_info_df, info_files

def insert_summary_if_new(client, summary_df: pd.DataFrame):
    """Insert only new summary records based on flight_id"""
    existing_ids = set()
    try:
        result = client.query("SELECT flight_id FROM autrik.flight_summary")
        existing_ids = set(row[0] for row in result.result_rows)
    except Exception:
        pass
    new_df = summary_df[~summary_df['flight_id'].astype(str).isin(existing_ids)]
    num_new = len(new_df)
    if num_new > 0:
        print(f"Inserting {num_new} new summary records...")
        client.insert_df(table='autrik.flight_summary', df=new_df)
        print(f"Inserted {num_new} summary records")
    else:
        print("No new summary records to insert.")
    return num_new

def insert_info_if_new(client, info_df: pd.DataFrame, info_file: Path):
    """Insert only new info records based on primary_key for each info file"""
    if 'primary_key' not in info_df.columns:
        print(f"SKIP: {info_file.name} has no primary_key column.")
        return 0
    flight_id = info_df['flight_id'].iloc[0] if 'flight_id' in info_df.columns and not info_df.empty else None
    if not flight_id:
        print(f"SKIP: {info_file.name} has no flight_id.")
        return 0
    existing_keys = set()
    try:
        result = client.query(f"SELECT primary_key FROM autrik.flight_info WHERE flight_id = %(flight_id)s", parameters={'flight_id': flight_id})
        existing_keys = set(row[0] for row in result.result_rows)
    except Exception:
        pass
    new_df = info_df[~info_df['primary_key'].astype(str).isin(existing_keys)]
    num_new = len(new_df)
    if num_new > 0:
        for col in new_df.columns:
            if new_df[col].dtype == 'object' or new_df[col].dtype.name == 'string':
                new_df[col] = new_df[col].astype(str).fillna('')
        print(f"Inserting {num_new} new info records for {flight_id}...")
        batch_size = 50000
        for i in range(0, num_new, batch_size):
            batch = new_df.iloc[i:i+batch_size]
            client.insert_df(table='autrik.flight_info', df=batch)
        print(f"Inserted {num_new} info records for {flight_id}")
    else:
        print(f"No new info records to insert for {flight_id}.")
    return num_new

def main(csv_dir: str = "transformed_logs",
         host: str = 'localhost',
         port: int = 8123,   # Change clickhouse user credentials here
         username: str = 'default',
         password: str = 'CHANGEME'):
    print("="*80)
    print("CLICKHOUSE DATA LOADER - FLIGHT DATA")
    print("="*80)
    print(f"Connecting to ClickHouse at {host}:{port}...")
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        compression=None
    )
    print("Connected to ClickHouse")
    try:
        print("\n" + "-"*60)
        print("LOADING DATA")
        print("-"*60)
        summary_df, combined_info_df, info_files = load_and_prepare_data(csv_dir)
        print("\n" + "-"*60)
        print("CREATING DATABASE AND TABLES")
        print("-"*60)
        create_database_and_tables(client, summary_df, combined_info_df)
        print("\n" + "-"*60)
        print("INSERTING SUMMARY DATA")
        print("-"*60)
        num_new_summary = insert_summary_if_new(client, summary_df)
        print("\n" + "-"*60)
        print("INSERTING INFO DATA")
        print("-"*60)
        total_new_info = 0
        for info_file in info_files:
            info_df = pd.read_csv(info_file)
            info_df = process_timestamp_columns(info_df, ['startTime'])
            info_df = compute_timestamp(info_df) # Compute timestamp here
            total_new_info += insert_info_if_new(client, info_df, info_file)
        print(f"\nTotal new summary records inserted: {num_new_summary}")
        print(f"Total new info records inserted: {total_new_info}")
        print("\n" + "-"*60)
        print("VERIFICATION")
        print("-"*60)
        summary_count = client.command('SELECT count() FROM autrik.flight_summary')
        info_count = client.command('SELECT count() FROM autrik.flight_info')
        partitions = client.command('SELECT count(DISTINCT flight_id) FROM autrik.flight_info')
        print(f"Summary table: {summary_count} records")
        print(f"Info table: {info_count} records")
        print(f"Info table partitions: {partitions}")
        print("\n" + "="*80)
        print("DATA LOADING COMPLETED SUCCESSFULLY!")
        print("="*80)
    except Exception as e:
        print(f"\n ERROR: {e}")
        raise
    finally:
        client.close()

if __name__ == "__main__":
    main()