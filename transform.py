import json
import pandas as pd
import re
import os
from typing import Dict, Any, Tuple, Optional
from pathlib import Path

#Fixes the json file and gets rid of formatting issues
def fix_json_content(content: str) -> str:
    content = re.sub(r',\s*([}\]])', r'\1', content)
    content = content.strip()
    if not content.endswith('}'):
        content += '}'
    return content

#Flatten dict for easier processing
def flatten_nested_dict(d: Dict[Any, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_nested_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_nested_dict(item, f"{new_key}_{i}", sep=sep).items())
                else:
                    items.append((f"{new_key}_{i}", item))
        elif isinstance(v, list):
            for i, item in enumerate(v):
                items.append((f"{new_key}_{i}", item))
        else:
            items.append((new_key, v))
    return dict(items)

def create_flight_tables(file_path: str, output_dir: str, all_summary_rows: list) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """Create separate summary and info tables for flight data"""
    base_filename = os.path.splitext(os.path.basename(file_path))[0] #Get the filename
    info_output = os.path.join(output_dir, f"{base_filename}_info.csv") #Flight log info table
    
    # Check if info file already exists
    if os.path.exists(info_output):
        print(f"SKIPPED: {file_path} - Info file already exists: {info_output}")
        return None, pd.DataFrame()  # Return empty DataFrame to indicate skipped but successful
    
    print(f"Processing file: {file_path}")
    print(f"Info output: {info_output}") #Where output is saved
    
    with open(file_path, 'r') as file:
        content = file.read()
    content = fix_json_content(content)
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}")
        return None, None
    
    #Process data for summary table - append to global list ONLY if file is being processed
    if 'summary' in data:
        summary_data = data['summary']
        flattened_summary = flatten_nested_dict(summary_data, sep='_')
        summary_row = {
            'flight_id': base_filename,  # Primary key as discussed
        }
        for key, value in flattened_summary.items():
            summary_row[key] = value
        all_summary_rows.append(summary_row)
    
    #Process data for info table
    info_rows = []
    if 'info' in data and 'frameTimeStates' in data['info']:
        info_data = data['info']['frameTimeStates']
        start_time = data.get('summary', {}).get('startTime', 0)
        for frame in info_data:
            flight_time = frame.get('flightControllerState', {}).get('flightTimeInSeconds', 0)
            time_sum = start_time + flight_time #For defining the primary key
            composite_key = f"{base_filename}_{time_sum}"
            flattened_frame = flatten_nested_dict(frame, sep='_')
            info_row = {
                'primary_key': composite_key,  #Primary key
                'flight_id': base_filename,    #For partitioning to easily fetch logs for Grafana
                'startTime': start_time,     
                'flightTimeInSeconds': flight_time,  
                'timestamp': time_sum          #For time series analysis
            }
            for key, value in flattened_frame.items():
                info_row[key] = value
            info_rows.append(info_row)
    
    info_df = pd.DataFrame(info_rows) if info_rows else pd.DataFrame()

    if not info_df.empty:
        cols = info_df.columns.tolist()
        priority_cols = ['primary_key', 'flight_id', 'startTime', 'flightTimeInSeconds', 'timestamp']
        for col in reversed(priority_cols):
            if col in cols:
                cols.insert(0, cols.pop(cols.index(col)))
        info_df = info_df[cols]
        info_df = info_df.fillna('')
        for col in info_df.columns:
            if info_df[col].dtype == 'object':
                info_df[col] = info_df[col].astype('string')
        info_df.to_csv(info_output, index=False)
        print(f"Info table saved to {info_output}")
    
    return None, info_df  # Return None for summary_df since we're handling it globally

def process_all_flight_logs(input_dir: str = "parsed_logs", output_dir: str = "transformed_logs"):

    #Specify input and output directories for transformation
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    if not input_path.exists():
        print(f"ERROR: Input directory '{input_dir}' does not exist!")
        return
    output_path.mkdir(exist_ok=True)
    print(f"Output directory '{output_dir}' ready")
    
    #Fetch all .json files from input directory
    file_pattern = ['*.json']
    all_files = []
    for pattern in file_pattern:
        all_files.extend(input_path.glob(pattern))
    if not all_files:
        print(f"ERROR: No JSON files found in '{input_dir}'")
        return
    
    print(f"Found {len(all_files)} files to process:")
    for file in all_files:
        print(f"   - {file.name}")
    
    # Initialize global summary list
    all_summary_rows = []
    successful = 0
    failed = 0
    skipped = 0
    new_csv_files_created = 0  # Track newly created CSV files
    
    #All this part basically checks is whether the processing is done and whether it is error-free.
    print(f"\n{'='*80}")
    print("STARTING BATCH PROCESSING")
    print(f"{'='*80}")
    
    for file_path in all_files:
        print(f"\n{'-'*60}")
        try:
            summary_df, info_df = create_flight_tables(str(file_path), str(output_path), all_summary_rows)
            if info_df is not None:
                if len(info_df) == 0:  # Empty DataFrame means file was skipped
                    print(f"SKIPPED: {file_path.name}")
                    skipped += 1
                else:
                    print(f"SUCCESS: Processed {file_path.name}")
                    successful += 1
                    new_csv_files_created += 1  # Count new info CSV file
            else:
                print(f"FAILED: Could not process {file_path.name}")
                failed += 1
        except Exception as e:
            print(f"ERROR: Processing {file_path.name}: {e}")
            failed += 1
    
    # Append new summary data to existing summary.csv file
    summary_output = os.path.join(output_path, "summary.csv")
    summary_created = False
    
    if all_summary_rows:
        new_summary_df = pd.DataFrame(all_summary_rows)
        cols = new_summary_df.columns.tolist()
        if 'flight_id' in cols:
            cols.insert(0, cols.pop(cols.index('flight_id'))) #Set primary key as first column
            new_summary_df = new_summary_df[cols]
        new_summary_df = new_summary_df.fillna('')
        for col in new_summary_df.columns:
            if new_summary_df[col].dtype == 'object':
                new_summary_df[col] = new_summary_df[col].astype('string')
        
        # Check if summary.csv already exists
        if os.path.exists(summary_output):
            # Read existing summary data
            existing_summary_df = pd.read_csv(summary_output)
            
            # Remove duplicates based on flight_id (keep existing entries)
            existing_flight_ids = set(existing_summary_df['flight_id'].tolist())
            new_summary_df = new_summary_df[~new_summary_df['flight_id'].isin(existing_flight_ids)]
            
            if not new_summary_df.empty:
                # Append new data to existing file
                new_summary_df.to_csv(summary_output, mode='a', header=False, index=False)
                print(f"\nAppended {len(new_summary_df)} new records to existing summary.csv")
            else:
                print(f"\nNo new records to append - all flight_ids already exist in summary.csv")
        else:
            # Create new summary file
            new_summary_df.to_csv(summary_output, index=False)
            print(f"\nCreated new summary.csv with {len(new_summary_df)} flight records")
            summary_created = True
        
        # Read final summary for reporting
        final_summary_df = pd.read_csv(summary_output)
        print(f"Total records in summary.csv: {len(final_summary_df)}")
    
    # Count new CSV files created (info files + summary if created)
    if summary_created:
        new_csv_files_created += 1
    
    #Final summary
    print(f"\n{'='*80}")
    print("BATCH PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"Successfully processed: {successful} files")
    print(f"Skipped (already exists): {skipped} files")
    print(f"Failed to process: {failed} files")
    print(f"Total files processed: {successful + skipped + failed}")
    print(f"Output files saved in: {output_path}")
    print(f"New CSV files created in this run: {new_csv_files_created}")

#Execute the batch processing
if __name__ == "__main__":
    process_all_flight_logs()