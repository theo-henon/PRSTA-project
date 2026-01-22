import pandas as pd
import re


def read_rte_file(file_path):
    """
    Read the RTE text file and combine all data blocks into a single dataframe.
    Each block contains data for a specific date with columns: Heures, PrévisionJ-1, PrévisionJ, Consommation
    
    Args:
        file_path: Path to the text file (with .xls extension)
        
    Returns:
        pd.DataFrame: Combined dataframe with all blocks and a 'date' column
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    all_dfs = []
    current_date = None
    current_block_lines = []
    in_data_block = False
    
    for line in lines:
        line = line.strip()
        
        if not line:
            continue
        
        # Check if this is a date header row (Journ­e du DD/MM/YYYY)
        if "Journ" in line and "/" in line:
            # Process previous block if exists
            if current_block_lines and current_date is not None:
                df_block = process_block(current_block_lines, current_date)
                if df_block is not None:
                    all_dfs.append(df_block)
            
            # Extract date from header
            match = re.search(r'(\d{2})/(\d{2})/(\d{4})', line)
            if match:
                day, month, year = match.groups()
                date_str = f"{year}-{month}-{day}"
                current_date = pd.to_datetime(date_str)
                
                # Reset for new block
                current_block_lines = []
                in_data_block = False
        
        elif line.startswith("Heures"):
            # This is the header row for the data
            in_data_block = True
            current_block_lines = []
        
        elif in_data_block and current_date is not None:
            # This is data after we've seen the header
            current_block_lines.append(line)
    
    # Don't forget the last block
    if current_block_lines and current_date is not None:
        df_block = process_block(current_block_lines, current_date)
        if df_block is not None:
            all_dfs.append(df_block)
    
    # Combine all blocks
    if all_dfs:
        result_df = pd.concat(all_dfs, ignore_index=True)
        # Reorder columns: date first, then the data columns
        cols = result_df.columns.tolist()
        if 'date' in cols:
            cols.remove('date')
        result_df = result_df[['date'] + cols]
        return result_df
    
    return pd.DataFrame()


def process_block(block_lines, current_date):
    """
    Process a single block of data lines.
    
    Args:
        block_lines: List of tab-separated data lines
        current_date: The date for this block
        
    Returns:
        pd.DataFrame or None: DataFrame for this block
    """
    if not block_lines:
        return None
    
    data_rows = []
    for line in block_lines:
        parts = line.split('\t')
        if len(parts) >= 4:
            try:
                heures = parts[0].strip()
                previsionj1 = float(parts[1].strip())
                previsionj = float(parts[2].strip())
                consommation = float(parts[3].strip())
                data_rows.append([heures, previsionj1, previsionj, consommation])
            except (ValueError, IndexError):
                # Skip rows that can't be parsed as numbers
                continue
    
    if not data_rows:
        return None
    
    # Create dataframe
    df = pd.DataFrame(data_rows, columns=['Heures', 'PrévisionJ-1', 'PrévisionJ', 'Consommation'])
    df['date'] = current_date
    
    return df


def main():
    print("Reading RTE consumption data...")
    
    file_path = "./data/rte/conso_mix_RTE_2025.xls"
    
    try:
        df = read_rte_file(file_path)
        
        print(f"\nDataframe shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        print(f"\nDate range: {df['date'].min()} to {df['date'].max()}")
        print(f"\nFirst few rows:")
        print(df.head(10))
        print(f"\nLast few rows:")
        print(df.tail(10))
        print(f"\nData types:")
        print(df.dtypes)
        
        # Optionally save to CSV
        output_path = "./data/rte/conso_mix_consolidated.csv"
        df.to_csv(output_path, index=False)
        print(f"\nData saved to {output_path}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
