
import os
import pandas as pd
import logging

# Set input and output directories
input_directory = './input/'
output_directory = './output/'

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_file_from_directory(input_directory):
    """ Load a single CSV file from the directory, handling errors """
    abs_path = os.path.abspath(input_directory)
    logging.info(f"Looking for files in: {abs_path}")
    
    if not os.path.exists(input_directory):
        logging.error(f"Directory not found: {abs_path}")
        exit(1)
    
    files = [f for f in os.listdir(input_directory) if f.endswith('.csv')]
    
    if len(files) == 0:
        logging.error("No CSV files found in the directory.")
        exit(1)
    elif len(files) > 1:
        logging.warning("Multiple CSV files found. Selecting the first one.")
    
    file_to_load = os.path.join(input_directory, files[0])
    logging.info(f"Loading file: {file_to_load}")
    return pd.read_csv(file_to_load, low_memory=False)

def ensure_required_columns(df, required_columns):
    """ Ensure that required columns exist in the DataFrame """
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logging.warning(f"Missing columns detected: {missing_columns}. Adding them with default values.")
        for col in missing_columns:
            df[col] = None  # Use NaN instead of empty string
    return df

def clean_barcode_column(df):
    """ Ensure the barcode column is properly formatted without losing data """
    duplicates = None
    if 'variant.barcode' in df.columns:
        df['variant.barcode'] = pd.to_numeric(df['variant.barcode'], errors='ignore')
        duplicates = df[df['variant.barcode'].duplicated(keep=False)].copy()
        df = df[~df['variant.barcode'].duplicated(keep=False)]
    return df, duplicates

def clear_input_directory(input_directory):
    """ Remove all files in the input directory """
    for file in os.listdir(input_directory):
        file_path = os.path.join(input_directory, file)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                logging.info(f"Deleted file: {file_path}")
        except Exception as e:
            logging.error(f"Error deleting file {file_path}: {e}")

def main():
    logging.info("Starting data processing")
    
    # Load the CSV file
    df = load_file_from_directory(input_directory)
    
    # Define required columns
    required_columns = ['variant.sku', 'variant.product_id', 'variant.barcode', 'variant.price']
    df = ensure_required_columns(df, required_columns)
    df, duplicate_barcodes = clean_barcode_column(df)
    
    # Save duplicates if any found
    if duplicate_barcodes is not None and not duplicate_barcodes.empty:
        error_file = os.path.join(output_directory, 'duplicate_barcodes.csv')
        duplicate_barcodes.to_csv(error_file, index=False)
        logging.warning(f"Duplicate barcodes saved to {error_file}")
    
    # Ensure output directory exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    output_file = os.path.join(output_directory, 'processed_data.csv')
    df.to_csv(output_file, index=False)
    
    logging.info(f"Successfully processed the data! The output is saved to {output_file}")
    
    # Clear input directory and move processed file back into input
    clear_input_directory(input_directory)
    new_input_file = os.path.join(input_directory, 'processed_data.csv')
    os.rename(output_file, new_input_file)
    logging.info(f"Processed data moved back to input directory: {new_input_file}")

if __name__ == "__main__":
    main()
