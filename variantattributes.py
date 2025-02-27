
import os
import pandas as pd
import logging

# Set input and output directories
input_directory = './input/'
output_directory = './output/'

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Check if input directory exists and fetch file
def load_file_from_directory(input_directory):
    files = os.listdir(input_directory)
    if len(files) != 1:
        logging.error("There are no files or more than one file in the directory.")
        exit()
    return pd.read_csv(os.path.join(input_directory, files[0]), low_memory=False)

# Remove rows where 'variant.name' contains 'Sample product'
def filter_sample_product(df):
    logging.info("Filtering rows where 'variant.name' contains 'Sample product'")
    return df[~df['variant.name'].str.contains('Sample product', na=False)]

# Clean NaN or empty string 'variant.sku' and duplicates in 'variant.sku' and 'variant.barcode'
def clean_sku_and_barcode(df):
    logging.info("Cleaning 'variant.sku' column")
    df = df[df['variant.sku'].notna() & (df['variant.sku'] != '')]  # Drop empty 'variant.sku'
    duplicated_skus = df['variant.sku'].dropna()[df['variant.sku'].duplicated(keep=False)]
    if not duplicated_skus.empty:
        logging.error("Duplicates found in 'variant.sku'. Please contact management!")
        logging.error(f"Duplicated 'variant.sku' values: {duplicated_skus.unique()}")
        exit(1)
    return df

def select_required_columns(df):
    logging.info("Selecting required columns")

    # Strip any leading/trailing whitespaces from column names
    df.columns = df.columns.str.strip()

    #Get location of variant.package_height
    start_index = df.columns.get_loc('variant.package_height')  # Get the index of 'variant.weight' column
    
   

    # Add 'variant.sku', 'brand', 'description', 'id', and 'variant.product_id' to the columns to be selected
    columns_to_keep = ['variant.sku', 'variant.weight'] + list(df.columns[start_index:])

    # Select the columns
    df = df[columns_to_keep]

    # Only drop columns if they exist in the DataFrame
    columns_to_drop = ['variant.target_listing_action']
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    if existing_columns_to_drop:
        df = df.drop(columns=existing_columns_to_drop)

    # Remove columns with all NaN or empty values
    df = df.dropna(axis=1, how='all')  # Drop columns with all NaN values
    df = df.loc[:, df.notna().any(axis=0)]  # Keep columns with at least one non-NaN value


    return df

def rename_columns(df):
    # Rename 'variant.sku' to 'sku'
    df = df.rename(columns={'variant.sku': 'sku'})
    
    # Rename 'variant.' columns to 'fields.' and add '.value' to specific columns like weight, height, width, length
    df.columns = [
        'fields.' + col[8:] + '.value' if col.startswith('variant.') and col in ['variant.weight', 'variant.height', 'variant.width', 'variant.length'] else 
        'fields.' + col[8:] if col.startswith('variant.') else col
        for col in df.columns
    ]
    
    return df

# Main function to run all steps
def main():
    logging.info("Starting data processing")

    # Load the CSV file
    df = load_file_from_directory(input_directory)
    
    # Apply transformations in sequence
    df = filter_sample_product(df)
    df = clean_sku_and_barcode(df)
    df = select_required_columns(df)
    df = rename_columns(df)

    # Ensure output directory exists and save the cleaned data
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    output_file = os.path.join(output_directory, 'variantattributes.csv')
    df.to_csv(output_file, index=False)
    
    logging.info(f"Successfully filtered the data! The filtered data is saved to {output_file}")

# Run the main function
if __name__ == "__main__":
    main()
