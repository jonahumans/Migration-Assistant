
import os
import pandas as pd
import logging

# Set input and output directories
input_directory = './input/'
output_directory = './output/'

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_file_from_directory(input_directory):
    files = os.listdir(input_directory)
    if len(files) != 1:
        logging.error("There are no files or more than one file in the directory.")
        exit()
    return pd.read_csv(os.path.join(input_directory, files[0]), low_memory=False)

def filter_sample_product(df):
    logging.info("Filtering rows where 'variant.name' contains 'Sample product'")
    return df[~df['variant.name'].str.contains('Sample product', na=False)]

def clean_sku_and_barcode(df):
    logging.info("Cleaning 'variant.sku' column")
    df = df[df['variant.sku'].notna() & (df['variant.sku'] != '')]
    duplicated_skus_df = df[df['variant.sku'].duplicated(keep=False)].copy()
    if not duplicated_skus_df.empty:
        error_file = os.path.join(output_directory, 'duplicates.csv')
        duplicated_skus_df.to_csv(error_file, index=False)
        logging.info(f"Duplicate SKUs saved to {error_file}")
    # Keep first occurrence in main df
    df = df[~df['variant.sku'].duplicated(keep='first')]
    return df

def select_required_columns(df):
    logging.info("Selecting required columns")
    df.columns = df.columns.str.strip()
    start_index = df.columns.get_loc('variant.package_height')
    columns_to_keep = ['variant.sku', 'variant.weight'] + list(df.columns[start_index:])
    df = df[columns_to_keep]
    df = df.drop(columns=['variant.weight.1', 'variant.target_enabled', 'variant.target_listing_action'])
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, df.notna().any(axis=0)]
    return df

def rename_columns(df):
    df = df.rename(columns={'variant.sku': 'sku'})
    df.columns = [
        'fields.' + col[8:] + '.value' if col.startswith('variant.') and col in ['variant.weight', 'variant.height', 'variant.width', 'variant.length'] else 
        'fields.' + col[8:] if col.startswith('variant.') else col
        for col in df.columns
    ]
    return df

def main():
    logging.info("Starting data processing")
    df = load_file_from_directory(input_directory)
    df = filter_sample_product(df)
    df = clean_sku_and_barcode(df)
    df = select_required_columns(df)
    df = rename_columns(df)
    
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    output_file = os.path.join(output_directory, 'variantattributes.csv')
    df.to_csv(output_file, index=False)
    logging.info(f"Successfully filtered the data! The filtered data is saved to {output_file}")

if __name__ == "__main__":
    main()
