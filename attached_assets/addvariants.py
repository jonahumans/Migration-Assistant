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
    logging.info("Cleaning 'variant.sku' and 'variant.barcode' columns")
    df = df[df['variant.sku'].notna() & (df['variant.sku'] != '')]  # Drop empty 'variant.sku'
    duplicated_skus = df['variant.sku'].dropna()[df['variant.sku'].duplicated(keep=False)]
    if not duplicated_skus.empty:
        logging.error("Duplicates found in 'variant.sku'. Please contact management!")
        logging.error(f"Duplicated 'variant.sku' values: {duplicated_skus.unique()}")
        exit(1)
    df['variant.barcode'] = pd.to_numeric(df['variant.barcode'], errors='coerce')
    df.loc[df['variant.barcode'].duplicated(keep=False), 'variant.barcode'] = None  # Remove duplicates
    return df

# Format pricing columns
def format_pricing(df):
    logging.info("Formatting 'variant.price' and 'variant.compare_price' columns")
    df['variant.price'] = df['variant.price'].apply(lambda x: '{:.2f}'.format(x) if pd.notna(x) else x)
    df['variant.compare_price'] = df['variant.compare_price'].apply(lambda x: '{:.2f}'.format(x) if pd.notna(x) else x)
    df['variant.compare_price'] = df['variant.compare_price'].fillna(df['variant.price'])  # Copy price to compare_price if NaN
    # Select only the required columns
    required_columns = ['variant.name', 'variant.sku', 'variant.barcode', 'variant.price', 'variant.compare_price', 'variant.images']
    df = df[required_columns]
    return df

# Dynamically split images into mainimage, alt1, alt2, ..., altN
def split_images(df):
    logging.info("Splitting 'variant.images' into multiple image columns")
    df['variant.images'] = df['variant.images'].fillna('')
    df['images_list'] = df['variant.images'].str.split(',')

    # Dynamically create columns for images, starting with 'mainimage'
    max_images = df['images_list'].apply(len).max()
    image_columns = ['mainimage'] + [f'alt{i}' for i in range(1, max_images + 1)]

    # Create new columns for each image
    for i, col in enumerate(image_columns):
        df[col] = df['images_list'].apply(lambda x: x[i] if isinstance(x, list) and i < len(x) else None)
    
    # Drop temporary columns
    df = df.drop(columns=['images_list', 'variant.images'])
    return df

# Insert 'group' column after 'variant.compare_price'
def insert_group_column(df):
    logging.info("Inserting 'group' column after 'variant.compare_price'")
    df.insert(df.columns.get_loc('variant.compare_price') + 1, 'group', 'variant')
    return df

# Rename the columns as per the requirement
def rename_columns(df):
    logging.info("Renaming columns to match the required format")
    return df.rename(columns={
        'variant.barcode': 'upc',
        'variant.price': 'pricing_item.price',
        'variant.compare_price': 'pricing_item.msrp.amount'
    })

# Main function to run all steps
def main():
    logging.info("Starting data processing")

    # Load the CSV file
    df = load_file_from_directory(input_directory)
    
    # Apply transformations in sequence
    df = filter_sample_product(df)
    df = clean_sku_and_barcode(df)
    df = format_pricing(df)
    df = split_images(df)
    df = insert_group_column(df)
    df = rename_columns(df)

    # Ensure output directory exists and save the cleaned data
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    output_file = os.path.join(output_directory, 'addvariants.csv')
    df.to_csv(output_file, index=False)
    
    logging.info(f"Successfully filtered the data! The filtered data is saved to {output_file}")

# Run the main function
if __name__ == "__main__":
    main()
