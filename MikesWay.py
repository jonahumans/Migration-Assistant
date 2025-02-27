import os
import pandas as pd
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_mikes_way(input_file):
    """
    Process a product spreadsheet in Mike's Way format.
    """
    logging.info("Starting MikesWay CSV generation")

    # Create output directory if it doesn't exist
    output_dir = './output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        # Load data from CSV files
        group_skus_path = os.path.join(output_dir, 'group_skus.csv')
        parent_attrs_path = os.path.join(output_dir, 'parentattributesonvarients.csv')
        parents_path = os.path.join(output_dir, 'parents.csv')
        variant_attrs_path = os.path.join(output_dir, 'variantattributes.csv')

        if all(os.path.exists(p) for p in [group_skus_path, parent_attrs_path, parents_path, variant_attrs_path]):
            group_skus_df = pd.read_csv(group_skus_path)
            parent_attrs_df = pd.read_csv(parent_attrs_path)
            parents_df = pd.read_csv(parents_path)
            variant_attrs_df = pd.read_csv(variant_attrs_path)

            logging.info(f"Loaded group_skus.csv: {len(group_skus_df)} rows")
            logging.info(f"Loaded parentattributesonvarients.csv: {len(parent_attrs_df)} rows")
            logging.info(f"Loaded parents.csv: {len(parents_df)} rows")
            logging.info(f"Loaded variantattributes.csv: {len(variant_attrs_df)} rows")

            # Load original input data to get variant.name and variant.barcode
            input_file_path = os.path.join('./input', os.listdir('./input')[0])
            original_df = pd.read_csv(input_file_path)

            # Create a mapping dataframe with sku, variant.name, and variant.barcode
            name_barcode_map = original_df[['variant.sku', 'variant.name', 'variant.barcode']].dropna(subset=['variant.sku'])
            name_barcode_map = name_barcode_map.rename(columns={'variant.sku': 'sku'})

            # Create parent rows
            parent_rows = parents_df.copy()
            # Identify as product (parent) in the group column
            parent_rows['group'] = 'product'

            # Create variant rows
            variant_rows = parent_attrs_df.copy()

            # Add group_skus information
            variant_rows = pd.merge(variant_rows, group_skus_df, on='sku', how='left')

            # Add variant attributes
            variant_rows = pd.merge(variant_rows, variant_attrs_df, on='sku', how='left')

            # Add name and barcode from original data
            # For variant rows
            variant_rows = pd.merge(variant_rows, name_barcode_map[['sku', 'variant.name', 'variant.barcode']], 
                                  on='sku', how='left')
            variant_rows['barcode'] = variant_rows['variant.barcode']
            # Ensure we use the variant.name from the input file for unique product names
            variant_rows['name'] = variant_rows['variant.name']
            # Remove the temporary columns
            variant_rows = variant_rows.drop(['variant.name', 'variant.barcode'], axis=1, errors='ignore')

            # For parent rows (will use sku as barcode if no match found)
            parent_rows = pd.merge(parent_rows, name_barcode_map[['sku', 'variant.name', 'variant.barcode']], 
                                 on='sku', how='left')
            parent_rows['barcode'] = parent_rows['variant.barcode'].fillna(parent_rows['sku'])
            # Ensure parent rows also use the correct name from the input file
            parent_rows['name'] = parent_rows['variant.name'].fillna(parent_rows['fields.name'])
            # Remove the temporary columns
            parent_rows = parent_rows.drop(['variant.name', 'variant.barcode'], axis=1, errors='ignore')

            # Identify as variant in the group column
            variant_rows['group'] = 'variant'

            # Combine parent and variant rows
            result_df = pd.concat([parent_rows, variant_rows], ignore_index=True)

            # Reorder columns to put group_skus.0 immediately after sku column
            if 'group_skus.0' in result_df.columns and 'sku' in result_df.columns:
                # Get all columns except sku and group_skus.0
                other_cols = [col for col in result_df.columns if col != 'sku' and col != 'group_skus.0']
                # Reorder with sku first, then group_skus.0, then all other columns
                cols = ['sku', 'group_skus.0'] + other_cols
                result_df = result_df[cols]

            # Save to MikesWay.csv
            output_file = os.path.join(output_dir, 'MikesWay.csv')
            result_df.to_csv(output_file, index=False)
            logging.info(f"Successfully created MikesWay.csv with {len(result_df)} rows")
            return True
        else:
            missing_files = [p for p in [group_skus_path, parent_attrs_path, parents_path, variant_attrs_path] if not os.path.exists(p)]
            logging.error(f"Missing required files: {missing_files}")
            return False

    except Exception as e:
        logging.error(f"Error generating MikesWay.csv: {str(e)}")
        import traceback
        error_traceback = traceback.format_exc()
        logging.error(error_traceback)
        return False

if __name__ == "__main__":
    # This script can be run standalone if needed
    if os.path.exists('./input') and len(os.listdir('./input')) == 1:
        input_file = os.path.join('./input', os.listdir('./input')[0])
        process_mikes_way(input_file)
    else:
        logging.error("No input file found in ./input directory")