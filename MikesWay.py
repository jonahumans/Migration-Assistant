
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

            # Create parent rows
            parent_rows = parents_df.copy()
            parent_rows['group'] = 'product'

            # Add group_skus column to parent rows
            parent_rows['group_skus'] = parent_rows['sku']  # Parent sku is the same as group_skus
            
            # Create variant rows
            variant_rows = parent_attrs_df.copy()

            # Add group_skus information
            variant_rows = pd.merge(variant_rows, group_skus_df, on='sku', how='left')
            
            # Rename the group_skus.0 column to group_skus
            variant_rows = variant_rows.rename(columns={'group_skus.0': 'group_skus'})

            # Add variant attributes
            variant_rows = pd.merge(variant_rows, variant_attrs_df, on='sku', how='left')

            # Add barcode column
            variant_rows['barcode'] = variant_rows['sku']
            parent_rows['barcode'] = parent_rows['sku']

            # Add group column for variants
            variant_rows['group'] = 'variant'

            # Combine parent and variant rows
            result_df = pd.concat([parent_rows, variant_rows], ignore_index=True)
            
            # Make sure group_skus is the very first column
            if 'group_skus' in result_df.columns:
                # Get all columns except group_skus
                other_cols = [col for col in result_df.columns if col != 'group_skus']
                # Reorder with group_skus as first column
                result_df = result_df[['group_skus'] + other_cols]

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
