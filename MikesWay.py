
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

            # Load addvariants.csv to get pricing data in correct format
            addvariants_path = os.path.join(output_dir, 'addvariants.csv')
            if os.path.exists(addvariants_path):
                addvariants_df = pd.read_csv(addvariants_path)
                logging.info(f"Loaded addvariants.csv: {len(addvariants_df)} rows")
                
                # Create mapping for pricing data
                pricing_map = addvariants_df[['sku', 'pricing_item.price.amount', 'pricing_item.msrp.amount']].dropna(subset=['sku'])
            else:
                logging.warning("addvariants.csv not found, pricing data may be missing")
                pricing_map = pd.DataFrame(columns=['sku', 'pricing_item.price.amount', 'pricing_item.msrp.amount'])

            # Create a mapping dataframe with sku, variant.name, variant.barcode, and variant.images
            name_barcode_map = original_df[['variant.sku', 'variant.name', 'variant.barcode', 'variant.images']].dropna(subset=['variant.sku'])
            name_barcode_map = name_barcode_map.rename(columns={'variant.sku': 'sku'})
            
            # Process images from the original data
            # Split images into mainimage and alt columns
            name_barcode_map['variant.images'] = name_barcode_map['variant.images'].fillna('')
            name_barcode_map['images_list'] = name_barcode_map['variant.images'].str.split(',')
            
            # Create columns for main image and alternates
            max_images = name_barcode_map['images_list'].apply(lambda x: len(x) if isinstance(x, list) else 0).max()
            image_columns = ['main'] + [f'images.default.{i}.alternate.url' for i in range(1, max_images)]
            
            # Fill in image columns
            for i, col in enumerate(image_columns):
                name_barcode_map[col] = name_barcode_map['images_list'].apply(
                    lambda x: x[i].strip() if isinstance(x, list) and i < len(x) and x[i].strip() != '' else None
                )
            
            # Drop temporary columns
            name_barcode_map = name_barcode_map.drop(columns=['images_list', 'variant.images'])

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

            # Add name, barcode, and pricing from original data and addvariants
            # For variant rows
            # Get all image columns
            image_cols = [col for col in name_barcode_map.columns if col == 'main' or col.startswith('images.default')]
            merge_cols = ['sku', 'variant.name', 'variant.barcode'] + image_cols
            
            variant_rows = pd.merge(variant_rows, name_barcode_map[merge_cols], 
                                  on='sku', how='left')
            # Add pricing data
            variant_rows = pd.merge(variant_rows, pricing_map, 
                                  on='sku', how='left')
            
            variant_rows['barcode'] = variant_rows['variant.barcode']
            # Ensure we use the variant.name from the input file for unique product names
            variant_rows['name'] = variant_rows['variant.name']
            # Remove the temporary columns
            variant_rows = variant_rows.drop(['variant.name', 'variant.barcode'], axis=1, errors='ignore')

            # For parent rows (will use sku as barcode if no match found)
            # Get all image columns
            image_cols = [col for col in name_barcode_map.columns if col == 'main' or col.startswith('images.default')]
            merge_cols = ['sku', 'variant.name', 'variant.barcode'] + image_cols
            
            parent_rows = pd.merge(parent_rows, name_barcode_map[merge_cols], 
                                 on='sku', how='left')
            # Add pricing data to parent rows
            parent_rows = pd.merge(parent_rows, pricing_map, 
                                 on='sku', how='left')
                                 
            parent_rows['barcode'] = parent_rows['variant.barcode'].fillna(parent_rows['sku'])
            # Ensure parent rows also use the correct name from the input file
            parent_rows['name'] = parent_rows['variant.name'].fillna(parent_rows['fields.name'])
            # Remove the temporary columns
            parent_rows = parent_rows.drop(['variant.name', 'variant.barcode'], axis=1, errors='ignore')

            # Identify as variant in the group column
            variant_rows['group'] = 'variant'
            
            # Group variants by their parent
            result_df = pd.DataFrame()
            
            # Get all unique parent SKUs
            parent_skus = parent_rows['sku'].unique()
            
            # For each parent, add parent row followed by its variants
            for parent_sku in parent_skus:
                # Get parent row
                parent = parent_rows[parent_rows['sku'] == parent_sku]
                
                # Get variant rows that belong to this parent
                parent_group_sku = parent_sku  # The parent SKU is variant-X format
                variants = variant_rows[variant_rows['group_skus.0'] == parent_group_sku]
                
                # Combine parent and its variants
                combined = pd.concat([parent, variants], ignore_index=True)
                
                # Add to result
                result_df = pd.concat([result_df, combined], ignore_index=True)

            # If there are any variants without a parent in our output, add them at the end
            orphan_variants = variant_rows[~variant_rows['group_skus.0'].isin(parent_skus)]
            if not orphan_variants.empty:
                result_df = pd.concat([result_df, orphan_variants], ignore_index=True)

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
