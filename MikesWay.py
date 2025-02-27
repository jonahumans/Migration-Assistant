
import os
import pandas as pd
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_mikes_way(input_file):
    """
    Process a product spreadsheet in Mike's Way format similar to Vapor Apparel.
    This creates a hierarchical parent-child structure with the format:
    - group: 'product' for parent rows
    - group: 'variant' for child rows (with variant-ID matching parent)
    - Children are grouped under their parents in the output file
    """
    logging.info("Starting Mike's Way processing")
    
    # Load the original input file
    if not os.path.exists(input_file):
        logging.error(f"Input file not found: {input_file}")
        return False
    
    try:
        # Read the CSV file
        logging.info(f"Attempting to read input file: {input_file}")
        df = pd.read_csv(input_file, low_memory=False)
        logging.info(f"Successfully loaded {len(df)} records from {input_file}")
        logging.info(f"Columns found: {', '.join(df.columns)}")
        
        # Create output directory if it doesn't exist
        output_dir = './output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Check for duplicate SKUs and alert
        if 'variant.sku' in df.columns:
            duplicated_skus = df['variant.sku'].dropna()[df['variant.sku'].duplicated(keep=False)]
            if not duplicated_skus.empty:
                logging.warning(f"Duplicated SKUs found: {duplicated_skus.unique().tolist()}")
        
        # Check for duplicate barcodes and alert
        if 'variant.barcode' in df.columns:
            duplicated_barcodes = df['variant.barcode'].dropna()[df['variant.barcode'].duplicated(keep=False)]
            if not duplicated_barcodes.empty:
                logging.warning(f"Duplicated barcodes found: {duplicated_barcodes.unique().tolist()}")
            
        # Process the data to create Vapor Apparel style format
        # Identify parent and variant rows
        if 'variant.product_id' in df.columns and 'id' in df.columns:
            # Find all unique parent IDs
            parent_ids = df[df['variant.product_id'].notna()]['variant.product_id'].unique()
            
            # Create an empty DataFrame to store the results
            result_df = pd.DataFrame()
            
            # For each parent ID
            for parent_id in parent_ids:
                # Get the parent row
                parent_row = df[df['id'] == parent_id].copy()
                if parent_row.empty:
                    # Create a dummy parent if none exists
                    parent_row = pd.DataFrame([df[df['variant.product_id'] == parent_id].iloc[0].copy()])
                    
                    # Extract the base product name (removing variant-specific info after " - " if present)
                    if 'variant.name' in parent_row.columns:
                        parent_name = parent_row['variant.name'].iloc[0].split(' - ')[0] if ' - ' in parent_row['variant.name'].iloc[0] else parent_row['variant.name'].iloc[0]
                        parent_row['variant.name'] = parent_name
                    elif 'name' in parent_row.columns:
                        parent_name = parent_row['name'].iloc[0].split(' - ')[0] if ' - ' in parent_row['name'].iloc[0] else parent_row['name'].iloc[0]
                        parent_row['name'] = parent_name
                
                # Set parent metadata
                parent_row['group'] = 'product'
                parent_row['sku'] = 'variant-' + str(int(parent_id))
                parent_row['group_skus.0'] = ""  # Parent has empty group_skus.0
                parent_row['options.0'] = 'size'
                parent_row['options.1'] = 'color'
                
                # Get all variant rows for this parent
                variant_rows = df[df['variant.product_id'] == parent_id].copy()
                variant_rows['group'] = 'variant'
                variant_rows['group_skus.0'] = 'variant-' + str(int(parent_id))
                
                # Rename columns to match Vapor Apparel format
                # For both parent and variants
                columns_to_rename = {
                    'variant.sku': 'sku',
                    'variant.barcode': 'upc',
                    'variant.mpn': 'mpn',
                    'variant.weight': 'fields.weight.value',
                    'variant.package_weight': 'fields.package_weight.value',
                    'variant.height': 'fields.height.value',
                    'variant.width': 'fields.width.value',
                    'variant.length': 'fields.length.value',
                    'variant.package_height': 'fields.package_height.value',
                    'variant.package_width': 'fields.package_width.value',
                    'variant.package_length': 'fields.package_length.value',
                    'variant.price': 'pricing_item.price.amount',
                    'variant.compare_price': 'pricing_item.msrp.amount',
                    'variant.name': 'name'
                }
                
                # Apply renames if columns exist
                for old_col, new_col in columns_to_rename.items():
                    if old_col in parent_row.columns and old_col not in ['sku']:
                        parent_row = parent_row.rename(columns={old_col: new_col})
                    if old_col in variant_rows.columns and old_col not in ['sku']:
                        variant_rows = variant_rows.rename(columns={old_col: new_col})
                
                # Extract size and color information for variants
                if 'option1.name' in variant_rows.columns and 'option1.value' in variant_rows.columns:
                    size_column = variant_rows['option1.value'] if variant_rows['option1.name'].iloc[0].lower() == 'size' else variant_rows['option2.value']
                    color_column = variant_rows['option2.value'] if variant_rows['option1.name'].iloc[0].lower() == 'size' else variant_rows['option1.value']
                    variant_rows['fields.size'] = size_column
                    variant_rows['fields.color'] = color_column
                
                # For variant names, prefix with parent name if not already there
                if 'name' in variant_rows.columns and 'name' in parent_row.columns:
                    parent_name = parent_row['name'].iloc[0]
                    variant_rows['name'] = variant_rows['name'].apply(
                        lambda x: f"{parent_name}, {x.split(' - ')[1]}" if ' - ' in x else x
                    )
                
                # Check for and handle duplicate column names before concatenation
                parent_cols = set(parent_row.columns)
                variant_cols = set(variant_rows.columns)
                
                # Log duplicate columns for debugging
                logging.info(f"Parent columns: {len(parent_cols)}, Variant columns: {len(variant_cols)}")
                
                # Ensure unique indexing before concatenation
                parent_row_copy = parent_row.copy()
                variant_rows_copy = variant_rows.copy()
                
                # Always reset indexes before concatenation to avoid conflicts
                parent_row_copy = parent_row_copy.reset_index(drop=True)
                variant_rows_copy = variant_rows_copy.reset_index(drop=True)
                
                # Combine parent and variants, with parent first
                try:
                    # Force ignore_index=True to avoid index conflicts
                    combined = pd.concat([parent_row_copy, variant_rows_copy], ignore_index=True)
                    
                    # Add to result, also with ignore_index=True
                    if result_df.empty:
                        result_df = combined.copy()
                    else:
                        result_df = pd.concat([result_df, combined], ignore_index=True)
                except Exception as e:
                    logging.error(f"Concatenation error: {str(e)}")
                    logging.error(f"Parent row shape: {parent_row_copy.shape}, Variant rows shape: {variant_rows_copy.shape}")
                    # Proceed without this batch to avoid complete failure
                    continue
                
            # Handle case where dataframe is still empty
            if result_df.empty:
                logging.warning("No parent-child relationships found in the data.")
                return False
                
        else:
            # For files without explicit parent-child relationship, group by product name
            logging.info("No explicit parent-child structure found, creating by product name")
            
            # Get unique product names (strip variant info if present)
            product_names = []
            name_column = 'variant.name' if 'variant.name' in df.columns else 'name'
            if name_column not in df.columns:
                logging.error("Could not find name column in the data")
                return False
                
            for name in df[name_column].unique():
                base_name = name.split(' - ')[0] if ' - ' in name else name
                if base_name not in product_names:
                    product_names.append(base_name)
            
            # Create an empty DataFrame to store the results
            result_df = pd.DataFrame()
            
            # For each product name
            for product_id, product_name in enumerate(product_names, start=1):
                # Find all variants with this base name
                variants = df[df[name_column].str.startswith(product_name)].copy()
                
                # Create parent row (use first variant as template)
                parent_row = variants.iloc[0:1].copy()
                parent_row[name_column] = product_name
                parent_row['group'] = 'product'
                parent_row['sku'] = f'variant-{product_id}'
                parent_row['group_skus.0'] = ""
                parent_row['options.0'] = 'size'
                parent_row['options.1'] = 'color'
                
                # Set variant properties
                variants['group'] = 'variant'
                variants['group_skus.0'] = f'variant-{product_id}'
                
                # Extract size and color if present
                if 'option1.name' in variants.columns and 'option1.value' in variants.columns:
                    size_column = variants['option1.value'] if variants['option1.name'].iloc[0].lower() == 'size' else variants['option2.value']
                    color_column = variants['option2.value'] if variants['option1.name'].iloc[0].lower() == 'size' else variants['option1.value']
                    variants['fields.size'] = size_column
                    variants['fields.color'] = color_column
                
                # Rename columns to match Vapor Apparel format
                columns_to_rename = {
                    'variant.sku': 'sku',
                    'variant.barcode': 'upc',
                    'variant.mpn': 'mpn',
                    'variant.weight': 'fields.weight.value',
                    'variant.package_weight': 'fields.package_weight.value',
                    'variant.height': 'fields.height.value',
                    'variant.width': 'fields.width.value',
                    'variant.length': 'fields.length.value',
                    'variant.package_height': 'fields.package_height.value',
                    'variant.package_width': 'fields.package_width.value',
                    'variant.package_length': 'fields.package_length.value',
                    'variant.price': 'pricing_item.price.amount',
                    'variant.compare_price': 'pricing_item.msrp.amount',
                    'variant.name': 'name'
                }
                
                # Apply renames if columns exist
                for old_col, new_col in columns_to_rename.items():
                    if old_col in parent_row.columns and old_col not in ['sku']:
                        parent_row = parent_row.rename(columns={old_col: new_col})
                    if old_col in variants.columns and old_col not in ['sku']:
                        variants = variants.rename(columns={old_col: new_col})
                
                # For variant names, prefix with parent name if not already there
                name_column = 'name' if 'name' in variants.columns else name_column
                if name_column in variants.columns and name_column in parent_row.columns:
                    parent_name = parent_row[name_column].iloc[0]
                    variants[name_column] = variants[name_column].apply(
                        lambda x: f"{parent_name}, {x.split(' - ')[1]}" if ' - ' in x else x
                    )
                
                # Combine parent and variants, with parent first
                combined = pd.concat([parent_row, variants], ignore_index=True)
                
                # Add to result
                result_df = pd.concat([result_df, combined], ignore_index=True)
        
        # Copy any attribute columns from parent to all its variants (similar to parentattributesonvarients.py)
        group_ids = result_df[result_df['group'] == 'product']['sku'].values
        for group_id in group_ids:
            # Get parent row
            parent = result_df[result_df['sku'] == group_id]
            if parent.empty:
                continue
                
            # Get all variants for this parent
            base_id = group_id.replace('variant-', '')
            variants_mask = (result_df['group'] == 'variant') & (result_df['group_skus.0'] == f'variant-{base_id}')
            
            # Copy attributes from parent to variants
            field_cols = [col for col in parent.columns if col.startswith('fields.') and 
                         col not in ['fields.size', 'fields.color']]
            
            for col in field_cols:
                if pd.notna(parent[col].iloc[0]):
                    result_df.loc[variants_mask, col] = parent[col].iloc[0]
        
        # Prepend 'fields.' to all attribute columns except key ones
        field_columns = [col for col in result_df.columns 
                        if not col.startswith('fields.') 
                        and col not in ['group', 'group_skus.0', 'options.0', 'options.1', 
                                        'name', 'sku', 'mpn', 'upc', 'pricing_item.price.amount', 
                                        'pricing_item.msrp.amount'] 
                        and not col.startswith('option')]
                        
        for col in field_columns:
            if col in result_df.columns:
                result_df = result_df.rename(columns={col: f'fields.{col}'})
        
        # Rearrange columns to match Vapor Apparel format
        # Ensure these key columns are at the beginning
        key_columns = ['group', 'group_skus.0', 'options.0', 'options.1', 'name', 
                      'fields.description', 'sku', 'mpn', 'upc', 
                      'pricing_item.price.amount', 'pricing_item.msrp.amount']
        
        # Filter for columns that actually exist in our DataFrame
        existing_key_columns = [col for col in key_columns if col in result_df.columns]
        other_columns = [col for col in result_df.columns if col not in key_columns]
        
        # Reorder columns
        result_df = result_df[existing_key_columns + other_columns]
        
        # Save the file in Mike's Way format
        output_file = os.path.join(output_dir, 'MikesWay.csv')
        result_df.to_csv(output_file, index=False)
        logging.info(f"Successfully saved Mike's Way format to {output_file}")
        
        # Also save a copy directly in output for easy access
        output_file_main = os.path.join('./output', 'MikesWay.csv')
        result_df.to_csv(output_file_main, index=False)
        logging.info(f"Also saved to {output_file_main} for direct access")
        
        return True
        
    except Exception as e:
        logging.error(f"Error processing data: {str(e)}")
        import traceback
        error_traceback = traceback.format_exc()
        logging.error(error_traceback)
        # Create error file for debugging
        try:
            # Ensure output directory exists
            if not os.path.exists('./output'):
                os.makedirs('./output')
                
            # Write detailed error log
            with open('./output/mikesway_error.log', 'w') as f:
                f.write(f"Error: {str(e)}\n\n{error_traceback}")
                
            # Also write to root directory for easier access
            with open('./mikesway_error.log', 'w') as f:
                f.write(f"Error: {str(e)}\n\n{error_traceback}")
                
            logging.error(f"Error logs written to ./output/mikesway_error.log and ./mikesway_error.log")
        except Exception as log_error:
            logging.error(f"Failed to write error log: {str(log_error)}")
        return False

if __name__ == "__main__":
    # This script can be run standalone if needed
    if os.path.exists('./input') and len(os.listdir('./input')) == 1:
        input_file = os.path.join('./input', os.listdir('./input')[0])
        process_mikes_way(input_file)
    else:
        logging.error("No input file found in ./input directory")
