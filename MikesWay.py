
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
        df = pd.read_csv(input_file, low_memory=False)
        logging.info(f"Loaded {len(df)} records from {input_file}")
        
        # Create output directory if it doesn't exist
        output_dir = './output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Process the data to create Vapor Apparel style format
        # Identify parent and variant rows
        if 'variant.product_id' in df.columns and 'id' in df.columns:
            # Find all unique parent IDs
            parent_ids = df[df['variant.product_id'].notna()]['variant.product_id'].unique()
            
            # Create an empty DataFrame to store the results
            result_df = pd.DataFrame(columns=df.columns)
            
            # For each parent ID
            for parent_id in parent_ids:
                # Get the parent row
                parent_row = df[df['id'] == parent_id].copy()
                if parent_row.empty:
                    # Create a dummy parent if none exists
                    parent_row = pd.DataFrame([df[df['variant.product_id'] == parent_id].iloc[0].copy()])
                    parent_name = parent_row['variant.name'].iloc[0].split(' - ')[0] if ' - ' in parent_row['variant.name'].iloc[0] else parent_row['variant.name'].iloc[0]
                    parent_row['variant.name'] = parent_name
                
                # Set parent metadata
                parent_row['group'] = 'product'
                parent_row['sku'] = 'variant-' + parent_id.astype(str)
                parent_row['options.0'] = 'size'
                parent_row['options.1'] = 'color'
                
                # Get all variant rows for this parent
                variant_rows = df[df['variant.product_id'] == parent_id].copy()
                variant_rows['group'] = 'variant'
                variant_rows['group_skus.0'] = 'variant-' + parent_id.astype(str)
                
                # Combine parent and variants, with parent first
                combined = pd.concat([parent_row, variant_rows], ignore_index=True)
                
                # Add to result
                result_df = pd.concat([result_df, combined], ignore_index=True)
            
        else:
            # For files without parent-child relationship, group by product name
            logging.info("No explicit parent-child structure found, creating by product name")
            
            # Get unique product names (strip variant info if present)
            product_names = []
            for name in df['name'].unique():
                base_name = name.split(' - ')[0] if ' - ' in name else name
                if base_name not in product_names:
                    product_names.append(base_name)
            
            # Create an empty DataFrame to store the results
            result_df = pd.DataFrame(columns=df.columns)
            
            # For each product name
            for product_id, product_name in enumerate(product_names, start=1):
                # Find all variants with this base name
                variants = df[df['name'].str.startswith(product_name)].copy()
                
                # Create parent row (use first variant as template)
                parent_row = variants.iloc[0:1].copy()
                parent_row['name'] = product_name
                parent_row['group'] = 'product'
                parent_row['sku'] = f'variant-{product_id}'
                parent_row['options.0'] = 'size'
                parent_row['options.1'] = 'color'
                
                # Set variant properties
                variants['group'] = 'variant'
                variants['group_skus.0'] = f'variant-{product_id}'
                
                # Combine parent and variants, with parent first
                combined = pd.concat([parent_row, variants], ignore_index=True)
                
                # Add to result
                result_df = pd.concat([result_df, combined], ignore_index=True)
        
        # Rearrange columns to match Vapor Apparel format
        # Ensure these key columns are at the beginning
        key_columns = ['group', 'group_skus.0', 'options.0', 'options.1', 'name']
        existing_columns = [col for col in key_columns if col in result_df.columns]
        other_columns = [col for col in result_df.columns if col not in key_columns]
        
        # Reorder columns
        result_df = result_df[existing_columns + other_columns]
        
        # Save the file in Mike's Way format
        output_file = os.path.join(output_dir, 'MikesWay.csv')
        result_df.to_csv(output_file, index=False)
        logging.info(f"Successfully saved Mike's Way format to {output_file}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error processing data: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    # This script can be run standalone if needed
    if os.path.exists('./input') and len(os.listdir('./input')) == 1:
        input_file = os.path.join('./input', os.listdir('./input')[0])
        process_mikes_way(input_file)
    else:
        logging.error("No input file found in ./input directory")
