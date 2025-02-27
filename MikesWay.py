
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
    - group: 'variant' for child rows
    - parent-child connections using group_skus.0 column
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
        # Determine which rows should be parents vs variants
        if 'variant.product_id' in df.columns and 'id' in df.columns:
            # Use existing parent-child structure if available
            parents = df[df['id'].notna() & df['variant.product_id'].isna()].copy()
            variants = df[df['variant.product_id'].notna()].copy()
            
            # Add 'group' column
            parents['group'] = 'product'
            variants['group'] = 'variant'
            
            # Create group_skus.0 for variants
            variants['group_skus.0'] = variants['variant.product_id'].apply(
                lambda x: f"variant-{int(x)}" if pd.notna(x) else None
            )
            
            # Create options columns
            parents['options.0'] = 'size'
            parents['options.1'] = 'color'
            
            # Combine parents and variants
            combined = pd.concat([parents, variants], ignore_index=True)
            
        else:
            # If no existing parent-child structure, create one based on product names
            # This is a simplified approach - ideally we'd use actual variant relationships
            logging.info("No parent-child structure found, creating based on product names")
            df['group'] = 'variant'
            
            # Extract parent products - group by product name and keep one row per product
            parents = df.drop_duplicates(subset=['name']).copy()
            parents['group'] = 'product'
            parents['sku'] = parents.apply(lambda row: f"parent-{row.name}", axis=1)
            
            # Add group_skus to link variants to parents
            df['group_skus.0'] = df.apply(
                lambda row: next(
                    (p['sku'] for _, p in parents.iterrows() if p['name'] == row['name']), 
                    None
                ), 
                axis=1
            )
            
            # Add options columns
            parents['options.0'] = 'size'
            parents['options.1'] = 'color'
            
            # Combine parents and variants
            combined = pd.concat([parents, df], ignore_index=True)
        
        # Rearrange columns to match Vapor Apparel format
        # Ensure these key columns are at the beginning
        key_columns = ['group', 'group_skus.0', 'options.0', 'options.1', 'name']
        existing_columns = [col for col in key_columns if col in combined.columns]
        other_columns = [col for col in combined.columns if col not in key_columns]
        
        # Reorder columns
        combined = combined[existing_columns + other_columns]
        
        # Save the file in Mike's Way format
        output_file = os.path.join(output_dir, 'MikesWay.csv')
        combined.to_csv(output_file, index=False)
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
