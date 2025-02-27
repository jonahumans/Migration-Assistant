
import os
import pandas as pd
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_mikesway_csv():
    """
    Generates MikesWay.csv by combining data from:
    - group_skus.csv
    - parentattributesonvarients.csv
    - parents.csv
    - variantattributes.csv
    """
    logging.info("Starting MikesWay CSV generation")
    
    # Create output directory if it doesn't exist
    output_dir = './output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Check if required files exist
    required_files = [
        os.path.join(output_dir, 'group_skus.csv'),
        os.path.join(output_dir, 'parentattributesonvarients.csv'),
        os.path.join(output_dir, 'parents.csv'),
        os.path.join(output_dir, 'variantattributes.csv')
    ]
    
    for file_path in required_files:
        if not os.path.exists(file_path):
            logging.error(f"Required file not found: {file_path}")
            return False
    
    try:
        # Load data from CSV files
        group_skus_df = pd.read_csv(os.path.join(output_dir, 'group_skus.csv'))
        parent_attrs_df = pd.read_csv(os.path.join(output_dir, 'parentattributesonvarients.csv'))
        parents_df = pd.read_csv(os.path.join(output_dir, 'parents.csv'))
        variant_attrs_df = pd.read_csv(os.path.join(output_dir, 'variantattributes.csv'))
        
        logging.info(f"Loaded group_skus.csv: {len(group_skus_df)} rows")
        logging.info(f"Loaded parentattributesonvarients.csv: {len(parent_attrs_df)} rows")
        logging.info(f"Loaded parents.csv: {len(parents_df)} rows")
        logging.info(f"Loaded variantattributes.csv: {len(variant_attrs_df)} rows")
        
        # Create parent rows first
        parent_rows = parents_df.copy()
        parent_rows['group'] = 'product'
        parent_rows['barcode'] = parent_rows['sku']
        
        # Add variant column (replacing status if it exists)
        if 'status' in parent_rows.columns:
            parent_rows.drop('status', axis=1, inplace=True, errors='ignore')
        parent_rows['variant'] = parent_rows['sku']
        
        # Prepare variant rows
        variant_rows = parent_attrs_df.copy()
        
        # Add group_skus information
        variant_rows = pd.merge(variant_rows, group_skus_df, on='sku', how='left')
        
        # Add variant attributes
        variant_rows = pd.merge(variant_rows, variant_attrs_df, on='sku', how='left')
        
        # Add barcode column
        variant_rows['barcode'] = variant_rows['sku'].apply(lambda x: ''.join(filter(str.isalnum, str(x))))
        
        # Replace status column with sku and name it "variant"
        if 'status' in variant_rows.columns:
            variant_rows.drop('status', axis=1, inplace=True, errors='ignore')
        variant_rows['variant'] = variant_rows['sku']
        
        # Add group column
        variant_rows['group'] = 'variant'
        
        # Combine parent and variant rows
        # First, organize by parent-child relationships
        result_df = pd.DataFrame()
        
        # Get unique parent SKUs
        parent_skus = parent_rows['sku'].unique()
        
        # For each parent, add its rows followed by its children
        for parent_sku in parent_skus:
            # Get parent row
            parent = parent_rows[parent_rows['sku'] == parent_sku].copy()
            
            # Extract parent ID from 'variant-X' format
            parent_id = parent_sku.replace('variant-', '')
            
            # Get child rows for this parent
            children = variant_rows[variant_rows['group_skus.0'] == parent_sku].copy()
            
            # Combine parent and children
            combined = pd.concat([parent, children], ignore_index=True)
            
            # Add to result
            if result_df.empty:
                result_df = combined
            else:
                result_df = pd.concat([result_df, combined], ignore_index=True)
        
        # Add any variants without parents
        orphans = variant_rows[~variant_rows['sku'].isin(result_df['sku'])].copy()
        if not orphans.empty:
            result_df = pd.concat([result_df, orphans], ignore_index=True)
        
        # Save to MikesWay.csv
        output_file = os.path.join(output_dir, 'MikesWay.csv')
        result_df.to_csv(output_file, index=False)
        logging.info(f"Successfully created MikesWay.csv with {len(result_df)} rows")
        return True
        
    except Exception as e:
        logging.error(f"Error generating MikesWay.csv: {str(e)}")
        import traceback
        error_traceback = traceback.format_exc()
        logging.error(error_traceback)
        
        # Create error file for debugging
        try:
            with open('./output/mikesway_error.log', 'w') as f:
                f.write(f"Error: {str(e)}\n\n{error_traceback}")
        except:
            pass
        
        return False

if __name__ == "__main__":
    generate_mikesway_csv()
