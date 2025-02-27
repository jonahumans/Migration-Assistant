
import pandas as pd
import os

def combine_product_data():
    print("Starting data combination process...")
    
    # Create output directory if it doesn't exist
    output_dir = './output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Load the CSV files
    try:
        # Check if files exist
        files_to_check = [
            os.path.join(output_dir, 'group_skus.csv'),
            os.path.join(output_dir, 'parentattributesonvarients.csv'),
            os.path.join(output_dir, 'parents.csv'),
            os.path.join(output_dir, 'variantattributes.csv')
        ]
        
        for file_path in files_to_check:
            if not os.path.exists(file_path):
                print(f"Error: Required file {file_path} not found.")
                return False
        
        # Load the data
        group_skus_df = pd.read_csv(os.path.join(output_dir, 'group_skus.csv'))
        parent_attrs_df = pd.read_csv(os.path.join(output_dir, 'parentattributesonvarients.csv'))
        parents_df = pd.read_csv(os.path.join(output_dir, 'parents.csv'))
        variant_attrs_df = pd.read_csv(os.path.join(output_dir, 'variantattributes.csv'))
        
        print(f"Successfully loaded input files:")
        print(f"  - group_skus.csv: {len(group_skus_df)} rows")
        print(f"  - parentattributesonvarients.csv: {len(parent_attrs_df)} rows")
        print(f"  - parents.csv: {len(parents_df)} rows")
        print(f"  - variantattributes.csv: {len(variant_attrs_df)} rows")
    
    except Exception as e:
        print(f"Error loading CSV files: {str(e)}")
        return False
    
    # Start with parent_attrs_df as the base dataframe since it contains most product info
    combined_df = parent_attrs_df.copy()
    
    # Add group_skus information
    combined_df = pd.merge(combined_df, group_skus_df, on='sku', how='left')
    
    # Add variant attributes
    combined_df = pd.merge(combined_df, variant_attrs_df, on='sku', how='left')
    
    # Add barcode column (generated from SKU for this example)
    combined_df['barcode'] = combined_df['sku'].apply(lambda x: ''.join(filter(str.isalnum, x)))
    
    # Ensure all columns have consistent naming
    for col in combined_df.columns:
        if not col.startswith('fields.') and col not in ['sku', 'barcode', 'group_skus.0']:
            combined_df.rename(columns={col: f'fields.{col}'}, inplace=True)
    
    # Save the combined data
    output_file = os.path.join(output_dir, 'MikesWay.csv')
    combined_df.to_csv(output_file, index=False)
    print(f"Successfully created combined data file with {len(combined_df)} rows at {output_file}")
    return True

if __name__ == "__main__":
    combine_product_data()
