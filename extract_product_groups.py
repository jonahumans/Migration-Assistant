
import os
import pandas as pd

def extract_product_groups():
    """Extract all items with group='product' from MikesWay.csv"""
    output_dir = './output'
    mikesway_file = os.path.join(output_dir, 'MikesWay.csv')
    
    if not os.path.exists(mikesway_file):
        print(f"Error: {mikesway_file} not found.")
        return False
    
    try:
        # Load the MikesWay CSV file
        mikesway_df = pd.read_csv(mikesway_file)
        print(f"MikesWay file loaded: {len(mikesway_df)} rows")
        
        # Filter for rows where group='product'
        product_rows = mikesway_df[mikesway_df['group'] == 'product']
        print(f"Found {len(product_rows)} rows with group='product'")
        
        # Save to a new CSV file
        output_file = os.path.join(output_dir, 'product_groups.csv')
        product_rows.to_csv(output_file, index=False)
        print(f"Successfully saved {len(product_rows)} product groups to {output_file}")
        
        # Display sample
        print("\nSample of products:")
        if not product_rows.empty:
            sample_cols = ['sku', 'name'] if 'name' in product_rows.columns else ['sku']
            print(product_rows[sample_cols].head())
        
        return True
    
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    extract_product_groups()
