
import os
import pandas as pd

def update_product_skus():
    """Update group_skus.0 to match sku for all rows where group='product'"""
    output_dir = './output'
    mikesway_file = os.path.join(output_dir, 'MikesWay.csv')
    
    if not os.path.exists(mikesway_file):
        print(f"Error: {mikesway_file} not found.")
        return False
    
    try:
        # Load the MikesWay CSV file
        mikesway_df = pd.read_csv(mikesway_file)
        print(f"MikesWay file loaded: {len(mikesway_df)} rows")
        
        # Create or update group_skus.0 for product rows to match their sku
        product_mask = mikesway_df['group'] == 'product'
        print(f"Found {product_mask.sum()} rows with group='product'")
        
        # For product rows, set group_skus.0 equal to sku
        mikesway_df.loc[product_mask, 'group_skus.0'] = mikesway_df.loc[product_mask, 'sku']
        
        # Save back to MikesWay.csv
        mikesway_df.to_csv(mikesway_file, index=False)
        print(f"Successfully updated MikesWay.csv. {product_mask.sum()} product rows updated.")
        
        # Display sample of updated products
        print("\nSample of updated products:")
        if not mikesway_df[product_mask].empty:
            sample = mikesway_df[product_mask][['sku', 'group_skus.0', 'group']].head()
            print(sample)
        
        return True
    
    except Exception as e:
        print(f"Error updating file: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    update_product_skus()
