
import os
import pandas as pd

def extract_variant_names():
    # Load the input CSV file
    input_dir = './input'
    output_dir = './output'
    
    # Get the input file
    input_files = os.listdir(input_dir)
    if len(input_files) == 0:
        print("No input file found.")
        return False
    
    input_file = os.path.join(input_dir, input_files[0])
    print(f"Processing file: {input_file}")
    
    # Load the group_skus.csv file to get products with group_skus.0 values
    group_skus_file = os.path.join(output_dir, 'group_skus.csv')
    if not os.path.exists(group_skus_file):
        print(f"Error: {group_skus_file} not found.")
        return False
    
    # Read the input CSV and group_skus CSV
    try:
        input_df = pd.read_csv(input_file, low_memory=False)
        group_skus_df = pd.read_csv(group_skus_file)
        
        print(f"Input file loaded: {len(input_df)} rows")
        print(f"Group SKUs file loaded: {len(group_skus_df)} rows")
        
        # Create a list of all unique parent SKUs (group_skus.0 values)
        parent_skus = group_skus_df['group_skus.0'].unique()
        print(f"Found {len(parent_skus)} unique parent SKUs")
        
        # Extract the corresponding parent IDs from the variant formatting
        parent_ids = [int(sku.split('-')[1]) for sku in parent_skus if '-' in sku]
        
        # Create a result dataframe to store the parent SKU and corresponding product name
        result_df = pd.DataFrame(columns=['parent_sku', 'product_name'])
        
        # For each parent ID, get the product name
        for parent_id in parent_ids:
            # Find the parent product name by matching the ID
            parent_rows = input_df[input_df['id'] == parent_id]
            
            if not parent_rows.empty:
                parent_name = parent_rows['name'].values[0]
                parent_sku = f"variant-{parent_id}"
                result_df = pd.concat([result_df, pd.DataFrame({'parent_sku': [parent_sku], 'product_name': [parent_name]})], ignore_index=True)
        
        # Save the results to CSV
        output_file = os.path.join(output_dir, 'product_names.csv')
        result_df.to_csv(output_file, index=False)
        print(f"Successfully created {output_file} with {len(result_df)} product names")
        
        # Display the first few results
        print("\nSample of extracted product names:")
        print(result_df.head())
        
        return True
    
    except Exception as e:
        print(f"Error processing files: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    extract_variant_names()
