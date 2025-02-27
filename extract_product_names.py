
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
    
    mikesway_file = os.path.join(output_dir, 'MikesWay.csv')
    if not os.path.exists(mikesway_file):
        print(f"Error: {mikesway_file} not found.")
        return False
    
    # Read the input CSV, group_skus CSV, and MikesWay CSV
    try:
        input_df = pd.read_csv(input_file, low_memory=False)
        group_skus_df = pd.read_csv(group_skus_file)
        mikesway_df = pd.read_csv(mikesway_file)
        
        print(f"Input file loaded: {len(input_df)} rows")
        print(f"Group SKUs file loaded: {len(group_skus_df)} rows")
        print(f"MikesWay file loaded: {len(mikesway_df)} rows")
        
        # Filter MikesWay to only get rows where group='variant'
        variant_rows = mikesway_df[mikesway_df['group'] == 'variant']
        print(f"Found {len(variant_rows)} rows with group='variant'")
        
        # Merge with group_skus to get the parent SKUs
        variant_with_groups = pd.merge(variant_rows, group_skus_df, on='sku', how='inner')
        print(f"Found {len(variant_with_groups)} variants with group_skus.0 values")
        
        # Get the variant.name from the input file by matching variant.sku
        result_df = pd.DataFrame(columns=['sku', 'group_skus.0', 'variant_name'])
        
        for index, row in variant_with_groups.iterrows():
            sku = row['sku']
            group_sku = row['group_skus.0']
            
            # Find the matching row in the input file by variant.sku
            matching_rows = input_df[input_df['variant.sku'] == sku]
            
            if not matching_rows.empty:
                variant_name = matching_rows['variant.name'].values[0]
                result_df = pd.concat([result_df, pd.DataFrame({
                    'sku': [sku], 
                    'group_skus.0': [group_sku], 
                    'variant_name': [variant_name]
                })], ignore_index=True)
        
        # Save the results to CSV
        output_file = os.path.join(output_dir, 'variant_names.csv')
        result_df.to_csv(output_file, index=False)
        print(f"Successfully created {output_file} with {len(result_df)} variant names")
        
        # Display the first few results
        print("\nSample of extracted variant names:")
        print(result_df.head())
        
        return True
    
    except Exception as e:
        print(f"Error processing files: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    extract_variant_names()
