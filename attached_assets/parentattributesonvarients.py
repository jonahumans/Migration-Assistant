import os
import pandas as pd
import logging

# Set input and output directories
input_directory = './input/'
output_directory = './output/'

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Check if input directory exists and fetch file
def load_file_from_directory(input_directory):
    files = os.listdir(input_directory)
    if len(files) != 1:
        logging.error("There are no files or more than one file in the directory.")
        exit()
    return pd.read_csv(os.path.join(input_directory, files[0]), low_memory=False)

# Remove rows where 'variant.name' contains 'Sample product'
def filter_sample_product(df):
    logging.info("Filtering rows where 'variant.name' or 'name' contains 'Sample product'")
    
    # Filter rows where 'variant.name' or 'name' is 'Sample product'
    df = df[~df['variant.name'].str.contains('Sample product', na=False)]  # Exclude 'Sample product' from 'variant.name'
    df = df[df['name'] != 'Sample product']  # Exclude rows where 'name' column is 'Sample product'
    
    return df

def clean_parents(parents, children):
    # Count how many children each parent has by checking how many times `parent.id` appears in `children.variant.product_id`
    parent_child_count = children['variant.product_id'].value_counts()

    # Filter parents to keep only those whose `id` has more than one corresponding child
    parents_with_multiple_children = parents[parents['id'].isin(parent_child_count[parent_child_count > 1].index)]

    # Ensure you're working on a copy to avoid the SettingWithCopyWarning
    parents = parents_with_multiple_children.copy()

    # Create new 'sku' column by combining 'variant.id' and 'variant.sku'
    parents.loc[:, 'sku'] = 'variant-' + parents['id'].astype(int).astype(str)  


    # Drop the 'variant.sku' column
    parents = parents.drop(columns=['variant.sku', 'target_enabled', 'id', 'variant.product_id'])

    # Reorder columns to make 'sku' the first column
    columns = ['sku'] + [col for col in parents.columns if col != 'sku']
    parents = parents[columns]

   # Assuming df is the DataFrame you're working with
    columns_to_export = [col for col in parents.columns if col != 'sku' and col != 'brand' and col != 'description'] 
    column_string = ','.join(columns_to_export)

    # Write the column names to a text file
    with open('./output/parent_columns.txt', 'w') as f:
        f.write(column_string)

    # Rename columns to add 'fields.' prefix, except for 'sku'
    parents.columns = ['fields.' + col if col != 'sku' else col for col in parents.columns]

    # Add 'options.0' and 'options.1' columns with values 'size' and 'color', respectively
    parents['options.0'] = 'size'
    parents['options.1'] = 'color'

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    output_file = os.path.join(output_directory, 'parents.csv')
    parents.to_csv(output_file, index=False)
    return

def final_link(parents, children):
    # Count how many children each parent has by checking how many times `parent.id` appears in `children.variant.product_id`
    parent_child_count = children['variant.product_id'].value_counts()

    # Filter parents to keep only those whose `id` has more than one corresponding child
    parents_with_multiple_children = parents[parents['id'].isin(parent_child_count[parent_child_count > 1].index)]

    # Ensure you're working on a copy to avoid the SettingWithCopyWarning
    parents = parents_with_multiple_children.copy()

    # Merge all columns from parents and children based on 'variant.product_id' and 'id'
    merged = children.merge(parents, left_on='variant.product_id', right_on='id', how='left')

    # Create the 'sku' column (which contains all 'variant.sku' from the children)
    merged['sku'] = merged['variant.sku_x']
    
    # Create the 'group_skus.0' column (which combines 'variant.id' and 'variant.sku')
    merged['group_skus.0'] = 'variant-' + merged['variant.product_id_x'].astype(int).astype(str)

    # Filter the merged dataframe to only include parents with multiple children
    merged = merged[merged['variant.product_id_x'].isin(parent_child_count[parent_child_count > 1].index)]

    # Select only the 'sku' and 'group_skus.0' columns
    final_df = merged[['sku', 'group_skus.0']]

    # Save the final result to a CSV file
    output_file = './output/group_skus.csv'
    final_df.to_csv(output_file, index=False)
    
    logging.info(f"Successfully linked parents and children. The data is saved to {output_file}")




def link_parent_child(df):
    logging.info("Linking child rows to parent rows based on 'variant.product_id'")

    # Filter out only the children (rows where 'variant.product_id' exists)
    children = df[df['variant.product_id'].notna()]  # Children have 'variant.product_id' linking to parent's 'id'

    # Filter out only the parents (rows where 'id' exists)
    parents = df[df['id'].notna()]  # Parents have an 'id' but no 'variant.product_id'

    # PARENTS STUFF FOR SEPARATE OUTPUT
    clean_parents(parents, children)
    final_link(parents.copy(), children.copy())
    
    # Perform the merge dynamically on all common columns, except for 'variant.product_id' and 'id'
    columns_to_merge = list(set(children.columns) & set(parents.columns) - {'variant.product_id', 'id'})
    children_filled = children.merge(parents[columns_to_merge + ['id']], 
                                     left_on='variant.product_id', 
                                     right_on='id', 
                                     suffixes=('', '_parent'))

    # Fill missing values in the children with the corresponding parent values
    for col in columns_to_merge:
        parent_col = col + '_parent'  # The corresponding parent column will have the '_parent' suffix
        children_filled[col] = children_filled[col].fillna(children_filled[parent_col])

    # Drop the extra columns added during the merge (with the '_parent' suffix)
    children_filled = children_filled.drop(columns=[col + '_parent' for col in columns_to_merge])

    # Drop the 'id' and 'variant.product_id' columns
    children_filled = children_filled.drop(columns=['id', 'variant.product_id', 'id_parent'])

    # Return the updated children DataFrame with parent values filled in
    return children_filled




# Clean NaN or empty string 'variant.sku' and duplicates in 'variant.sku' 
def clean_sku_and_barcode(df):
    logging.info("Cleaning 'variant.sku' column")
    df = df[df['variant.sku'].notna() & (df['variant.sku'] != '')]  # Drop empty 'variant.sku'
    duplicated_skus = df['variant.sku'].dropna()[df['variant.sku'].duplicated(keep=False)]
    if not duplicated_skus.empty:
        logging.error("Duplicates found in 'variant.sku'. Please contact management!")
        logging.error(f"Duplicated 'variant.sku' values: {duplicated_skus.unique()}")
        exit(1)

    # Rename 'variant.sku' to 'sku'
    df = df.rename(columns={'variant.sku': 'sku'})

    # Assuming df is the DataFrame you're working with
    columns_to_export = [col for col in df.columns if col != 'sku' and col != 'brand' and col != 'description'] 
    column_string = ','.join(columns_to_export)

    # Write the column names to a text file
    with open('./output/variant_columns.txt', 'w') as f:
        f.write(column_string)

    # Loop through all columns and rename them, prefixing 'fields.' to all except 'sku'
    df.columns = ['fields.' + col if col != 'sku' else col for col in df.columns]

    return df

def select_required_columns(df):
    logging.info("Selecting required columns from 'variant.sku', 'brand', 'description', and from 'material' to 'variant.id', along with 'id' and 'variant.product_id'")

    # Strip any leading/trailing whitespaces from column names
    df.columns = df.columns.str.strip()

    # Find the index of the 'material' and 'variant.id' columns
    material_index = df.columns.get_loc('material')
    variant_id_index = df.columns.get_loc('variant.id')

    # Add 'variant.sku', 'brand', 'description', 'id', and 'variant.product_id' to the columns to be selected
    columns_to_keep = ['name', 'variant.sku', 'brand', 'description', 'id', 'variant.product_id'] + list(df.columns[material_index:variant_id_index])

    # Select the columns
    df = df[columns_to_keep]

    # Remove columns with all NaN or empty values
    df = df.dropna(axis=1, how='all')  # Drop columns with all NaN values
    df = df.loc[:, df.notna().any(axis=0)]  # Keep columns with at least one non-NaN value

    return df





# Main function to run all steps
def main():
    logging.info("Starting data processing")

    # Load the CSV file
    df = load_file_from_directory(input_directory)
    
    # Apply transformations in sequence
    df = filter_sample_product(df)
    df = select_required_columns(df)
    df = link_parent_child(df)
    df = clean_sku_and_barcode(df)

    # Ensure output directory exists and save the cleaned data
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    output_file = os.path.join(output_directory, 'parentattributesonvarients.csv')
    df.to_csv(output_file, index=False)
    
    logging.info(f"Successfully filtered the data! The selected data is saved to {output_file}")

# Run the main function
if __name__ == "__main__":
    main()
