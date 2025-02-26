
import os
import pandas as pd
import logging

# Set input and output directories
input_directory = './input/'
output_directory = './output/'

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_file_from_directory(input_directory):
    files = os.listdir(input_directory)
    if len(files) != 1:
        logging.error("There are no files or more than one file in the directory.")
        exit()
    return pd.read_csv(os.path.join(input_directory, files[0]), low_memory=False)

def filter_sample_product(df):
    logging.info("Filtering rows where 'variant.name' or 'name' contains 'Sample product'")
    df = df[~df['variant.name'].str.contains('Sample product', na=False)]
    df = df[df['name'] != 'Sample product']
    return df

def clean_parents(parents, children):
    parent_child_count = children['variant.product_id'].value_counts()
    parents_with_multiple_children = parents[parents['id'].isin(parent_child_count[parent_child_count > 1].index)]
    parents = parents_with_multiple_children.copy()
    parents.loc[:, 'sku'] = 'variant-' + parents['id'].astype(int).astype(str)
    parents = parents.drop(columns=['variant.sku', 'target_enabled', 'id', 'variant.product_id'])
    columns = ['sku'] + [col for col in parents.columns if col != 'sku']
    parents = parents[columns]
    
    columns_to_export = [col for col in parents.columns if col != 'sku' and col != 'brand' and col != 'description']
    column_string = ','.join(columns_to_export)
    
    with open('./output/parent_columns.txt', 'w') as f:
        f.write(column_string)
        
    parents.columns = ['fields.' + col if col != 'sku' else col for col in parents.columns]
    parents['options.0'] = 'size'
    parents['options.1'] = 'color'
    
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    output_file = os.path.join(output_directory, 'parents.csv')
    parents.to_csv(output_file, index=False)

def final_link(parents, children):
    parent_child_count = children['variant.product_id'].value_counts()
    parents_with_multiple_children = parents[parents['id'].isin(parent_child_count[parent_child_count > 1].index)]
    parents = parents_with_multiple_children.copy()
    merged = children.merge(parents, left_on='variant.product_id', right_on='id', how='left')
    merged['sku'] = merged['variant.sku_x']
    merged['group_skus.0'] = 'variant-' + merged['variant.product_id_x'].astype(int).astype(str)
    merged = merged[merged['variant.product_id_x'].isin(parent_child_count[parent_child_count > 1].index)]
    final_df = merged[['sku', 'group_skus.0']]
    output_file = './output/group_skus.csv'
    final_df.to_csv(output_file, index=False)
    logging.info(f"Successfully linked parents and children. The data is saved to {output_file}")

def link_parent_child(df):
    logging.info("Linking child rows to parent rows based on 'variant.product_id'")
    children = df[df['variant.product_id'].notna()]
    parents = df[df['id'].notna()]
    clean_parents(parents, children)
    final_link(parents.copy(), children.copy())
    
    columns_to_merge = list(set(children.columns) & set(parents.columns) - {'variant.product_id', 'id'})
    children_filled = children.merge(parents[columns_to_merge + ['id']], 
                                   left_on='variant.product_id', 
                                   right_on='id', 
                                   suffixes=('', '_parent'))

    for col in columns_to_merge:
        parent_col = col + '_parent'
        children_filled[col] = children_filled[col].fillna(children_filled[parent_col])

    children_filled = children_filled.drop(columns=[col + '_parent' for col in columns_to_merge])
    children_filled = children_filled.drop(columns=['id', 'variant.product_id', 'id_parent'])
    return children_filled

def clean_sku_and_barcode(df):
    logging.info("Cleaning 'variant.sku' column")
    df = df[df['variant.sku'].notna() & (df['variant.sku'] != '')]
    duplicated_skus = df['variant.sku'].dropna()[df['variant.sku'].duplicated(keep=False)]
    if not duplicated_skus.empty:
        logging.error("Duplicates found in 'variant.sku'. Please contact management!")
        logging.error(f"Duplicated 'variant.sku' values: {duplicated_skus.unique()}")
        exit(1)

    df = df.rename(columns={'variant.sku': 'sku'})
    columns_to_export = [col for col in df.columns if col != 'sku' and col != 'brand' and col != 'description']
    column_string = ','.join(columns_to_export)
    
    with open('./output/variant_columns.txt', 'w') as f:
        f.write(column_string)
        
    df.columns = ['fields.' + col if col != 'sku' else col for col in df.columns]
    return df

def select_required_columns(df):
    logging.info("Selecting required columns from 'variant.sku', 'brand', 'description', and from 'material' to 'variant.id'")
    df.columns = df.columns.str.strip()
    material_index = df.columns.get_loc('material')
    variant_id_index = df.columns.get_loc('variant.id')
    columns_to_keep = ['name', 'variant.sku', 'brand', 'description', 'id', 'variant.product_id'] + list(df.columns[material_index:variant_id_index])
    df = df[columns_to_keep]
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, df.notna().any(axis=0)]
    return df

def main():
    logging.info("Starting data processing")
    df = load_file_from_directory(input_directory)
    df = filter_sample_product(df)
    df = select_required_columns(df)
    df = link_parent_child(df)
    df = clean_sku_and_barcode(df)
    
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    output_file = os.path.join(output_directory, 'parentattributesonvarients.csv')
    df.to_csv(output_file, index=False)
    
    logging.info(f"Successfully filtered the data! The selected data is saved to {output_file}")

if __name__ == "__main__":
    main()
