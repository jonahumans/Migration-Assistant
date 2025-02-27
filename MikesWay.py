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
        logging.info(f"Attempting to read input file: {input_file}")
        df = pd.read_csv(input_file, low_memory=False)
        logging.info(f"Successfully loaded {len(df)} records from {input_file}")

        # Create output directory if it doesn't exist
        output_dir = './output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Process the data to create Vapor Apparel style format
        # Identify parent and variant rows
        if 'variant.product_id' in df.columns and 'id' in df.columns:
            # Create empty result dataframe
            result_df = pd.DataFrame()

            # Find all unique parent IDs
            parent_ids = df[df['variant.product_id'].notna()]['variant.product_id'].unique()

            # Process each parent ID and its variants
            for parent_id in parent_ids:
                # Get the parent row
                parent_row = df[df['id'] == parent_id].copy()

                # Skip if no parent found
                if parent_row.empty:
                    continue

                # Set parent metadata
                parent_row['group'] = 'product'
                parent_row['group_skus.0'] = ""

                # Get all variant rows for this parent
                variant_rows = df[df['variant.product_id'] == parent_id].copy()
                variant_rows['group'] = 'variant'
                variant_rows['group_skus.0'] = 'variant-' + str(int(parent_id))

                # Rename variant.name to name if needed
                if 'name' not in variant_rows.columns and 'variant.name' in variant_rows.columns:
                    variant_rows = variant_rows.rename(columns={'variant.name': 'name'})

                if 'name' not in parent_row.columns and 'variant.name' in parent_row.columns:
                    parent_row = parent_row.rename(columns={'variant.name': 'name'})

                # Create barcode column from SKU
                if 'variant.sku' in variant_rows.columns:
                    variant_rows['barcode'] = variant_rows['variant.sku'].apply(lambda x: ''.join(filter(str.isalnum, str(x))))
                    parent_row['barcode'] = 'variant-' + str(int(parent_id))

                # Combine parent and variants, with parent first
                try:
                    # Reset index to avoid conflicts
                    parent_row.reset_index(drop=True, inplace=True)
                    variant_rows.reset_index(drop=True, inplace=True)

                    # Stack parent on top of variants
                    combined = pd.concat([parent_row, variant_rows], ignore_index=True)

                    # Add to result dataframe
                    if result_df.empty:
                        result_df = combined
                    else:
                        result_df = pd.concat([result_df, combined], ignore_index=True)
                except Exception as e:
                    logging.error(f"Error combining parent and variants: {str(e)}")
                    continue
        else:
            # For files without explicit parent-child relationship
            # Try to load and merge existing data
            try:
                # Load existing output files
                group_skus_path = os.path.join(output_dir, 'group_skus.csv')
                parent_attrs_path = os.path.join(output_dir, 'parentattributesonvarients.csv')
                parents_path = os.path.join(output_dir, 'parents.csv')
                variant_attrs_path = os.path.join(output_dir, 'variantattributes.csv')

                if all(os.path.exists(p) for p in [group_skus_path, parent_attrs_path, parents_path, variant_attrs_path]):
                    # Load the data
                    group_skus_df = pd.read_csv(group_skus_path)
                    parent_attrs_df = pd.read_csv(parent_attrs_path)
                    parents_df = pd.read_csv(parents_path)
                    variant_attrs_df = pd.read_csv(variant_attrs_path)

                    # Start with parent_attrs_df as the base dataframe
                    result_df = parent_attrs_df.copy()

                    # Add group_skus information
                    result_df = pd.merge(result_df, group_skus_df, on='sku', how='left')

                    # Add variant attributes
                    result_df = pd.merge(result_df, variant_attrs_df, on='sku', how='left')

                    # Add barcode column
                    result_df['barcode'] = result_df['sku'].apply(lambda x: ''.join(filter(str.isalnum, str(x))))

                    # Add group column
                    result_df['group'] = 'variant'

                    # Add parent rows
                    parent_rows = parents_df.copy()
                    parent_rows['group'] = 'product'
                    parent_rows['barcode'] = parent_rows['sku'].apply(lambda x: ''.join(filter(str.isalnum, str(x))))

                    # Combine parent and variant rows
                    result_df = pd.concat([parent_rows, result_df], ignore_index=True)

                else:
                    # Just copy the input file if we can't merge
                    result_df = df.copy()

                    # Replace status column with variant.sku and name it "variant"
                    if 'status' in result_df.columns:
                        result_df.drop('status', axis=1, inplace=True, errors='ignore')

                    # Add variant column based on available SKU field
                    if 'variant.sku' in result_df.columns:
                        result_df['variant'] = result_df['variant.sku']
                    elif 'sku' in result_df.columns:
                        result_df['variant'] = result_df['sku']

                    # Add group column - determine product type if possible, otherwise default to variant
                    if 'group' not in result_df.columns:
                        if 'variant.product_id' in result_df.columns:
                            # If it has a product_id, it's a variant
                            result_df['group'] = 'variant'
                            # Identify potential parent rows
                            if 'id' in result_df.columns:
                                parents_mask = result_df['id'].isin(result_df['variant.product_id'])
                                result_df.loc[parents_mask, 'group'] = 'product'
                        else:
                            result_df['group'] = 'variant'

                    # Add barcode column
                    if 'variant.sku' in result_df.columns:
                        result_df['barcode'] = result_df['variant.sku'].apply(lambda x: ''.join(filter(str.isalnum, str(x))))
                    elif 'sku' in result_df.columns:
                        result_df['barcode'] = result_df['sku'].apply(lambda x: ''.join(filter(str.isalnum, str(x))))

            except Exception as e:
                logging.error(f"Error merging existing data: {str(e)}")
                # Just copy the input file as fallback
                result_df = df.copy()

                # Add group column
                if 'group' not in result_df.columns:
                    result_df['group'] = 'variant'

                # Add barcode column
                if 'variant.sku' in result_df.columns:
                    result_df['barcode'] = result_df['variant.sku'].apply(lambda x: ''.join(filter(str.isalnum, str(x))))
                elif 'sku' in result_df.columns:
                    result_df['barcode'] = result_df['sku'].apply(lambda x: ''.join(filter(str.isalnum, str(x))))

        # Remove id and status columns if they exist
        columns_to_drop = ['id', 'status']
        for col in columns_to_drop:
            if col in result_df.columns:
                result_df = result_df.drop(columns=[col])
        
        # For parent rows, set sku to match group_skus of its variants
        if 'group' in result_df.columns and 'group_skus.0' in result_df.columns:
            # For each row where group is 'product'
            for idx in result_df[result_df['group'] == 'product'].index:
                parent_id = result_df.loc[idx, 'sku'].replace('variant-', '')
                # Set sku to match group_skus pattern
                result_df.loc[idx, 'sku'] = result_df.loc[idx, 'group_skus.0'] if pd.notna(result_df.loc[idx, 'group_skus.0']) else result_df.loc[idx, 'sku']
        
        # Reorder columns to put variant.barcode, group, group_skus, and variant.sku at the front
        priority_columns = []
        
        # Add variant.barcode first if it exists
        if 'variant.barcode' in result_df.columns:
            priority_columns.append('variant.barcode')
        elif 'barcode' in result_df.columns:
            priority_columns.append('barcode')
            
        if 'group' in result_df.columns:
            priority_columns.append('group')
        
        # Add any group_skus columns
        group_skus_cols = [col for col in result_df.columns if 'group_skus' in col]
        priority_columns.extend(group_skus_cols)
        
        # Add variant.sku or variant column
        if 'variant.sku' in result_df.columns:
            priority_columns.append('variant.sku')
        if 'variant' in result_df.columns:
            priority_columns.append('variant')
        
        # Filter out columns that don't exist in dataframe
        priority_columns = [col for col in priority_columns if col in result_df.columns]
        
        # Get the rest of the columns
        other_columns = [col for col in result_df.columns if col not in priority_columns]
        
        # Reorder columns
        result_df = result_df[priority_columns + other_columns]
        
        # Load additional fields from variant attributes and parent attributes files
        try:
            variant_attrs_path = os.path.join(output_dir, 'variantattributes.csv')
            parent_attrs_path = os.path.join(output_dir, 'parentattributesonvarients.csv')
            parents_path = os.path.join(output_dir, 'parents.csv')
            
            variant_fields = []
            parent_fields = []
            parent_only_fields = []
            
            # Load variant attributes if file exists
            if os.path.exists(variant_attrs_path):
                variant_attrs_df = pd.read_csv(variant_attrs_path)
                variant_fields = [col for col in variant_attrs_df.columns if col != 'sku']
                
                # Merge variant attributes with result_df
                if not variant_fields:
                    logging.warning("No variant attribute fields found")
                else:
                    # Normalize column names by removing 'fields.' prefix for merging
                    variant_attrs_df.columns = [col.replace('fields.', '') if col.startswith('fields.') else col for col in variant_attrs_df.columns]
                    variant_fields = [col.replace('fields.', '') for col in variant_fields]
                    
                    # Create temporary normalized columns for merging
                    sku_col = 'sku'
                    if 'sku' not in result_df.columns and 'variant.sku' in result_df.columns:
                        result_df['sku_temp'] = result_df['variant.sku']
                        sku_col = 'sku_temp'
                    
                    # Merge the dataframes
                    variant_attrs_df_copy = variant_attrs_df.copy()
                    result_df = pd.merge(result_df, variant_attrs_df_copy, how='left', left_on=sku_col, right_on='sku', suffixes=('', '_variant'))
                    
                    # Clean up temporary column
                    if 'sku_temp' in result_df.columns:
                        result_df = result_df.drop(columns=['sku_temp'])
            
            # Load parent attributes if file exists
            if os.path.exists(parent_attrs_path):
                parent_attrs_df = pd.read_csv(parent_attrs_path)
                parent_fields = [col for col in parent_attrs_df.columns if col != 'sku']
                
                # Normalize column names by removing 'fields.' prefix for merging
                parent_attrs_df.columns = [col.replace('fields.', '') if col.startswith('fields.') else col for col in parent_attrs_df.columns]
                parent_fields = [col.replace('fields.', '') for col in parent_fields]
                
                # Merge parent attributes with result_df
                if not parent_fields:
                    logging.warning("No parent attribute fields found")
                else:
                    sku_col = 'sku'
                    if 'sku' not in result_df.columns and 'variant.sku' in result_df.columns:
                        result_df['sku_temp'] = result_df['variant.sku']
                        sku_col = 'sku_temp'
                    
                    # Merge the dataframes
                    parent_attrs_df_copy = parent_attrs_df.copy()
                    result_df = pd.merge(result_df, parent_attrs_df_copy, how='left', left_on=sku_col, right_on='sku', suffixes=('', '_parent'))
                    
                    # Clean up temporary column
                    if 'sku_temp' in result_df.columns:
                        result_df = result_df.drop(columns=['sku_temp'])
            
            # Load parent-only fields if file exists
            if os.path.exists(parents_path):
                parents_df = pd.read_csv(parents_path)
                parent_only_fields = [col for col in parents_df.columns if col != 'sku']
                
                # Normalize column names by removing 'fields.' prefix for merging
                parents_df.columns = [col.replace('fields.', '') if col.startswith('fields.') else col for col in parents_df.columns]
                parent_only_fields = [col.replace('fields.', '') for col in parent_only_fields]
                
                # For parent rows in result_df, add parent-only fields
                if not parent_only_fields:
                    logging.warning("No parent-only fields found")
                else:
                    # Merge only for parent rows if group column exists
                    if 'group' in result_df.columns:
                        # Get group_skus value for parent rows
                        parent_skus = result_df.loc[result_df['group'] == 'product', 'sku']
                        
                        # Filter parents_df to only include those SKUs
                        filtered_parents = parents_df[parents_df['sku'].isin(parent_skus)]
                        
                        # Merge parent-only fields for parent rows
                        for parent_sku in parent_skus:
                            if parent_sku in filtered_parents['sku'].values:
                                parent_row = filtered_parents[filtered_parents['sku'] == parent_sku]
                                for field in parent_only_fields:
                                    if field in parent_row.columns:
                                        value = parent_row[field].iloc[0]
                                        result_df.loc[result_df['sku'] == parent_sku, field] = value
            
            # Add the pricing and other specified fields if not already in the dataframe
            specified_fields = ['pricing_it', 'pricing_fr', 'fields.pac', 'fields.pac', 'fields.pac', 'fields.wei', 'fields.pac', 'fields.abn', 'fields.targ']
            for field in specified_fields:
                if field not in result_df.columns:
                    result_df[field] = ""
            
            # Make sure to place 'pricing_it', 'pricing_fr', etc. right after UPC/barcode
            barcode_col = None
            if 'variant.barcode' in result_df.columns:
                barcode_col = 'variant.barcode'
            elif 'barcode' in result_df.columns:
                barcode_col = 'barcode'
            
            if barcode_col:
                # Create a new order of columns with specified fields after the barcode
                col_order = list(result_df.columns)
                barcode_pos = col_order.index(barcode_col)
                
                # Remove specified fields from current position
                for field in specified_fields:
                    if field in col_order:
                        col_order.remove(field)
                
                # Insert specified fields after barcode
                for i, field in enumerate(reversed(specified_fields)):
                    if field in result_df.columns:
                        col_order.insert(barcode_pos + 1, field)
                
                # Reorder the dataframe
                result_df = result_df[col_order]
        
        except Exception as e:
            logging.error(f"Error adding additional fields: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
        
        # Save the file in Mike's Way format
        output_file = os.path.join(output_dir, 'MikesWay.csv')
        result_df.to_csv(output_file, index=False)
        logging.info(f"Successfully saved Mike's Way format to {output_file}")

        return True

    except Exception as e:
        logging.error(f"Error processing data: {str(e)}")
        import traceback
        error_traceback = traceback.format_exc()
        logging.error(error_traceback)

        # Create error file for debugging
        try:
            # Ensure output directory exists
            if not os.path.exists('./output'):
                os.makedirs('./output')

            with open('./output/mikesway_error.log', 'w') as f:
                f.write(f"Error: {str(e)}\n\n{error_traceback}")

            with open('./mikesway_error.log', 'w') as f:
                f.write(f"Error: {str(e)}\n\n{error_traceback}")
        except:
            pass

        return False

if __name__ == "__main__":
    # This script can be run standalone if needed
    if os.path.exists('./input') and len(os.listdir('./input')) == 1:
        input_file = os.path.join('./input', os.listdir('./input')[0])
        process_mikes_way(input_file)
    else:
        logging.error("No input file found in ./input directory")