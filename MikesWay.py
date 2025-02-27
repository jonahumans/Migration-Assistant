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
            # Just copy the input file for now
            result_df = df.copy()

            # Add group column
            if 'group' not in result_df.columns:
                result_df['group'] = 'variant'

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