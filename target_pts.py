
import os
import pandas as pd
import logging

# Set input and output directories
input_directory = './input/'
output_directory = './output/'

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Starting target PTS data extraction")
    
    # Check if output files exist
    parents_file = os.path.join(output_directory, 'parents.csv')
    variants_file = os.path.join(output_directory, 'parentattributesonvarients.csv')
    
    if not os.path.exists(parents_file) or not os.path.exists(variants_file):
        logging.error("Required input files missing. Please run parentattributesonvarients.py first.")
        return
    
    # Load the CSV files
    try:
        parents_df = pd.read_csv(parents_file)
        variants_df = pd.read_csv(variants_file)
        
        logging.info(f"Loaded {len(parents_df)} parent records and {len(variants_df)} variant records")
    except Exception as e:
        logging.error(f"Error loading CSV files: {str(e)}")
        return
    
    # Initialize the target dataframe with columns we need
    target_cols = ['sku', 'fields.target_posting_template', 'fields.target_listing_action']
    target_df = pd.DataFrame(columns=target_cols)
    
    # Extract target information from parents
    if 'fields.target_posting_template' in parents_df.columns or 'fields.target_listing_action' in parents_df.columns:
        parents_subset = parents_df[['sku'] + [col for col in target_cols[1:] if col in parents_df.columns]]
        logging.info(f"Found {len(parents_subset)} parent records with target information")
        target_df = pd.concat([target_df, parents_subset], ignore_index=True)
    
    # Extract target information from variants
    if 'fields.target_posting_template' in variants_df.columns or 'fields.target_listing_action' in variants_df.columns:
        variants_subset = variants_df[['sku'] + [col for col in target_cols[1:] if col in variants_df.columns]]
        logging.info(f"Found {len(variants_subset)} variant records with target information")
        target_df = pd.concat([target_df, variants_subset], ignore_index=True)
    
    # Fill missing columns if they don't exist in one of the files
    for col in target_cols:
        if col not in target_df.columns:
            target_df[col] = None
    
    # Ensure all required columns are present
    target_df = target_df[target_cols]
    
    # Remove duplicate SKUs, keeping the first occurrence
    target_df = target_df.drop_duplicates(subset=['sku'], keep='first')
    
    # Create a new 'pts' column with deduplicated target_posting_templates
    logging.info("Creating 'pts' column with deduplicated target_posting_templates")
    
    # Create a copy of the target_posting_template column for the pts column
    if 'fields.target_posting_template' in target_df.columns:
        target_df['pts'] = target_df['fields.target_posting_template']
        
        # Remove duplicates by creating a set of unique values for each cell
        def dedup_templates(value):
            if pd.isna(value) or value == '':
                return value
            try:
                # Try to split by comma if it's a string with multiple values
                templates = [t.strip() for t in str(value).split(',')]
                # Remove duplicates by converting to set and back to list
                unique_templates = []
                for template in templates:
                    if template and template not in unique_templates:
                        unique_templates.append(template)
                return ','.join(unique_templates)
            except:
                return value
        
        target_df['pts'] = target_df['pts'].apply(dedup_templates)
    else:
        # If the column doesn't exist, create an empty pts column
        target_df['pts'] = ''
    
    # Reorder columns to place 'pts' as the fourth column
    cols = list(target_df.columns)
    cols.remove('pts')
    new_cols = cols[:3] + ['pts'] + cols[3:]
    target_df = target_df[new_cols]
    
    # Save the output
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        
    output_file = os.path.join(output_directory, 'target_pts.csv')
    target_df.to_csv(output_file, index=False)
    
    logging.info(f"Successfully created target PTS file with {len(target_df)} records. Saved to {output_file}")

if __name__ == "__main__":
    main()
