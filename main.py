from flask import Flask, render_template, request, send_file, url_for
import os
import shutil
import zipfile
import subprocess

app = Flask(__name__)

# Set template and static folders relative to app root
app = Flask(__name__, 
            template_folder='templates',
            static_folder='static')

# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create necessary directories
if not os.path.exists('input'):
    os.makedirs('input')
if not os.path.exists('output'):
    os.makedirs('output')
if not os.path.exists('static'):
    os.makedirs('static')
if not os.path.exists('templates'):
    os.makedirs('templates')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/about')
@app.route('/about/')
@app.route('/portfolio')
@app.route('/portfolio/')
def about():
    logger.debug(f"Template folder: {app.template_folder}")
    logger.debug(f"Looking for about.html in: {os.path.join(app.template_folder, 'about.html')}")
    return render_template('about.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return {'status': 'error', 'log': ['No file uploaded']}, 400
        file = request.files['file']
    except Exception as e:
        return {'status': 'error', 'log': [str(e)]}, 400
        
    if file.filename == '':
        return {'status': 'error', 'log': ['No selected file']}

    if file and file.filename.endswith('.csv'):
        # Clear directories
        for directory in ['input', 'output']:
            for f in os.listdir(directory):
                os.remove(os.path.join(directory, f))

        # Save new file
        file_path = os.path.join('input', file.filename)
        file.save(file_path)

        log_messages = []
        try:
            scripts = ["addvariants.py", "parentattributesonvarients.py", "variantattributes.py"]
            for script in scripts:
                log_messages.append(f"Running {script}...")
                result = subprocess.run(['python', script], capture_output=True, text=True)
                if result.returncode == 0:
                    log_messages.append(f"✓ {script} completed successfully.")
                    if result.stdout:
                        log_messages.append(result.stdout)
                else:
                    log_messages.append(f"✗ Error in {script}:")
                    log_messages.append(result.stderr)
                    return {'status': 'error', 'log': log_messages}

            # Create zip file
            with zipfile.ZipFile('processed_files.zip', 'w') as zipf:
                for root, dirs, files in os.walk('output'):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, 'output')
                        zipf.write(file_path, arcname)

            log_messages.append("All processing complete. Files ready for download.")
            return {'status': 'success', 'log': log_messages}

        except Exception as e:
            log_messages.append(f"Error: {str(e)}")
            return {'status': 'error', 'log': log_messages}

    return {'status': 'error', 'log': ['Invalid file type']}

@app.route('/download')
def download():
    output_files = [
        'input/processed_data.csv',
        'output/group_skus.csv', 
        'output/parent_columns.txt',
        'output/parents.csv',
        'output/variant_columns.txt',
        'output/variantattributes.csv',
        'output/parentattributesonvarients.csv'
    ]
    
    # Create a zip of available files
    with zipfile.ZipFile('output_files.zip', 'w') as zipf:
        files_found = False
        for file in output_files:
            if os.path.exists(file):
                zipf.write(file, os.path.basename(file))
                files_found = True
        
        # If no files were found, add a README
        if not files_found:
            zipf.writestr('README.txt', 'No output files were generated during processing.')
    
    # Force cleanup of all files
    for directory in ['input', 'output']:
        for file in os.listdir(directory):
            file_path = os.path.join(directory, file)
            try:
                if os.path.isfile(file_path):
                    # Force close any open file handles
                    import gc
                    gc.collect()
                    os.chmod(file_path, 0o777)  # Give full permissions
                    os.remove(file_path)
                    logging.info(f"Deleted {file_path}")
            except Exception as e:
                logging.error(f"Failed to delete {file_path}: {e}")
                try:
                    import signal
                    os.kill(os.getpid(), signal.SIGINT)  # Force interrupt to release file handles
                    os.remove(file_path)
                except:
                    pass
    
    # Send the zip file
    response = send_file('output_files.zip', as_attachment=True)
    
    # Force cleanup of zip file
    try:
        os.remove('output_files.zip')
        logging.info("Deleted output_files.zip")
    except Exception as e:
        logging.error(f"Failed to delete output_files.zip: {e}")
        
    return response

@app.errorhandler(Exception)
def handle_error(e):
    error_msg = str(e)
    status_code = getattr(e, 'code', 500)
    
    # Log the error for debugging
    logging.error(f"Error occurred: {error_msg}")
    
    if status_code == 413:
        error_msg = "File too large. Please upload a smaller file."
    elif status_code == 404:
        error_msg = "Resource not found."
    elif status_code == 400:
        error_msg = "Invalid request. Please check your input."
    
    return {
        'status': 'error',
        'log': [error_msg]
    }, status_code

if __name__ == '__main__':
    # Get port from environment variable or default to 8080
    port = int(os.environ.get('PORT', 8080))
    # In production, disable debug mode and use 0.0.0.0 to accept all incoming connections
    app.run(host='0.0.0.0', port=port, debug=False)