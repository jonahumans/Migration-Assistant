from flask import Flask, render_template, request, send_file, url_for
import os
import shutil
import zipfile
import subprocess

app = Flask(__name__)

# Set absolute paths for templates and static files
current_dir = os.path.dirname(os.path.abspath(__file__))
app.template_folder = os.path.join(current_dir, 'templates')
app.static_folder = os.path.join(current_dir, 'static')

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
@app.route('/about')
@app.route('/about/')
@app.route('/portfolio')
@app.route('/portfolio/')
def about():
    return render_template('about.html')

@app.route('/tool')
def index():
    logger.debug(f"Template folder: {app.template_folder}")
    logger.debug(f"Looking for about.html in: {os.path.join(app.template_folder, 'about.html')}")
    return render_template('about.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return {'status': 'error', 'log': ['No file uploaded']}

    file = request.files['file']
    if file.filename == '':
        return {'status': 'error', 'log': ['No selected file']}

    if not file.filename.endswith('.csv'):
        return {'status': 'error', 'log': ['Invalid file type - please upload a CSV file']}

    log_messages = []
    try:
        # Clear directories
        for directory in ['input', 'output']:
            for f in os.listdir(directory):
                try:
                    os.remove(os.path.join(directory, f))
                except Exception as e:
                    logger.error(f"Error clearing {directory}: {str(e)}")
                    return {'status': 'error', 'log': [f'Error preparing directories: {str(e)}']}

        # Save new file
        file_path = os.path.join('input', file.filename)
        try:
            file.save(file_path)
            logger.info(f"File saved successfully to {file_path}")
            log_messages.append("✓ File uploaded successfully")
        except Exception as e:
            logger.error(f"Error saving file: {str(e)}")
            return {'status': 'error', 'log': [f'Error saving file: {str(e)}']}

        scripts = ["addvariants.py", "parentattributesonvarients.py", "variantattributes.py"]
        for script in scripts:
            log_messages.append(f"Running {script}...")
            try:
                result = subprocess.run(
                    ['python', script], 
                    capture_output=True, 
                    text=True,
                    timeout=300  # 5 minute timeout
                )
                if result.returncode == 0:
                    logger.info(f"{script} completed successfully")
                    log_messages.append(f"✓ {script} completed successfully")
                    if result.stdout:
                        log_messages.append(result.stdout)
                else:
                    error_msg = f"Error in {script}: {result.stderr}"
                    logger.error(error_msg)
                    return {'status': 'error', 'log': log_messages + [error_msg]}
            except subprocess.TimeoutExpired:
                error_msg = f"Timeout running {script} - process took too long"
                logger.error(error_msg)
                return {'status': 'error', 'log': log_messages + [error_msg]}
            except Exception as e:
                error_msg = f"Error running {script}: {str(e)}"
                logger.error(error_msg)
                return {'status': 'error', 'log': log_messages + [error_msg]}

        # Create zip file
        try:
            with zipfile.ZipFile('processed_files.zip', 'w') as zipf:
                for root, dirs, files in os.walk('output'):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, 'output')
                        zipf.write(file_path, arcname)
            log_messages.append("✓ Files packaged successfully")
        except Exception as e:
            error_msg = f"Error creating zip file: {str(e)}"
            logger.error(error_msg)
            return {'status': 'error', 'log': log_messages + [error_msg]}

        logger.info("Processing completed successfully")
        return {'status': 'success', 'log': log_messages + ["All processing complete. Files ready for download."]}

    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        return {'status': 'error', 'log': log_messages + [error_msg]}

@app.route('/download')
def download():
    return send_file('processed_files.zip', as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)