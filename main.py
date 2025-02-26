from flask import Flask, render_template, request, send_file, url_for
import os
import shutil
import zipfile
import subprocess

app = Flask(__name__)

# Set absolute paths for templates and static files
app.template_folder = os.path.abspath('templates')
app.static_folder = os.path.abspath('static')

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

@app.route('/portfolio')
def portfolio():
    return render_template('portfolio.html')

@app.route('/portfolio/')
def portfolio_slash():
    return render_template('portfolio.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return {'status': 'error', 'log': ['No file uploaded']}

    file = request.files['file']
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
    return send_file('processed_files.zip', as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)