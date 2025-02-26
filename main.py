from flask import Flask, render_template, request, send_file
import os
import shutil
import zipfile
import subprocess

app = Flask(__name__)

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
        return 'No file part'

    file = request.files['file']

    if file.filename == '':
        return 'No selected file'

    if file and file.filename.endswith('.csv'):
        # Clear input directory
        for f in os.listdir('input'):
            os.remove(os.path.join('input', f))

        # Clear output directory
        for f in os.listdir('output'):
            os.remove(os.path.join('output', f))

        # Save new file
        file_path = os.path.join('input', file.filename)
        file.save(file_path)

        # Run the Python scripts
        log_messages = []

        try:
            # Run each script individually to catch errors
            scripts = ["addvariants.py", "parentattributesonvarients.py", "variantattributes.py"]

            for script in scripts:
                log_messages.append(f"Running {script}...")
                result = subprocess.run(['python', script], capture_output=True, text=True)
                if result.returncode == 0:
                    log_messages.append(f"✓ {script} completed successfully.")
                    log_messages.append(result.stdout)
                else:
                    log_messages.append(f"✗ Error in {script}:")
                    log_messages.append(result.stderr)
                    break

            # Create zip file of outputs
            zip_path = 'processed_files.zip'
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for root, dirs, files in os.walk('output'):
                    for file in files:
                        zipf.write(os.path.join(root, file), 
                                  os.path.relpath(os.path.join(root, file), 
                                                 os.path.join('output', '..')))

            log_messages.append("All processing complete. Files ready for download.")
            return {'status': 'success', 'log': log_messages}

        except Exception as e:
            log_messages.append(f"Error: {str(e)}")
            return {'status': 'error', 'log': log_messages}

    return 'Invalid file type'

@app.route('/download')
def download():
    return send_file('processed_files.zip', as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)