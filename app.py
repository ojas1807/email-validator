from flask import Flask, request, render_template, send_file
import os
import uuid
from werkzeug.utils import secure_filename
from validator import validate_emails
from werkzeug.exceptions import RequestEntityTooLarge

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'outputs'

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB

@app.errorhandler(RequestEntityTooLarge)
def handle_large_file(e):
    return "File too large. Maximum allowed size is 100MB.", 413

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        file = request.files.get('file')
        if not file:
            return "No file uploaded", 400

        filename = secure_filename(file.filename)
        if not filename:
            return "Invalid file name", 400

        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)

        session_id = uuid.uuid4().hex
        valid_path, invalid_path, total, valid_count, invalid_count = validate_emails(
            file_path, session_id, output_folder=app.config['OUTPUT_FOLDER'])

        return render_template('result.html',
                               valid_file=os.path.basename(valid_path),
                               invalid_file=os.path.basename(invalid_path),
                               total=total,
                               valid_count=valid_count,
                               invalid_count=invalid_count)

    return render_template('index.html')

@app.route('/download/<filename>')
def download_file(filename):
    safe_filename = secure_filename(filename)
    file_path = os.path.join(app.config['OUTPUT_FOLDER'], safe_filename)
    if not os.path.exists(file_path):
        return "File not found", 404
    return send_file(file_path, as_attachment=True)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
