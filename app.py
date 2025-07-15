from flask import Flask, request, render_template, send_file
import os
from werkzeug.utils import secure_filename
from validator import validate_emails
from werkzeug.exceptions import RequestEntityTooLarge

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max upload size

@app.errorhandler(RequestEntityTooLarge)
def handle_large_file(e):
    return "File too large. Maximum allowed size is 100MB.", 413

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        file = request.files['file']
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        validate_emails(filepath)

        return render_template('result.html',
                               valid_file='valid_emails.csv',
                               invalid_file='invalid_emails.csv')

    return render_template('index.html')

@app.route('/download/<filename>')
def download_file(filename):
    return send_file(filename, as_attachment=True)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
