import os
import pandas as pd
import re
import PyPDF2

def extract_emails_from_text(text):
    pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    return re.findall(pattern, text)

def extract_emails_from_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == '.csv':
        df = pd.read_csv(file_path, dtype=str)
    elif ext in ['.xls', '.xlsx']:
        df = pd.read_excel(file_path, dtype=str)
    elif ext == '.txt':
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        return extract_emails_from_text(content)
    elif ext == '.pdf':
        text = ''
        with open(file_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            for page in reader.pages:
                text += page.extract_text() or ''
        return extract_emails_from_text(text)
    else:
        raise ValueError("❌ Unsupported file format.")

    email_col = next((col for col in df.columns if 'email' in col.lower()), None)
    if email_col:
        return df[email_col].dropna().str.strip().unique().tolist()
    else:
        text_data = df.fillna('').apply(lambda row: ' '.join(row.values), axis=1)
        return extract_emails_from_text(' '.join(text_data))
