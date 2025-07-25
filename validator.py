import re
import dns.resolver
import smtplib
import pandas as pd
import os
import pdfplumber
from concurrent.futures import ThreadPoolExecutor

ALLOWED_DOMAINS = {'gmail.com', 'yahoo.com', 'outlook.com'}

def is_valid_format(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@(gmail\.com|yahoo\.com|outlook\.com)$'
    return re.match(pattern, email) is not None

domain_cache = {}

def mx_and_smtp_check(email):
    try:
        domain = email.split('@')[1]
        if domain in domain_cache:
            return domain_cache[domain]

        mx_records = dns.resolver.resolve(domain, 'MX')
        mx_record = str(mx_records[0].exchange)

        server = smtplib.SMTP(timeout=10)
        server.connect(mx_record)
        server.helo()
        server.mail('check@yourdomain.com')
        code, _ = server.rcpt(email)
        server.quit()

        is_valid = code == 250
        domain_cache[domain] = is_valid
        return is_valid
    except Exception:
        domain_cache[domain] = False
        return False

def extract_emails_from_pdf(file_path):
    emails = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                found = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
                emails.extend(found)
    return list(set(emails))

def validate_emails(file_path, session_id, chunk_size=5000, output_folder='outputs'):
    ext = os.path.splitext(file_path)[1].lower()
    all_results = []
    seen_emails = set()

    if ext == '.csv':
        reader = pd.read_csv(file_path, chunksize=chunk_size, encoding='utf-8', errors='replace')
    elif ext in ['.xls', '.xlsx']:
        df = pd.read_excel(file_path, sheet_name=0)
        reader = [df]
    elif ext == '.pdf':
        email_list = extract_emails_from_pdf(file_path)
        df = pd.DataFrame({'email': email_list})
        reader = [df]
    else:
        raise ValueError("Unsupported file format. Please upload .csv, .xlsx, or .pdf")

    for chunk_emails in reader:
        # ✅ Flexible column name detection
        email_col = next((col for col in chunk_emails.columns if 'email' in col.lower()), None)
        if not email_col:
            raise ValueError("No column found that contains the word 'email'.")

        # ✅ Clean email data
        chunk_emails['email'] = chunk_emails[email_col].astype(str).str.strip().str.lower()
        chunk_emails = chunk_emails[['email']].drop_duplicates()
        chunk_emails = chunk_emails[~chunk_emails['email'].isin(seen_emails)]
        seen_emails.update(chunk_emails['email'].tolist())

        # ✅ Filter by allowed domains
        chunk_emails = chunk_emails[chunk_emails['email'].apply(
            lambda e: e.split('@')[-1] in ALLOWED_DOMAINS)]

        # ✅ Format check
        chunk_emails['Format_Valid'] = chunk_emails['email'].apply(is_valid_format)

        # ✅ SMTP check
        valid_emails = chunk_emails[chunk_emails['Format_Valid']]['email'].tolist()
        with ThreadPoolExecutor(max_workers=20) as executor:
            smtp_results = list(executor.map(mx_and_smtp_check, valid_emails))

        chunk_emails.loc[chunk_emails['Format_Valid'], 'SMTP_Valid'] = smtp_results
        chunk_emails['Final_Status'] = chunk_emails.apply(
            lambda row: 'Valid' if row['Format_Valid'] and row.get('SMTP_Valid', False) else 'Invalid',
            axis=1)

        all_results.append(chunk_emails)

    final_df = pd.concat(all_results, ignore_index=True)
    valid_df = final_df[final_df['Final_Status'] == 'Valid']
    invalid_df = final_df[final_df['Final_Status'] == 'Invalid']

    valid_path = f"{output_folder}/valid_emails_{session_id}.csv"
    invalid_path = f"{output_folder}/invalid_emails_{session_id}.csv"

    valid_df.to_csv(valid_path, index=False)
    invalid_df.to_csv(invalid_path, index=False)

    return valid_path, invalid_path, len(final_df), len(valid_df), len(invalid_df)
