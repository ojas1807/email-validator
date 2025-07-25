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

def smtp_check(email):
    try:
        domain = email.split('@')[1]
        if domain in domain_cache:
            return domain_cache[domain]

        mx_records = dns.resolver.resolve(domain, 'MX')
        mx_record = str(mx_records[0].exchange)

        server = smtplib.SMTP(timeout=10)
        server.connect(mx_record)
        server.helo()
        server.mail('validator@yourdomain.com')  # Fake sender
        code, _ = server.rcpt(email)
        server.quit()

        result = code == 250
        domain_cache[domain] = result
        return result
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

    for chunk in reader:
        email_col = next((col for col in chunk.columns if 'email' in col.lower()), None)
        if not email_col:
            raise ValueError("No column found that contains the word 'email'.")

        chunk['email'] = chunk[email_col].astype(str).str.strip().str.lower()
        chunk = chunk[['email']].drop_duplicates()
        chunk = chunk[~chunk['email'].isin(seen_emails)]
        seen_emails.update(chunk['email'].tolist())

        # Format + domain filtering
        chunk['Format_Valid'] = chunk['email'].apply(is_valid_format)
        chunk['Domain_Allowed'] = chunk['email'].apply(lambda e: e.split('@')[-1] in ALLOWED_DOMAINS)

        smtp_emails = chunk[chunk['Format_Valid'] & chunk['Domain_Allowed']]['email'].tolist()

        with ThreadPoolExecutor(max_workers=10) as executor:
            smtp_results = list(executor.map(smtp_check, smtp_emails))

        chunk['SMTP_Valid'] = False
        chunk.loc[chunk['Format_Valid'] & chunk['Domain_Allowed'], 'SMTP_Valid'] = smtp_results

        chunk['Final_Status'] = chunk.apply(
            lambda row: 'Valid' if row['Format_Valid'] and row['Domain_Allowed'] and row['SMTP_Valid'] else 'Invalid',
            axis=1
        )

        all_results.append(chunk)

    final_df = pd.concat(all_results, ignore_index=True)
    valid_df = final_df[final_df['Final_Status'] == 'Valid']
    invalid_df = final_df[final_df['Final_Status'] == 'Invalid']

    valid_path = f"{output_folder}/valid_emails_{session_id}.csv"
    invalid_path = f"{output_folder}/invalid_emails_{session_id}.csv"

    os.makedirs(output_folder, exist_ok=True)
    valid_df.to_csv(valid_path, index=False)
    invalid_df.to_csv(invalid_path, index=False)

    return valid_path, invalid_path, len(final_df), len(valid_df), len(invalid_df)
