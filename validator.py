import re
import dns.resolver
import smtplib
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from utils import extract_emails_from_file

# ✅ Format check
def is_valid_format(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

# ✅ MX + SMTP with caching
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

# ✅ Main function with chunked processing
def validate_emails(file_path, chunk_size=5000):
    all_results = []
    seen_emails = set()

    for chunk_emails in pd.read_csv(file_path, chunksize=chunk_size):
        if 'email' not in chunk_emails.columns.str.lower().tolist():
            continue

        email_col = [col for col in chunk_emails.columns if 'email' in col.lower()][0]
        chunk_emails = chunk_emails.dropna(subset=[email_col])
        chunk_emails['email'] = chunk_emails[email_col].str.strip().str.lower()
        chunk_emails = chunk_emails[['email']].drop_duplicates()

        chunk_emails = chunk_emails[~chunk_emails['email'].isin(seen_emails)]
        seen_emails.update(chunk_emails['email'].tolist())

        chunk_emails['Format_Valid'] = chunk_emails['email'].apply(is_valid_format)

        valid_emails = chunk_emails[chunk_emails['Format_Valid']]['email'].tolist()
        with ThreadPoolExecutor(max_workers=20) as executor:
            smtp_results = list(executor.map(mx_and_smtp_check, valid_emails))

        chunk_emails.loc[chunk_emails['Format_Valid'], 'SMTP_Valid'] = smtp_results
        chunk_emails['Final_Status'] = chunk_emails.apply(
            lambda row: 'Valid' if row['Format_Valid'] and row.get('SMTP_Valid', False) else 'Invalid', axis=1)

        all_results.append(chunk_emails)

    final_df = pd.concat(all_results, ignore_index=True)
    final_df[final_df['Final_Status'] == 'Valid'].to_csv('valid_emails.csv', index=False)
    final_df[final_df['Final_Status'] == 'Invalid'].to_csv('invalid_emails.csv', index=False)
