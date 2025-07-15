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

# ✅ Main function
def validate_emails(file_path):
    emails = extract_emails_from_file(file_path)
    emails = list(set(emails))  # deduplicate

    df = pd.DataFrame({'email': emails})
    df['Format_Valid'] = df['email'].apply(is_valid_format)

    emails_to_check = df[df['Format_Valid']]['email'].tolist()
    with ThreadPoolExecutor(max_workers=20) as executor:
        smtp_results = list(executor.map(mx_and_smtp_check, emails_to_check))

    df.loc[df['Format_Valid'], 'SMTP_Valid'] = smtp_results

    df['Final_Status'] = df.apply(
        lambda row: 'Valid' if row['Format_Valid'] and row['SMTP_Valid'] else 'Invalid',
        axis=1
    )

    df[df['Final_Status'] == 'Valid'].to_csv('valid_emails.csv', index=False)
    df[df['Final_Status'] == 'Invalid'].to_csv('invalid_emails.csv', index=False)
