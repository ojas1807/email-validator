import re
import dns.resolver
import smtplib
import pandas as pd
import os
import pdfplumber
from concurrent.futures import ThreadPoolExecutor

# Allowed domains for validation
ALLOWED_DOMAINS = {'gmail.com', 'yahoo.com', 'outlook.com'}
COMMON_PROVIDERS = {'gmail.com', 'yahoo.com', 'outlook.com'}  # For skipping SMTP verification

def is_valid_format(email):
    """
    Check if email has a valid format (simplified regex).
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

email_cache = {}

def mx_and_smtp_check(email):
    """
    Check existence of email by SMTP RCPT TO command.
    Cache results per email.
    """
    if email in email_cache:
        return email_cache[email]

    try:
        domain = email.split('@')[1]
        mx_records = dns.resolver.resolve(domain, 'MX')
        mx_record = str(mx_records[0].exchange).rstrip('.')

        server = smtplib.SMTP(timeout=10)
        server.connect(mx_record)
        server.helo('yourdomain.com')  # Replace 'yourdomain.com' with your actual domain
        server.mail('check@yourdomain.com')  # Replace with your email or domain-based sender
        code, _ = server.rcpt(email)
        server.quit()

        is_valid = code == 250
        email_cache[email] = is_valid
        return is_valid
    except Exception:
        email_cache[email] = False
        return False

def extract_emails_from_pdf(file_path):
    """
    Extract emails from PDF text using regex.
    """
    emails = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                found = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
                emails.extend(found)
    return list(set(emails))

def validate_emails(file_path, session_id, chunk_size=5000, output_folder='outputs'):
    """
    Validate emails - skip SMTP for common providers, check domain is allowed.

    Args:
        file_path (str): Path to input file (.csv, .xlsx, .pdf)
        session_id (str): Unique session id for output filenames
        chunk_size (int): Chunk size for CSV reading
        output_folder (str): Output folder path

    Returns:
        (valid_path, invalid_path, total, valid_count, invalid_count)
    """

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
        # For PDFs, the dataframe already has 'email' column
        if ext != '.pdf':
            email_col = next((col for col in chunk_emails.columns if 'email' in col.lower()), None)
            if not email_col:
                raise ValueError("No column found that contains the word 'email'.")
            emails_series = chunk_emails[email_col].astype(str).str.strip().str.lower()
        else:
            emails_series = chunk_emails['email'].astype(str).str.strip().str.lower()

        # Deduplicate and exclude already processed emails
        emails_series = emails_series[~emails_series.isin(seen_emails)].drop_duplicates()
        seen_emails.update(emails_series.tolist())

        chunk_df = pd.DataFrame({'email': emails_series})

        # Validate email format
        chunk_df['Format_Valid'] = chunk_df['email'].apply(is_valid_format)

        # Extract domain and split by common vs uncommon
        chunk_df['domain'] = chunk_df['email'].apply(lambda x: x.split('@')[1])
        common_emails = chunk_df[chunk_df['domain'].isin(COMMON_PROVIDERS)].copy()
        uncommon_emails = chunk_df[~chunk_df['domain'].isin(COMMON_PROVIDERS)].copy()

        # For common providers, skip SMTP check; Set SMTP_Valid = Format_Valid
        common_emails['SMTP_Valid'] = common_emails['Format_Valid']

        # For uncommon domains, run SMTP check only on format-valid emails
        if not uncommon_emails.empty:
            emails_to_check = uncommon_emails[uncommon_emails['Format_Valid']]['email'].tolist()
            with ThreadPoolExecutor(max_workers=20) as executor:
                smtp_results = list(executor.map(mx_and_smtp_check, emails_to_check))
            uncommon_emails.loc[uncommon_emails['Format_Valid'], 'SMTP_Valid'] = smtp_results
            uncommon_emails['SMTP_Valid'] = uncommon_emails['SMTP_Valid'].fillna(False)

        # Combine all emails back
        final_chunk = pd.concat([common_emails, uncommon_emails])

        # Final domain validation - domain must be in allowed list
        final_chunk['Domain_Valid'] = final_chunk['domain'].isin(ALLOWED_DOMAINS)

        # Determine final validity: must have valid format, SMTP check pass, and allowed domain
        final_chunk['Final_Status'] = final_chunk.apply(
            lambda row: 'Valid' if row['Format_Valid'] and row['SMTP_Valid'] and row['Domain_Valid'] else 'Invalid',
            axis=1
        )

        all_results.append(final_chunk)

    # Concatenate all chunks
    final_df = pd.concat(all_results, ignore_index=True)

    valid_df = final_df[final_df['Final_Status'] == 'Valid']
    invalid_df = final_df[final_df['Final_Status'] == 'Invalid']

    os.makedirs(output_folder, exist_ok=True)
    valid_path = os.path.join(output_folder, f"valid_emails_{session_id}.csv")
    invalid_path = os.path.join(output_folder, f"invalid_emails_{session_id}.csv")

    valid_df.to_csv(valid_path, index=False)
    invalid_df.to_csv(invalid_path, index=False)

    return valid_path, invalid_path, len(final_df), len(valid_df), len(invalid_df)
