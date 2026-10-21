"""PII masking utilities for NammaMart customer data.
Compliant with India's Digital Personal Data Protection (DPDP) Act 2023.
"""

import pandas as pd


def mask_email(email) -> str:
    """Mask email address while retaining domain and leading prefix.
    
    Example:
        priya.rao@example.com -> pr***@example.com
    """
    if pd.isna(email) or not isinstance(email, str):
        return "masked_user@unknown.com"
    
    cleaned = email.strip()
    if "@" not in cleaned:
        return "masked_user@unknown.com"
    
    parts = cleaned.split("@")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return "masked_user@unknown.com"
    
    local_part, domain = parts
    if len(local_part) >= 2:
        masked_local = f"{local_part[:2]}***"
    else:
        masked_local = f"{local_part[:1]}***"
        
    return f"{masked_local}@{domain}"


def mask_phone(phone) -> str:
    """Mask 10-digit phone number, preserving the first 2 and last 2 digits.
    
    Example:
        9876543210 -> 98******10
    """
    if pd.isna(phone):
        return "99******99"
    
    phone_str = str(phone).strip()
    # Handle float conversions from CSV like '9876543210.0'
    if phone_str.endswith(".0"):
        phone_str = phone_str[:-2]
        
    # Extract only digits
    digits = "".join(ch for ch in phone_str if ch.isdigit())
    
    if len(digits) == 10:
        return f"{digits[:2]}******{digits[-2:]}"
    
    return "99******99"
