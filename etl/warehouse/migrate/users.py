"""for functions realted too users"""
from typing import Dict, Tuple

from .transform_json import get_city, get_country


def transform_user(row: Dict) -> Tuple:
    """Transform / Load for User Dimension"""
    country = get_country(row["country"])
    city = get_city(row["city"])
    gender = (
        row["gender"] if (row["gender"] is not None) and row["gender"] else "Unknown"
    )
    roles = row["roles"][0] if row["roles"] else "Unknown"
    return (
        row["user_id"],
        row["dob"],
        country,
        city,
        gender,
        row["user_type"],
        roles,
        row["mailing_list"],
        row["when"],
    )
