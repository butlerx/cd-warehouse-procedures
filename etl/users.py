"""for functions realted too users"""
from typing import Dict, Tuple


def transform_user(row: Dict) -> Tuple:
    """Transform / Load for User Dimension"""

    # For fields which zen prod dbs are storing as json
    country = row['country'] if (
        row['country'] is not None) and row['country'] else None
    country = country['countryName'] if country is not None else 'Unknown'

    city = row['city'] if (row['city'] is not None) and row['city'] else None
    city = city['nameWithHierarchy'] if city is not None else 'Unknown'

    gender = row['gender'] if (
        row['gender'] is not None) and row['gender'] else 'Unknown'

    roles = row['roles'][0] if row['roles'] else 'Unknown'

    return (row['user_id'], row['dob'], country, city, gender,
            row['user_type'], roles, row['mailing_list'], row['when'])
