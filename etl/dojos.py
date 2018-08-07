"""for functions realted too dojos"""
from typing import Dict, Tuple


def transform_dojo(row: Dict) -> Tuple:
    """Transform / Load for Dojo Dimension"""

    # These ones are ugly the data base stores them in json originally
    country = row['country']['countryName'] if (
        row['country'] is not None) and row['country'] else 'Unknown'
    county = row['county']['toponymName'] if (
        row['county'] is not None) and row['county'] else 'Unknown'
    city = row['place']['name'] if (
        row['city'] is not None) and row['city'] else 'Unknown'
    state = row['state']['toponymName'] if (
        row['state'] is not None) and row['state'] else 'Unknown'

    expected_attendees = row['expected_attendees'] if (
        row['expected_attendees'] is not None
    ) else 0  # Maybe something other than 0????
    inactive = 1 if (row['stage'] == 4) else 0
    is_eb = 1 if row['eventbrite_token'] and row['eventbrite_wh_id'] else 0

    return (row['id'], row['created'], row['verified_at'], row['stage'],
            country, city, county, state, row['continent'],
            row['tao_verified'], expected_attendees, row['verified'],
            row['deleted'], inactive, row['inactive_at'], is_eb,
            row['dojo_lead_id'])


def link_users(row: Dict) -> Tuple:
    """link users too dojos"""
    return (row['id'], row['user_id'], row['dojo_id'], row['user_type'])
