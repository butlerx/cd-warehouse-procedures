"""function related to events"""

from typing import Dict, Tuple

from transform_json import get_city, get_country


def transform_event(row: Dict) -> Tuple:
    """ Transform / Load for Event Dimension"""
    country = get_country(row['country'])
    is_eb = row['eventbrite_id'] is not None
    city = get_city(row['city'])

    return (row['id'], row['recurring_type'], country, city, row['created_at'],
            row['type'], row['dojo_id'], row['public'], row['status'], is_eb,
            row['start_time'])
