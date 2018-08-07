"""function related to events"""

from typing import Dict, Tuple


def transform_event(row: Dict) -> Tuple:
    """ Transform / Load for Event Dimension"""
    # For fields which zen prod dbs are storing as json
    country = row['country'] if (
        row['country'] is not None) and row['country'] else None
    country = country['countryName'] if country is not None else 'Unknown'

    is_eb = row['eventbrite_id'] is not None

    city = row['city'] if (row['city'] is not None) and row['city'] else None
    city = 'Unkown' if city is None else city[
        'toponymName'] if 'toponymName' in city else city['nameWithHierarchy']

    return (row['id'], row['recurring_type'], country, city, row['created_at'],
            row['type'], row['dojo_id'], row['public'], row['status'], is_eb,
            row['start_time'])
