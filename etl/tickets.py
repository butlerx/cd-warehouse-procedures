"""functions related to tickets"""

from typing import Dict, Tuple


def transform_ticket(row: Dict) -> Tuple:
    """transform ticket for warehouse"""
    return (row['ticket_id'], row['type'], row['quantity'], row['deleted'])
