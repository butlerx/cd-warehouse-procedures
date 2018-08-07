"""Get all ids"""
import uuid
from typing import Dict, Tuple


def get_id(args: Dict) -> Tuple:
    """Get all ids"""
    return (args['dojo_id'], args['ticket_id'], args['event_id'],
            args['user_id'], args['time'], args['location_id'],
            str(uuid.uuid4()), args['badge_id'], args['checked_in'])
