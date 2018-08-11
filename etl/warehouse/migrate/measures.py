"""Get all ids"""
from typing import Dict, Tuple
from uuid import uuid4


def get_id(args: Dict) -> Tuple:
    """Get all ids"""
    return (
        args["dojo_id"],
        args["ticket_id"],
        args["event_id"],
        args["user_id"],
        args["time"],
        args["location_id"],
        str(uuid4()),
        args["badge_id"],
        args["checked_in"],
    )
