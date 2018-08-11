"""Badge related transformations"""
from typing import Dict, List, Tuple
from uuid import uuid4


def transform_badges(row: Dict) -> List[Tuple]:
    """Transform Original badge object to tuple for insertion"""
    user_id = row["user_id"]
    badges = row["badges"]

    def _transform(element: Dict) -> Tuple:
        issued_on = element.get("assertion", {}).get("issuedOn", None)
        badge_id = str(uuid4())
        return (
            element["id"],
            element["archived"],
            element["type"],
            element["name"],
            badge_id,
            user_id,
            issued_on,
        )

    return list(map(_transform, badges))


def add_badges(rows: List) -> List[Tuple]:
    """add badge to db"""
    return list(map(lambda row: (row["badge_id"], row["user_id"]), rows))
