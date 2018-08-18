"""function related to leads"""
from typing import Dict, Tuple


def transform_lead(row: Dict) -> Tuple:
    """ Transform / Load for Lead Dimension"""
    has_mentors = (
        True
        if (row["has_mentors"] == "exists")
        else False
        if (row["has_mentors"] == "solo")
        else None
    )
    return (
        row["id"],
        row["user_id"],
        row["confidence_coding"],
        row["confidence_mentoring"],
        row["venue_type"],
        row["alternative_venue_type"],
        row["referer"],
        row["alternative_referer"],
        has_mentors,
        bool(row["mentor_youth_workers"]),
        bool(row["mentor_parents"]),
        bool(row["mentor_it_professionals"]),
        bool(row["mentor_venue_staff"]),
        bool(row["mentor_students"]),
        bool(row["mentor_teachers"]),
        bool(row["mentor_youth_u18"]),
        row["mentor_other"],
        row["created_at"],
        row["updated_at"],
        row["completed_at"],
    )
