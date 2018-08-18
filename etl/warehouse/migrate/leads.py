"""function related to leads"""
from typing import Dict, Optional, Tuple


class Lead:
    """Lead object"""

    def __init__(self, row: Dict) -> None:
        self._data = row
        self.has_mentors: Optional[bool] = (
            True
            if (self._data["has_mentors"] == "exists")
            else False
            if (self._data["has_mentors"] == "solo")
            else None
        )

    @staticmethod
    def select_sql() -> str:
        """sql select command"""
        return """SELECT id, user_id,
            application->'champion'->>'confidentCoding' as "confidence_coding",
            application->'champion'->>'confidentMentoring' as "confidence_mentoring",
            application->'venue'->>'type' as "venue_type",
            application->'venue'->>'alternativeType' as "alternative_venue_type",
            application->'champion'->>'reference' as "referer",
            application->'champion'->>'alternativeReference' as "alternative_referer",
            application->'team'->>'status' as "has_mentors",
            application->'team'->'src'->>'community' as "mentor_youth_workers",
            application->'team'->'src'->>'parents' as "mentor_parents",
            application->'team'->'src'->>'pro' as "mentor_it_professionals",
            application->'team'->'src'->>'staff' as "mentor_venue_staff",
            application->'team'->'src'->>'students' as "mentor_students",
            application->'team'->'src'->>'teachers' as "mentor_teachers",
            application->'team'->'src'->>'youth' as "mentor_youth_u18",
            application->'team'->'alternativeSrc' as "mentor_other",
            created_at, updated_at, completed_at
        FROM cd_dojoleads ORDER BY completed_at desc"""

    @staticmethod
    def insert_sql() -> str:
        """sql insert command"""
        return """INSERT INTO "public"."dimDojoLeads"(
            id,
            user_id,
            confidence_coding,
            confidence_mentoring,
            venue_type,
            alternative_venue_type,
            referer,
            alternative_referer,
            has_mentors,
            mentor_youth_workers,
            mentor_parents,
            mentor_it_professionals,
            mentor_venue_staff,
            mentor_students,
            mentor_teachers,
            mentor_youth_u18,
            mentor_other,
            created_at,
            updated_at,
            completed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    def to_tuple(self) -> Tuple:
        """convert Lead to tuple"""
        return (
            self._data["id"],
            self._data["user_id"],
            self._data["confidence_coding"],
            self._data["confidence_mentoring"],
            self._data["venue_type"],
            self._data["alternative_venue_type"],
            self._data["referer"],
            self._data["alternative_referer"],
            self.has_mentors,
            bool(self._data["mentor_youth_workers"]),
            bool(self._data["mentor_parents"]),
            bool(self._data["mentor_it_professionals"]),
            bool(self._data["mentor_venue_staff"]),
            bool(self._data["mentor_students"]),
            bool(self._data["mentor_teachers"]),
            bool(self._data["mentor_youth_u18"]),
            self._data["mentor_other"],
            self._data["created_at"],
            self._data["updated_at"],
            self._data["completed_at"],
        )
