from asyncio import gather


async def transform_lead(row: dict) -> tuple:
    """Transform / Load for Lead Dimension"""
    completion = 0
    if row["champion_is_valid"] == "true":
        completion += 1
    if row["dojo_is_valid"] == "true":
        completion += 10
    if row["venue_is_valid"] == "true":
        completion += 100
    if row["team_is_valid"] == "true":
        completion += 1000
    if row["charter_is_valid"] == "true":
        completion += 10000
    return (
        row["id"],
        row["user_id"],
        row["confidence_coding"],
        row["confidence_mentoring"],
        row["venue_type"],
        row["alternative_venue_type"],
        row["referer"],
        row["alternative_referer"],
        True
        if (row["has_mentors"] == "exists")
        else False
        if (row["has_mentors"] == "solo")
        else None,
        bool(row["mentor_youth_workers"]),
        bool(row["mentor_parents"]),
        bool(row["mentor_it_professionals"]),
        bool(row["mentor_venue_staff"]),
        bool(row["mentor_students"]),
        bool(row["mentor_teachers"]),
        bool(row["mentor_youth_u18"]),
        row["mentor_other"],
        True
        if (
            (row["email"] is not None and "coderdojo.com" in row["email"])
            or row["request_email"] == "true"
        )
        else False,
        completion,
        row["created_at"],
        row["updated_at"],
        row["completed_at"],
    )


async def migrate_lead(cursor, dw_cursor):
    # Queries - Leads
    cursor.execute(
        """ SELECT id, user_id,
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
        application->'dojo'->>'requestEmail' as "request_email",
        application->'dojo'->>'email' as "email",
        application->'champion'->>'isValid' as "champion_is_valid",
        application->'dojo'->>'isValid' as "dojo_is_valid",
        application->'venue'->>'isValid' as "venue_is_valid",
        application->'team'->>'isValid' as "team_is_valid",
        application->'charter'->>'isValid' as "charter_is_valid",
        created_at, updated_at, completed_at
        FROM cd_dojoleads ORDER BY completed_at desc
    """
    )
    dw_cursor.executemany(
        """
        INSERT INTO "public"."dimDojoLeads"(
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
            requested_email,
            completion,
            created_at,
            updated_at,
            completed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
        await gather(*(transform_lead(dojo) for dojo in cursor.fetchall())),
    )
    print("Inserted leads")
