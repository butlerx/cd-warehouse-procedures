from asyncio import gather
from uuid import uuid4


async def transform_badges(element: dict, id: str) -> tuple:
    return (
        element["id"],
        element["archived"],
        element["type"],
        element["name"],
        str(uuid4()),
        id,
        element.get("assertion", {}).get("issuedOn", None),
    )


async def migrate_badges(cursor, dw_cursor):
    cursor.execute(
        """SELECT user_id, to_json(badges)
        AS badges
        FROM cd_profiles
        WHERE badges IS NOT null AND json_array_length(to_json(badges)) >= 1"""
    )
    for row in cursor.fetchall():
        dw_cursor.executemany(
            """INSERT INTO "public"."dimBadges"(
                id,
                archived,
                type,
                name,
                badge_id,
                user_id,
                issued_on
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            await gather(
                *(transform_badges(badge, row["user_id"]) for badge in row["badges"])
            ),
        )
    print("Inserted badges")


async def _add_badges(row: dict):
    return (row["badge_id"], row["user_id"])


async def add_badges(dw_cursor):
    dw_cursor.execute('SELECT badge_id, user_id FROM "dimBadges"')
    dw_cursor.executemany(
        """
        UPDATE "staging"
        SET badge_id=%s
        WHERE user_id=%s
    """,
        await gather(*(_add_badges(row) for row in dw_cursor.fetchall())),
    )
    print("Badges added to staging")
