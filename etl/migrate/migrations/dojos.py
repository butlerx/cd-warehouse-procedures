from asyncio import gather


async def transform_dojo(row: dict) -> tuple:
    """Transform / Load for Dojo Dimension"""
    return (
        row["id"],
        row["created"],
        row["verified_at"],
        row["stage"],
        (
            row["country"]["countryName"]
            if (row["country"] is not None) and row["country"]
            else "Unknown"
        ),
        (
            row["place"]["name"]
            if (row["city"] is not None) and row["city"]
            else "Unknown"
        ),
        (
            row["county"]["toponymName"]
            if (row["county"] is not None) and row["county"]
            else "Unknown"
        ),
        (
            row["state"]["toponymName"]
            if (row["state"] is not None) and row["state"]
            else "Unknown"
        ),
        row["continent"],
        row["tao_verified"],
        (
            row["expected_attendees"] if (row["expected_attendees"] is not None) else 0
        ),  # Maybe something other than 0????
        row["verified"],
        row["deleted"],
        1 if (row["stage"] == 4) else 0,
        row["inactive_at"],
        1 if row["eventbrite_token"] and row["eventbrite_wh_id"] else 0,
        row["dojo_lead_id"],
        f"https://zen.coderdojo.com/dojos/{row['url_slug']}",
    )


async def transform_link_users(row: dict) -> tuple:
    return (row["id"], row["user_id"], row["dojo_id"], row["user_type"])


async def link_users(cursor, dw_cursor):
    cursor.execute(
        """
        SELECT id, user_id, dojo_id, unnest(user_types) as user_type
        FROM cd_usersdojos
        WHERE deleted = 0
    """
    )
    dw_cursor.executemany(
        """
        INSERT INTO "public"."dimUsersDojos"(
            id,
            user_id,
            dojo_id,
            user_type)
        VALUES (%s, %s, %s, %s)
    """,
        await gather(*(transform_link_users(dojo) for dojo in cursor.fetchall())),
    )
    print("Linked all dojos and users")


async def migrate_dojos(cursor, dw_cursor):
    cursor.execute(
        """
        SELECT * FROM cd_dojos
        LEFT JOIN (
            SELECT dojo_id, max(updated_at) as inactive_at
            FROM audit.dojo_stage
            WHERE stage = 4 GROUP BY dojo_id)
        as q ON q.dojo_id = cd_dojos.id
        WHERE verified = 1 and deleted = 0
    """
    )
    dw_cursor.executemany(
        """
        INSERT INTO "public"."dimDojos"(
            id,
            created,
            verified_at,
            stage,
            country,
            city,
            county,
            state,
            continent,
            tao_verified,
            expected_attendees,
            verified,
            deleted,
            inactive,
            inactive_at,
            is_eb,
            lead_id,
            url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
        await gather(*(transform_dojo(dojo) for dojo in cursor.fetchall())),
    )
    print("Inserted all dojos")
