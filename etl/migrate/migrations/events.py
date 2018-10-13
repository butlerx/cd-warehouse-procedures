from asyncio import gather


async def transform_event(row: dict) -> tuple:
    """Transform / Load for Event Dimension"""

    # For fields which zen prod dbs are storing as json
    city = row["city"] if (row["city"] is not None) and row["city"] else "Unknown"
    if city != "Unknown":
        city = (
            city["toponymName"] if "toponymName" in city else city["nameWithHierarchy"]
        )

    return (
        row["id"],
        row["recurring_type"],
        (
            row["country"]["countryName"]
            if (row["country"] is not None)
            and row["country"]
            and "countryName" in row["country"]
            else "Unknown"
        ),
        city,
        row["created_at"],
        row["type"],
        row["dojo_id"],
        row["public"],
        row["status"],
        row["eventbrite_id"] is not None,
        row["start_time"],
        row["published_at"],
    )


async def migrate_events(cursor, dw_cursor):
    cursor.execute(
        """"SELECT cd_events.*,
            CASE (d.date->>'startTime')
                WHEN 'Invalid date'
                THEN NULL
                ELSE (d.date->>'startTime')::timestamp
            END start_time
            FROM cd_events
            LEFT OUTER JOIN (
                SELECT id, unnest(dates) as date FROM cd_events
            ) d ON d.id = cd_events.id"""
    )
    dw_cursor.executemany(
        """
        INSERT INTO "public"."dimEvents"(
            event_id,
            recurring_type,
            country,
            city,
            created_at,
            type,
            dojo_id,
            public,
            status,
            is_eb,
            start_time,
            published_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
        await gather(*(transform_event(events) for events in cursor.fetchall())),
    )
    print("Inserted all events and locations")
