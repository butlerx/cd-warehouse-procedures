from asyncio import gather
from uuid import uuid4


async def get_id(args: dict) -> tuple:
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


async def add_ids(dw_cursor):
    dw_cursor.execute(
        """
    SELECT "staging".dojo_id, "staging".ticket_id, "staging".session_id,
        "staging".event_id, "staging".user_id, "staging".time,
        "staging".location_id, "staging".badge_id, "staging".checked_in
    FROM "staging"
     INNER JOIN "dimDojos"
      ON "staging".dojo_id = "dimDojos".id
    INNER JOIN "dimUsers"
      ON "staging".user_id = "dimUsers".user_id
    INNER JOIN "dimLocation" ON
      "staging".location_id = "dimLocation".location_id
    INNER JOIN "dimBadges"
      ON "staging".badge_id = "dimBadges".badge_id
     GROUP BY "staging".event_id, "staging".dojo_id, "staging".ticket_id, "staging".session_id,
        "staging".event_id, "staging".user_id, "staging".time,
        "staging".location_id, "staging".badge_id, "staging".checked_in
    """
    )
    dw_cursor.executemany(
        """
        INSERT INTO "public"."factUsers"(
            dojo_id,
            ticket_id,
            event_id,
            user_id,
            time,
            location_id,
            id,
            badge_id,
            checked_in
        ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        await gather(*(get_id(id) for id in dw_cursor.fetchall())),
    )
