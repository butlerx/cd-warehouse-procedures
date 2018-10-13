from asyncio import gather


async def transform_ticket(row):
    return (row["ticket_id"], row["type"], row["quantity"], row["deleted"])


async def migrate_tickets(cursor, dw_cursor):
    cursor.execute(
        """SELECT status, cd_tickets.id AS ticket_id, type, quantity, deleted
        FROM cd_sessions
        INNER JOIN cd_tickets ON cd_sessions.id = cd_tickets.session_id"""
    )
    dw_cursor.executemany(
        """ INSERT INTO "public"."dimTickets"(
            ticket_id,
            type,
            quantity,
            deleted
        ) VALUES (%s, %s, %s, %s)""",
        await gather(*(transform_ticket(event) for event in cursor.fetchall())),
    )
    print("Inserted all tickets")
