"""functions related to tickets"""

from typing import Dict, Tuple, Type

from warehouse import Migration, Runner


def ticket(_) -> Type[Runner]:
    """ticket Migration"""

    class Ticket(Migration):
        """ticket object"""

        def __init__(self, row: Dict) -> None:
            self.ticket_id: str = row["ticket_id"]
            self.type: str = row["type"]
            self.quantity: str = row["quantity"]
            self.deleted: str = row["deleted"]

        def to_tuple(self) -> Tuple:
            """reuturn tuple of ticket"""
            return (self.ticket_id, self.type, self.quantity, self.deleted)

        @staticmethod
        def insert_sql() -> str:
            """sql for inserting ticket"""
            return """INSERT INTO "public"."dimTickets"(
                    ticket_id,
                    type,
                    quantity,
                    deleted)
                VALUES (%s, %s, %s, %s)"""

        @staticmethod
        def select_sql() -> str:
            """sql for selecting ticket"""
            return """SELECT status,
                cd_tickets.id AS ticket_id,
                type,
                quantity,
                deleted
            FROM cd_sessions
            INNER JOIN cd_tickets ON cd_sessions.id = cd_tickets.session_id"""

    class _TicketMigration(Runner):
        @staticmethod
        def setup() -> Tuple[Type[Ticket], str]:
            """set type and message"""
            return Ticket, "Inserted all tickets"

    return _TicketMigration
