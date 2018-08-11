"""reads and writes to database"""

from badges import add_badges, transform_badges
from dojos import link_users, transform_dojo
from events import transform_event
from leads import transform_lead
from local_types import Connection, Databases
from measures import get_id
from psycopg2 import connect
from psycopg2.extras import DictCursor
from staging import stage
from tickets import transform_ticket
from users import transform_user


class Migrator():
    """converts orginal dbs to warehouse"""

    def __init__(self, databases: Databases, con: Connection) -> None:
        self.dw_cursor = self._connect_db(databases.warehouse)
        self.dw_cursor.execute(open("./sql/dw.sql", "r").read())
        self.dojos_cursor = self._connect_db(databases.dojos)
        self.events_cursor = self._connect_db(databases.events)
        self.users_cursor = self._connect_db(databases.users)
        self.con = con
        self.databases = databases

    def _connect_db(self, database):
        conn = connect(
            dbname=database,
            host=self.con.host,
            user=self.con.user,
            password=self.con.password)
        conn.set_session(autocommit=True)
        return conn.cursor(
            cursor_factory=DictCursor)

    async def disconnect(self) -> None:
        """disconnect from db's"""
        self.dw_cursor.connection.close()
        self.users_cursor.connection.close()
        self.events_cursor.connection.close()
        self.dojos_cursor.connection.close()

    async def migrate_db(self):
        """perform db migrations"""
        await self.__truncate()
        await self.__migrate_dojos()
        await self.__link_dojos_users()
        await self.__migrate_events()
        await self.__migrate_users()
        await self.__migrate_tickets()
        await self.__migrate_badges()
        await self.__migrate_leads()
        await self.__stage()
        await self.__stage_badges()
        await self.__measure()

    async def __truncate(self) -> None:
        """ Truncate all tables before fresh insert from sources"""
        self.dw_cursor.execute('TRUNCATE TABLE "factUsers" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimDojos" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimDojoLeads" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimUsers" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimEvents" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimLocation" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimUsersDojos" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimTickets" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "staging" CASCADE')
        self.dw_cursor.execute('TRUNCATE TABLE "dimBadges" CASCADE')

    async def __migrate_dojos(self) -> None:
        # Queries - Dojos
        self.dojos_cursor.execute('''
            SELECT * FROM cd_dojos
            LEFT JOIN (
                SELECT dojo_id, max(updated_at) as inactive_at
                FROM audit.dojo_stage
                WHERE stage = 4 GROUP BY dojo_id)
            as q ON q.dojo_id = cd_dojos.id
            WHERE verified = 1 and deleted = 0
        ''')
        self.dw_cursor.executemany('''
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
                lead_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s)
        ''', map(transform_dojo, self.dojos_cursor.fetchall()))
        print("Inserted all dojos")

    async def __link_dojos_users(self):
        # Queries - Dojos
        self.dojos_cursor.execute('''
            SELECT id, user_id, dojo_id, unnest(user_types) as user_type
            FROM cd_usersdojos
            WHERE deleted = 0
        ''')
        self.dw_cursor.executemany('''
            INSERT INTO "public"."dimUsersDojos"(
                id,
                user_id,
                dojo_id,
                user_type)
            VALUES (%s, %s, %s, %s)
        ''', map(link_users, self.dojos_cursor.fetchall()))
        print("Linked all dojos and users")

    async def __migrate_events(self) -> None:
        # Queries - Events
        self.events_cursor.execute("""
            SELECT cd_events.*,
            CASE (d.date->>\'startTime\')
            WHEN \'Invalid date\'
                THEN NULL
                ELSE (d.date->>\'startTime\')::timestamp
            END start_time
            FROM cd_events
            LEFT OUTER JOIN (SELECT id, unnest(dates) as date
            FROM cd_events) d ON d.id = cd_events.id'
        """)
        self.dw_cursor.executemany('''
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
                start_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', map(transform_event, self.events_cursor.fetchall()))
        print("Inserted all events and locations")

    async def __migrate_users(self) -> None:
        # Queries - Users
        self.users_cursor.execute('''
            SELECT *
            FROM cd_profiles
            INNER JOIN sys_user ON cd_profiles.user_id = sys_user.id
        ''')
        self.dw_cursor.executemany('''
            INSERT INTO "public"."dimUsers"(
                user_id,
                dob,
                country,
                city,
                gender,
                user_type,
                roles,
                mailing_list,
                created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', map(transform_user, self.users_cursor.fetchall()))
        print("Inserted all users")

    async def __migrate_tickets(self) -> None:
        # Queries - Tickets
        self.events_cursor.execute('''
            SELECT status, cd_tickets.id AS ticket_id, type, quantity, deleted
            FROM cd_sessions
            INNER JOIN cd_tickets ON cd_sessions.id = cd_tickets.session_id
        ''')
        self.dw_cursor.executemany('''
            INSERT INTO "public"."dimTickets"(
                ticket_id,
                type,
                quantity,
                deleted)
            VALUES (%s, %s, %s, %s)
        ''', map(transform_ticket, self.events_cursor.fetchall()))
        print("Inserted all tickets")

    async def __migrate_badges(self) -> None:
        # Queries - Badges
        self.users_cursor.execute('''
            SELECT user_id, to_json(badges)
            AS badges
            FROM cd_profiles
            WHERE badges IS NOT null
            AND json_array_length(to_json(badges)) >= 1
        ''')
        for row in self.users_cursor.fetchall():
            self.dw_cursor.executemany('''
                INSERT INTO "public"."dimBadges"(
                    id,
                    archived,
                    type,
                    name,
                    badge_id,
                    user_id,
                    issued_on)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', transform_badges(row))
        print('Inserted badges')

    async def __migrate_leads(self):
        # Queries - Leads
        self.dojos_cursor.execute('''
            SELECT id, user_id,
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
                FROM cd_dojoleads ORDER BY completed_at desc
        ''')
        self.dw_cursor.executemany('''
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
                created_at,
                updated_at,
                completed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s)
        ''', map(transform_lead, self.dojos_cursor.fetchall()))
        print('Inserted leads')

    async def __stage(self) -> None:
        # Queries - Staging
        self.events_cursor.execute('''
            SELECT cd_applications.id, cd_applications.ticket_id,
                cd_applications.session_id, cd_applications.event_id,
                cd_applications.dojo_id, cd_applications.user_id,
                cd_applications.attendance,
                dates, country, city
            FROM cd_applications
            INNER JOIN cd_events ON cd_applications.event_id = cd_events.id
        ''')
        self.dw_cursor.executemany('''
            INSERT INTO "staging"(
                user_id,
                dojo_id,
                event_id,
                session_id,
                ticket_id,
                checked_in,
                time,
                location_id,
                id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', map(stage(self.dw_cursor), self.events_cursor.fetchall()))
        print('Populated staging')

    async def __stage_badges(self) -> None:
        self.dw_cursor.execute('SELECT badge_id, user_id FROM "dimBadges"')
        self.dw_cursor.executemany('''
            UPDATE "staging"
            SET badge_id=%s
            WHERE user_id=%s
        ''', add_badges(self.dw_cursor.fetchall()))
        print('Badges added to staging')

    async def __measure(self) -> None:
        # Queries - Measures
        self.dw_cursor.execute('''
            SELECT "staging".dojo_id, "staging".ticket_id,
                "staging".session_id, "staging".event_id,
                "staging".user_id, "staging".time, "staging".location_id,
                "staging".badge_id, "staging".checked_in
            FROM "staging"
             INNER JOIN "dimDojos"
              ON "staging".dojo_id = "dimDojos".id
            INNER JOIN "dimUsers"
              ON "staging".user_id = "dimUsers".user_id
            INNER JOIN "dimLocation" ON
              "staging".location_id = "dimLocation".location_id
            INNER JOIN "dimBadges"
              ON "staging".badge_id = "dimBadges".badge_id
            GROUP BY "staging".event_id, "staging".dojo_id,
                "staging".ticket_id, "staging".session_id, "staging".event_id,
                "staging".user_id, "staging".time, "staging".location_id,
                "staging".badge_id, "staging".checked_in
        ''')
        self.dw_cursor.executemany('''
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
            ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', map(get_id, self.dw_cursor.fetchall()))
