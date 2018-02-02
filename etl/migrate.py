import sys

import psycopg2
import psycopg2.extras
from badges import add_badges, transform_badges
from dojos import link_users, transform_dojo
from events import transform_event
from measures import get_id
from staging import stage
from tickets import transform_ticket
from users import transform_user


async def setup_warehouse(dw_cursor):
    try:
        dw_cursor.execute(open("./sql/dw.sql", "r").read())
    except (psycopg2.Error) as e:
        print(e)


def migrate_db(dw_cursor, users_cursor, dojos_cursor, events_cursor):
    try:
        # Truncate all tables before fresh insert from sources
        dw_cursor.execute('TRUNCATE TABLE "factUsers" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimDojos" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimUsers" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimEvents" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimLocation" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimUsersDojos" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimTickets" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "staging" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimBadges" CASCADE')

        # Queries - Dojos
        dojos_cursor.execute('''
            SELECT *
            FROM cd_dojos
            WHERE verified = 1 and deleted = 0 and stage != 4
        ''')
        dw_cursor.executemany('''
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
                deleted)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', map(transform_dojo, dojos_cursor.fetchall()))
        print("Inserted all dojos")
        sys.stdout.flush()

        # Queries - Dojos
        dojos_cursor.execute('''
            SELECT *
            FROM cd_usersdojos
            WHERE deleted = 0
        ''')
        dw_cursor.executemany('''
            INSERT INTO "public"."dimUsersDojos"(
                id,
                user_id,
                dojo_id)
            VALUES (%s, %s, %s)
        ''', map(link_users, dojos_cursor.fetchall()))
        print("Linked all dojos and users")
        sys.stdout.flush()

        # Queries - Events
        events_cursor.execute('SELECT * FROM cd_events')
        dw_cursor.executemany('''
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
                is_eb
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', map(transform_event, events_cursor.fetchall()))
        print("Inserted all events and locations")
        sys.stdout.flush()

        # Queries - Users
        users_cursor.execute('''
            SELECT *
            FROM cd_profiles
            INNER JOIN sys_user ON cd_profiles.user_id = sys_user.id
        ''')
        dw_cursor.executemany('''
            INSERT INTO "public"."dimUsers"(
                user_id,
                dob,
                country,
                continent,
                city,
                gender,
                user_type,
                roles,
                mailing_list,
                created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', map(transform_user, users_cursor.fetchall()))
        print("Inserted all users")
        sys.stdout.flush()

        # Queries - Tickets
        events_cursor.execute('''
            SELECT status, cd_tickets.id AS ticket_id, type, quantity, deleted
            FROM cd_sessions
            INNER JOIN cd_tickets ON cd_sessions.id = cd_tickets.session_id
        ''')
        dw_cursor.executemany('''
            INSERT INTO "public"."dimTickets"(
                ticket_id,
                type,
                quantity,
                deleted
            ) VALUES (%s, %s, %s, %s)
        ''', map(transform_ticket, events_cursor.fetchall()))
        print("Inserted all tickets")
        sys.stdout.flush()

        # Queries - Badges
        users_cursor.execute('''
            SELECT user_id, to_json(badges)
            AS badges
            FROM cd_profiles
            WHERE badges IS NOT null AND json_array_length(to_json(badges)) >= 1
        ''')
        for row in users_cursor.fetchall():
            dw_cursor.executemany('''
                INSERT INTO "public"."dimBadges"(
                    id,
                    archived,
                    type,
                    name,
                    badge_id,
                    user_id
                ) VALUES (%s, %s, %s, %s, %s, %s)
            ''', transform_badges(row))
        print('Inserted badges')
        sys.stdout.flush()

        # Queries - Staging
        events_cursor.execute('''
        SELECT cd_applications.id, cd_applications.ticket_id,
            cd_applications.session_id, cd_applications.event_id,
            cd_applications.dojo_id, cd_applications.user_id,
            dates, country, city
        FROM cd_applications
        INNER JOIN cd_events ON cd_applications.event_id = cd_events.id
        ''')
        dw_cursor.executemany('''
        INSERT INTO "staging"(
            user_id,
            dojo_id,
            event_id,
            session_id,
            ticket_id,
            time,
            location_id,
            id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ''', map(stage(dw_cursor), events_cursor.fetchall()))
        print('Populated staging')
        sys.stdout.flush()

        dw_cursor.execute('SELECT badge_id, user_id FROM "dimBadges"')
        rows = dw_cursor.fetchall()
        dw_cursor.executemany('''
            UPDATE "staging"
            SET badge_id=%s
            WHERE user_id=%s
        ''', add_badges(rows))
        print('Badges added to staging')
        sys.stdout.flush()

        # Queries - Measures
        dw_cursor.execute('''
        SELECT "staging".dojo_id, "staging".ticket_id, "staging".session_id,
            "staging".event_id, "staging".user_id, "staging".time,
            "staging".location_id, "staging".badge_id
        FROM "staging"
        INNER JOIN "dimDojos"
        ON "staging".dojo_id = "dimDojos".id
        INNER JOIN "dimEvents"
        ON "staging".event_id = "dimEvents".event_id
        INNER JOIN "dimUsers"
        ON "staging".user_id = "dimUsers".user_id
        INNER JOIN "dimLocation" ON
        "staging".location_id = "dimLocation".location_id
        INNER JOIN "dimBadges"
        ON "staging".badge_id = "dimBadges".badge_id
        ''')
        ids = dw_cursor.fetchall()
        dw_cursor.executemany('''
            INSERT INTO "public"."factUsers"(
                dojo_id,
                ticket_id,
                event_id,
                user_id,
                time,
                location_id,
                id,
                badge_id
            ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)
        ''', map(get_id, ids))
    except (psycopg2.Error) as e:
        raise (e)
