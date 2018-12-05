import sys

import psycopg2
import psycopg2.extras
from badges import add_badges, transform_badges
from dojos import link_users, transform_dojo
from events import transform_event
from measures import get_id
from staging import stage
from tickets import transform_ticket
from leads import transform_lead 
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
        dw_cursor.execute('TRUNCATE TABLE "dimDojoLeads" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimUsers" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimEvents" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimLocation" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimUsersDojos" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimTickets" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "staging" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimBadges" CASCADE')

        # Queries - Dojos
        dojos_cursor.execute('''
            SELECT * FROM cd_dojos 
            LEFT JOIN (
                SELECT dojo_id, max(updated_at) as inactive_at 
                FROM audit.dojo_stage 
                WHERE stage = 4 GROUP BY dojo_id)
            as q ON q.dojo_id = cd_dojos.id
            WHERE verified = 1 and deleted = 0
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
                deleted,
                inactive,
                inactive_at,
                is_eb,
                lead_id,
                url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', map(transform_dojo, dojos_cursor.fetchall()))
        print("Inserted all dojos")
        sys.stdout.flush()

        # Queries - Dojos
        dojos_cursor.execute('''
            SELECT id, user_id, dojo_id, unnest(user_types) as user_type
            FROM cd_usersdojos
            WHERE deleted = 0
        ''')
        dw_cursor.executemany('''
            INSERT INTO "public"."dimUsersDojos"(
                id,
                user_id,
                dojo_id,
                user_type)
            VALUES (%s, %s, %s, %s)
        ''', map(link_users, dojos_cursor.fetchall()))
        print("Linked all dojos and users")
        sys.stdout.flush()

        # Queries - Events
        events_cursor.execute('SELECT cd_events.*, CASE (d.date->>\'startTime\') WHEN \'Invalid date\' THEN NULL ELSE (d.date->>\'startTime\')::timestamp END start_time FROM cd_events LEFT OUTER JOIN (SELECT id, unnest(dates) as date FROM cd_events) d ON d.id = cd_events.id')
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
                is_eb,
                start_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                city,
                gender,
                user_type,
                roles,
                mailing_list,
                created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    user_id,
                    issued_on
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', transform_badges(row))
        print('Inserted badges')
        sys.stdout.flush()
        
        # Queries - Leads 
        dojos_cursor.execute('''
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
            application->'dojo'->>'requestEmail' as "request_email",
            application->'dojo'->>'email' as "email",
            application->'champion'->>'isValid' as "champion_is_valid",
            application->'dojo'->>'isValid' as "dojo_is_valid",
            application->'venue'->>'isValid' as "venue_is_valid",
            application->'team'->>'isValid' as "team_is_valid",
            application->'charter'->>'isValid' as "charter_is_valid",
            created_at, updated_at, completed_at
            FROM cd_dojoleads ORDER BY completed_at desc
        ''')
        dw_cursor.executemany('''
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
        ''', map(transform_lead, dojos_cursor.fetchall()))
        print('Inserted leads')
        sys.stdout.flush()

        # Queries - Staging
        events_cursor.execute('''
        SELECT cd_applications.id, cd_applications.ticket_id,
            cd_applications.session_id, cd_applications.event_id,
            cd_applications.dojo_id, cd_applications.user_id,
            cd_applications.attendance,
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
            checked_in,
            time,
            location_id,
            id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                badge_id,
                checked_in
            ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', map(get_id, ids))
    except (psycopg2.Error) as e:
        raise (e)
