import uuid

import measures
import psycopg2
import psycopg2.extras
from databases import (dojos_cursor, dw_conn, dw_cursor, events_cursor,
                       users_cursor)
from isodate import parse_datetime


def setup_db():
    try:
        dw_conn.set_session(autocommit=True)
        dw_cursor.execute(open("./sql/dw.sql", "r").read())
        dw_conn.set_session(autocommit=False)
    except (psycopg2.Error) as e:
        print(e)
        dw_conn.set_session(autocommit=False)
        pass


def migrate_db():
    try:
        # Truncate all tables before fresh insert from sources
        dw_cursor.execute('TRUNCATE TABLE "factUsers" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimDojos" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimUsers" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimEvents" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimLocation" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimTime" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimTickets" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "zen_source"."staging" CASCADE')
        dw_cursor.execute('TRUNCATE TABLE "dimBadges" CASCADE')

        # Queries - Dojos
        dojos_cursor.execute('''
            SELECT *
            FROM cd_dojos
            WHERE verified = 1 and deleted = 0 and stage != 4
        ''')
        for row in dojos_cursor:
            transformDojo(row)
        print("Inserted all dojos")

        # Queries - Events
        events_cursor.execute('SELECT * FROM cd_events')
        for row in events_cursor:
            transformEvent(row)
        print("Inserted all events and locations")

        # Queries - Users
        users_cursor.execute('''
            SELECT *
            FROM cd_profiles
            INNER JOIN sys_user ON cd_profiles.user_id = sys_user.id
        ''')
        for row in users_cursor:
            transformUser(row)
        print("Inserted all users")

        # Queries - Tickets
        events_cursor.execute('''
            SELECT status, cd_tickets.id AS ticket_id, type, quantity, deleted
            FROM cd_sessions
            INNER JOIN cd_tickets ON cd_sessions.id = cd_tickets.session_id
        ''')
        for row in events_cursor:
            transformTicket(row)
        print("Inserted all tickets")

        # Queries - Badges
        users_cursor.execute('''
        SELECT user_id, to_json(badges)
        AS badges
        FROM cd_profiles
        WHERE badges IS NOT null AND json_array_length(to_json(badges)) >= 1
        ''')
        for row in users_cursor.fetchall():
            transformBadges(row)
        print('Inserted badges')

        # Queries - Staging
        events_cursor.execute('''
        SELECT cd_applications.id, cd_applications.ticket_id,
            cd_applications.session_id, cd_applications.event_id,
            cd_applications.dojo_id, cd_applications.user_id,
            dates, country, city
        FROM cd_applications
        INNER JOIN cd_events ON cd_applications.event_id = cd_events.id
        ''')
        for row in events_cursor:
            staging(row)
        print('Populated staging')

        dw_cursor.execute('SELECT badge_id, user_id FROM "dimBadges"')
        for row in dw_cursor.fetchall():
            addBadge(row)
        print('Badges added to staging')

        # Queries - Measures
        dw_cursor.execute('''
        SELECT staging.dojo_id, staging.ticket_id, staging.session_id,
            staging.event_id, staging.user_id, staging.time_id,
            staging.location_id, staging.badge_id
        FROM "zen_source"."staging"
        INNER JOIN "public"."dimDojos"
        ON "zen_source"."staging".dojo_id = "public"."dimDojos".id
        INNER JOIN "public"."dimEvents"
        ON "zen_source"."staging".event_id = "public"."dimEvents".event_id
        INNER JOIN "public"."dimUsers"
        ON "zen_source"."staging".user_id = "public"."dimUsers".user_id
        INNER JOIN "public"."dimTime"
        ON "zen_source"."staging".time_id = "public"."dimTime".time_id
        INNER JOIN "public"."dimLocation" ON
        "zen_source"."staging".location_id = "public"."dimLocation".location_id
        INNER JOIN "public"."dimBadges"
        ON "zen_source"."staging".badge_id = "public"."dimBadges".badge_id
        ''')
        for row in dw_cursor.fetchall():
            measures(row)
        print("Inserted measures")
    except (psycopg2.Error) as e:
        raise (e)


def transformBadges(row):
    user_id = row['user_id']
    badges = row['badges']
    for element in badges:
        id = element['id']
        archived = element['archived']
        type = element['type']
        name = element['name']
        badge_id = str(uuid.uuid4())
        insertBadge(id, archived, type, name, badge_id, user_id)


def transformDojo(row):  # Transform / Load for Dojo Dimension
    dojo_id = row['id']
    created_at = row['created']
    verified_at = row['verified_at']
    stage = row['stage']
    country = row['country'] if (row['country'] is not None
                                 ) and (len(row['country'])) > 0 else 'Unknown'
    city = row['place'] if (row['city'] is
                            not None) and (len(row['city'])) > 0 else 'Unknown'
    county = row['county'] if (
        row['county'] is not None) and (len(row['county'])) > 0 else 'Unknown'
    state = row['state'] if (
        row['state'] is not None) and (len(row['state'])) > 0 else 'Unknown'
    continent = row['continent']
    tao_verified = row['tao_verified']
    expected_attendees = row['expected_attendees'] if (
        row['expected_attendees'] is
        not None) else 0  # Maybe something other than 0????
    verified = row['verified']
    deleted = row['deleted']

    # For fields which zen prod dbs are storing as json
    if country is not 'Unknown':
        country = country['countryName']

    if city is not 'Unknown':
        city = city['name']

    if county is not 'Unknown':
        county = county['toponymName']

    if state is not 'Unknown':
        state = state['toponymName']

    insertDojo(dojo_id, created_at, verified_at, stage, country, city, county,
               state, continent, tao_verified, expected_attendees, verified,
               deleted)


def transformEvent(row):  # Transform / Load for Event Dimension
    event_id = row['id']
    recurring_type = row['recurring_type']
    country = row['country'] if (row['country'] is not None
                                 ) and (len(row['country'])) > 0 else 'Unknown'
    city = row['city'] if (row['city'] is
                           not None) and (len(row['city'])) > 0 else 'Unknown'
    created_at = row['created_at']
    event_type = row['type']
    dojo_id = row['dojo_id']
    public = row['public']
    status = row['status']

    # For fields which zen prod dbs are storing as json
    if country is not 'Unknown':
        country = country['countryName']

    if city is not 'Unknown':
        if 'toponymName' in city:
            city = city['toponymName']
        else:
            city = city['nameWithHierarchy']

    insertEvent(event_id, recurring_type, country, city, created_at,
                event_type, dojo_id, public, status)


def transformUser(row):  # Transform / Load for User Dimension
    user_id = row['user_id']
    dob = row['dob']
    country = row['country'] if (row['country'] is not None
                                 ) and (len(row['country'])) > 0 else 'Unknown'
    continent = row['country'] if (row['continent'] is not None) and (
        len(row['continent'])) > 0 else 'Unknown'
    city = row['city'] if (row['city'] is
                           not None) and (len(row['city'])) > 0 else 'Unknown'
    gender = row['gender'] if (
        row['gender'] is not None) and (len(row['gender'])) > 0 else 'Unknown'
    user_type = row['user_type']
    roles = row['roles']
    mailing_list = row['mailing_list']

    # For fields which zen prod dbs are storing as json
    if country is not 'Unknown':
        country = country['countryName']

    if continent is not 'Unknown':
        continent = continent['continent']

    if city is not 'Unknown':
        city = city['nameWithHierarchy']

    if roles:
        roles = roles[0] if len(roles) > 0 else 'Unknown'

    insertUser(user_id, dob, country, continent, city, gender, user_type,
               roles, mailing_list)


def transformTicket(row):
    ticket_id = row['ticket_id']
    ticket_type = row['type']
    quantity = row['quantity']
    deleted = row['deleted']

    insertTickets(ticket_id, ticket_type, quantity, deleted)


def insertDojo(dojo_id, created_at, verified_at, stage, country, city, county,
               state, continent, tao_verified, expected_attendees, verified,
               deleted):
    sql = '''
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
        '''
    data = (dojo_id, created_at, verified_at, stage, country, city, county,
            state, continent, tao_verified, expected_attendees, verified,
            deleted)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        raise (e)


def insertEvent(event_id, recurring_type, country, city, created_at,
                event_type, dojo_id, public, status):
    sql = '''
        INSERT INTO "public"."dimEvents"(
            event_id,
            recurring_type,
            country,
            city,
            created_at,
            type,
            dojo_id,
            public,
            status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    data = (event_id, recurring_type, country, city, created_at, event_type,
            dojo_id, public, status)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        raise (e)


def insertUser(user_id, dob, country, continent, city, gender, user_type,
               roles, mailing_list):
    sql = '''
        INSERT INTO "public"."dimUsers"(
            user_id,
            dob,
            country,
            continent,
            city,
            gender,
            user_type,
            roles,
            mailing_list)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    data = (user_id, dob, country, continent, city, gender, user_type, roles,
            mailing_list)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        raise (e)


def insertTickets(ticket_id, ticket_type, quantity, deleted):
    sql = '''
        INSERT INTO "public"."dimTickets"(
            ticket_id,
            type,
            quantity,
            deleted
        ) VALUES (%s, %s, %s, %s)
    '''
    data = (ticket_id, ticket_type, quantity, deleted)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        raise (e)


def insertLocation(country, city, location_id):
    sql = '''
        INSERT INTO "public"."dimLocation"(
            country,
            city,
            location_id
        ) VALUES (%s, %s, %s)
    '''
    data = (country, city, location_id)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        raise (e)


def insertTime(datetime, time_id):
    day = datetime.day
    month = datetime.month
    year = datetime.year
    sql = '''
        INSERT INTO "public"."dimTime"( year,
            month,
            day,
            time_id
        ) VALUES (%s, %s, %s, %s)
    '''
    data = (year, month, day, time_id)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        raise (e)


def staging(row):
    user_id = row['user_id']
    dojo_id = row['dojo_id']
    event_id = row['event_id']
    session_id = row['session_id']
    ticket_id = row['ticket_id']
    time_id = str(uuid.uuid4())
    location_id = str(uuid.uuid4())
    id = row['id']
    start_date = row['dates']

    if start_date[0]['startTime'] is not None:
        start_date = parse_datetime(start_date[0]['startTime'])
        insertTime(start_date, time_id)

    country = row['country'] if (row['country'] is not None
                                 ) and (len(row['country'])) > 0 else 'Unknown'
    city = row['city'] if (row['city'] is
                           not None) and (len(row['city'])) > 0 else 'Unknown'

    # For fields which zen prod dbs are storing as json
    if country is not 'Unknown':
        country = country['countryName']

    if city is not 'Unknown':
        if 'toponymName' in city:
            city = city['toponymName']
        else:
            city = city['nameWithHierarchy']

    insertLocation(country, city, location_id)

    sql = '''
        INSERT INTO "zen_source"."staging"(
            user_id,
            dojo_id,
            event_id,
            session_id,
            ticket_id,
            time_id,
            location_id,
            id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    '''
    data = (user_id, dojo_id, event_id, session_id, ticket_id, time_id,
            location_id, id)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        raise (e)


def addBadge(row):
    user_id = row['user_id']
    badge_id = row['badge_id']

    sql = '''
        UPDATE "zen_source".staging
        SET badge_id=%s
        WHERE user_id=%s
    '''
    data = (badge_id, user_id)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        raise (e)


def insertBadge(id, archived, type, name, badge_id, user_id):
    sql = '''
        INSERT INTO "public"."dimBadges"(
            id,
            archived,
            type,
            name,
            badge_id,
            user_id
        ) VALUES (%s, %s, %s, %s, %s, %s)
    '''
    data = (id, archived, type, name, badge_id, user_id)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        raise (e)
