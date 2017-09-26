#!/usr/bin/env python

import json
import shlex
import uuid
from subprocess import PIPE, Popen

import botocore
import psycopg2
import psycopg2.extras
from boto3 import resource
from isodate import parse_datetime

with open('./config.json') as data:
    data = json.load(data)

dojos = data['databases']['dojos']
users = data['databases']['users']
events = data['databases']['events']
dw = data['databases']['dw']
aws = data['s3']
dojos_cursor = None
dojos_conn = None
users_cursor = None
users_conn = None
events_cursor = None
events_conn = None
dw_cursor = None
dw_conn = None


def main():
    global dojos_cursor
    global dojos_conn
    download('dojo')
    restore_db(dojos.host, dojos.db, dojos.user, dojos.password,
               './db/dojos.tar.gz')
    print "Connecting to database\n    ->%s" % (dojos.db)
    dojos_conn = psycopg2.connect(
        dbname=dojos.db,
        host=dojos.host,
        user=dojos.user,
        password=dojos.password)
    dojos_cursor = dojos_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    global events_conn
    global events_cursor
    download('events')
    restore_db(events.host, events.db, events.user, events.password,
               './db/events.tar.gz')
    print "Connecting to database\n    ->%s" % (events.db)
    events_conn = psycopg2.connect(
        dbname=events.db,
        host=events.host,
        user=events.user,
        password=events.password)
    events_cursor = events_conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor)

    global users_conn
    global users_cursor
    download('users')
    restore_db(users.host, users.db, users.user, users.password,
               './db/users.tar.gz')
    print "Connecting to database\n    ->%s" % (users.db)
    users_conn = psycopg2.connect(
        dbname=users.db,
        host=users.host,
        user=users.user,
        password=users.password)
    users_cursor = users_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    print "Connecting to database\n    ->%s" % (dw.db)
    setup_warehouse()
    global dw_conn
    global dw_cursor
    dw_conn = psycopg2.connect(
        dbname=dw.db, host=dw.host, user=dw.user, password=dw.password)
    # use cursor with queries
    dw_cursor = dw_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    print "Connected to all databases!\nMigrating Data"
    migrate_db()


def get_last_modified(obj):
    return int(obj['LastModified'].strftime('%s'))


def setup_warehouse():
    print "setting up Data Warehouse"
    dw_setup = psycopg2.connect(
        dbname='postgres', host=dw.host, user=dw.user, password=dw.password)
    cursor = dw_setup.cursor()
    try:
        cursor.execute('DROP DATABASE IF EXISTS {0}'.format(dw.db))
        cursor.execute(open("./sql/dw.sql", "r").read())
        cursor.commit()
    except (psycopg2.Error) as e:
        print(e)


def download(db):
    s3 = resource('s3')
    bucket = s3.Bucket(aws.bucket)
    backups = bucket.objects.filter(db)
    sBackups = sorted(backups, key=get_last_modified)
    try:
        bucket.download_file(sBackups[0], './db/' + db + '.tar.gz')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


def restore_db(host, database, user, password, path):
    command = 'pg_restore -h {0} -d {1} -U {2} {3}'.format(
        host, database, user, path)
    command = shlex.split(command)
    p = Popen(command, shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    return p.communicate('{}\n'.format(password))


def migrate_db():
    # Truncate all tables before fresh insert from sources
    dw_cursor.execute('TRUNCATE TABLE "factUsers" CASCADE')
    dw_cursor.execute('TRUNCATE TABLE "dimDojos" CASCADE')
    dw_cursor.execute('TRUNCATE TABLE "dimUsers" CASCADE')
    dw_cursor.execute('TRUNCATE TABLE "dimEvents" CASCADE')
    dw_cursor.execute('TRUNCATE TABLE "dimLocation" CASCADE')
    dw_cursor.execute('TRUNCATE TABLE "dimTime" CASCADE')
    dw_cursor.execute('TRUNCATE TABLE "dimTickets" CASCADE')
    dw_cursor.execute('TRUNCATE TABLE "zen-source"."staging" CASCADE')
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
        FROM "zen-source"."staging"
        INNER JOIN "public"."dimDojos"
        ON "zen-source"."staging".dojo_id = "public"."dimDojos".id
        INNER JOIN "public"."dimEvents"
        ON "zen-source"."staging".event_id = "public"."dimEvents".event_id
        INNER JOIN "public"."dimUsers"
        ON "zen-source"."staging".user_id = "public"."dimUsers".user_id
        INNER JOIN "public"."dimTime"
        ON "zen-source"."staging".time_id = "public"."dimTime".time_id
        INNER JOIN "public"."dimLocation" ON
        "zen-source"."staging".location_id = "public"."dimLocation".location_id
        INNER JOIN "public"."dimBadges"
        ON "zen-source"."staging".badge_id = "public"."dimBadges".badge_id'
    ''')
    for row in dw_cursor.fetchall():
        measures(row)
    print("Inserted measures")


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
        print(e)


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
        print(e)


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
        print(e)


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
        print(e)


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
        print(e)


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
        print(e)


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
        INSERT INTO "zen-source"."staging"(
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
        print(e)


def addBadge(row):
    user_id = row['user_id']
    badge_id = row['badge_id']

    sql = '''
        UPDATE "zen-source".staging
        SET badge_id=%s
        WHERE user_id=%s
    '''
    data = (badge_id, user_id)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        print(e)


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
        print(e)


def measures(row):
    # was commented before i got the script
    # active_dojos = 0
    # countries_with_active_dojos = 0
    # events_in_last_30_days = 0
    # dojos_3_events_in_3_months = 0
    # total_champions = 0
    # total_mentors = 0
    # total_adults = 0
    # total_youth = 0
    # u13_male = 0
    # u13_female = 0
    # o13_male = 0
    # o13_female = 0
    # verified_dojos_since_2017 = 0
    # dojos_active_login = 0

    # # Active dojos
    # dojos_cursor.execute('''
    #     SELECT COUNT(*) AS cnt
    #     FROM cd_dojos
    #     WHERE verified = 1 AND deleted = 0 AND stage != 4
    # ''')
    # for row in dojos_cursor:
    #     active_dojos = row['cnt']

    # # Number of countries with active dojos
    # dojos_cursor.execute('''
    #     SELECT COUNT(*) AS cnt
    #     FROM (
    #         SELECT DISTINCT country::json->>\'countryName\'
    #         FROM cd_dojos
    #         WHERE verified = 1 AND deleted = 0 AND stage != 4
    #     ) AS temp
    # ''')
    # for row in dojos_cursor:
    #     countries_with_active_dojos = row['cnt']

    # # Number of events in last thirty days
    # events_cursor.execute('''
    #     SELECT COUNT(*)
    #     FROM cd_events
    #     WHERE created_at > CURRENT_DATE - INTERVAL \'1 months\'
    # ''')
    # for row in events_cursor:
    #     events_in_last_30_days = row['count']

    # # Number of dojos which have created more than 3 events
    # # in the last 3 months
    # events_cursor.execute('''
    #     SELECT COUNT(*) AS cnt
    #     FROM (
    #         SELECT COUNT(*) AS subcnt
    #         FROM cd_events
    #         WHERE created_at > CURRENT_DATE - INTERVAL \'3 months\'
    #         GROUP BY dojo_id HAVING COUNT(*) >= 3
    #     ) subcnt
    # ''')
    # for row in events_cursor:
    #     dojos_3_events_in_3_months = row['cnt']

    # # Total champions
    # users_cursor.execute('''
    #     SELECT COUNT(*) AS cnt
    #     FROM cd_profiles
    #     WHERE user_type = \'champion\''
    # ''')
    # for row in users_cursor:
    #     total_champions = row['cnt']

    # # Total mentors
    # users_cursor.execute('''
    #     SELECT COUNT(*) AS cnt
    #     FROM cd_profiles
    #     WHERE user_type = \'mentor\'
    # ''')
    # for row in users_cursor:
    #     total_mentors = row['cnt']

    # # Total adults
    # users_cursor.execute('''
    #     SELECT COUNT(*) AS cnt
    #     FROM cd_profiles
    #     WHERE dob < NOW() - INTERVAL \'18 years\''
    # ''')
    # for row in users_cursor:
    #     total_adults = row['cnt']

    # # Total youth
    # users_cursor.execute('''
    #     SELECT COUNT(*) AS cnt
    #     FROM cd_profiles
    #     WHERE user_type = \'attendee-u13\'
    # ''')
    # for row in users_cursor:
    #     total_youth = row['cnt']

    # # U13 male
    # users_cursor.execute('''
    #     SELECT COUNT(*) AS cnt
    #     FROM cd_profiles
    #     WHERE user_type = \'attendee-u13\' AND gender = \'Male\''
    # ''')
    # for row in users_cursor:
    #     u13_male = row['cnt']

    # # U13 female
    # users_cursor.execute('''
    #     SELECT COUNT(*) AS cnt
    #     FROM cd_profiles
    #     WHERE user_type = \'attendee-u13\' and gender = \'Female\''
    # ''')
    # for row in users_cursor:
    #     u13_female = row['cnt']

    # # O13 male
    # users_cursor.execute('''
    #     SELECT COUNT(*) AS cnt
    #     FROM cd_profiles
    #     WHERE user_type= \'attendee - o13\' and gender= \'Male\'
    # ''')
    # for row in users_cursor:
    #     o13_male = row['cnt']

    # # O13 female
    # users_cursor.execute('''
    #     SELECT COUNT(*) AS cnt
    #     FROM cd_profiles
    #     WHERE user_type= \'attendee - o13\' and gender= \'Female\'
    # ''')
    # for row in users_cursor:
    #     o13_female = row['cnt']

    # # Verified active dojos since 2017
    # dojos_cursor.execute('''
    #     SELECT COUNT(*) AS cnt
    #     FROM cd_dojos
    #     WHERE date_part(\'year\', created)=2017
    #     AND verified=1 AND deleted=0 AND stage != 4
    # ''')
    # for row in dojos_cursor:
    #     verified_dojos_since_2017 = row['cnt']

    # # Number of dojos active login on zen
    # users_cursor.execute('''
    #     SELECT COUNT(*) AS cnt
    #     FROM sys_user INNER JOIN cd_profiles
    #     ON sys_user.id=cd_profiles.user_id
    #     WHERE user_type='champion'
    #     AND last_login > CURRENT_DATE - INTERVAL \'12 months\'
    # ''')
    # for row in users_cursor:
    #     dojos_active_login = row['cnt']

    # sql = '''
    #    INSERT INTO "public"."factUsers"(
    #        active_dojos,
    #        countries_with_active_dojos,
    #        events_in_last_30_days,
    #        dojos_3_events_in_3_months,
    #        total_champions,
    #        total_mentors,
    #        total_adults,
    #        total_youth,
    #        u13_male,
    #        u13_female,
    #        o13_male,
    #        o13_female,
    #        verified_dojos_since_2017,
    #        dojos_active_login
    #    ) VALUES(
    #    % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s)
    # '''
    # data = (active_dojos, countries_with_active_dojos,
    #         events_in_last_30_days, dojos_3_events_in_3_months,
    #         total_champions, total_mentors, total_adults, total_youth,
    #         u13_male, u13_female, o13_male, o13_female,
    #         verified_dojos_since_2017, dojos_active_login)

    dojo_id = row['dojo_id']
    ticket_id = row['ticket_id']
    # session_id = row['session_id']
    event_id = row['event_id']
    user_id = row['user_id']
    time_id = row['time_id']
    location_id = row['location_id']
    id = str(uuid.uuid4())
    badge_id = row['badge_id']

    sql = '''
        INSERT INTO "public"."factUsers"(
            dojo_id,
            ticket_id,
            event_id,
            user_id,
            time_id,
            location_id,
            id,
            badge_id
        ) VALUES ( % s, % s, % s, % s, % s, % s, % s, % s)
    '''
    data = (dojo_id, ticket_id, event_id, user_id, time_id, location_id, id,
            badge_id)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        print(e)


if __name__ == '__main__':
    main()
