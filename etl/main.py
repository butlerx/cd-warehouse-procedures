#!/usr/bin/python
from isodate import parse_datetime
import psycopg2
import sys
import json
from pprint import pprint
import psycopg2.extras
import uuid

with open('./config.json') as data:
    data = json.load(data)

dojos = data['databases']['dojos']
users = data['databases']['users']
events = data['databases']['events']
dw = data['databases']['dw']
user = data['config']['user']
password = data['config']['password']
host = data['config']['host']

# connection strings
dojos_string = "host=" + host + " dbname=" + \
    dojos + " user=" + user + " password=" + password
users_string = "host=" + host + " dbname=" + \
    users + " user=" + user + " password=" + password
events_string = "host=" + host + " dbname=" + \
    events + " user=" + user + " password=" + password
dw_string = "host=" + host + " dbname=" + dw + \
    " user=" + user + " password=" + password

# print connection strings
print "Connecting to database\n    ->%s" % (dojos_string)
print "Connecting to database\n    ->%s" % (users_string)
print "Connecting to database\n    ->%s" % (events_string)
print "Connecting to database\n    ->%s" % (dw_string)

# get a connection
dojos_conn = psycopg2.connect(dojos_string)
users_conn = psycopg2.connect(users_string)
events_conn = psycopg2.connect(events_string)
dw_conn = psycopg2.connect(dw_string)

# use cursor with queries
dojos_cursor = dojos_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
users_cursor = users_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
events_cursor = events_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
dw_cursor = dw_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
print "Connected to all databases!\n"


def main():

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

    #Queries - Dojos
    dojos_cursor.execute(
        'SELECT * FROM cd_dojos where verified = 1 and deleted = 0 and stage != 4')
    for row in dojos_cursor:
        transformDojo(row)
    print("Inserted all dojos")

    #Queries - Events
    events_cursor.execute('SELECT * FROM cd_events')
    for row in events_cursor:
        transformEvent(row)
    print("Inserted all events and locations")

    #Queries - Users
    users_cursor.execute(
        'select * from cd_profiles inner join sys_user on cd_profiles.user_id = sys_user.id')
    for row in users_cursor:
        transformUser(row)
    print("Inserted all users")

    #Queries - Tickets
    events_cursor.execute(
        'select status, cd_tickets.id as ticket_id, type, quantity, deleted from cd_sessions inner join cd_tickets on cd_sessions.id = cd_tickets.session_id')
    for row in events_cursor:
        transformTicket(row)
    print("Inserted all tickets")

    #Queries - Badges
    users_cursor.execute(
        'select user_id, to_json(badges) as badges from cd_profiles where badges is not null and json_array_length(to_json(badges)) >= 1')
    for row in users_cursor.fetchall():
        transformBadges(row)
    print('Inserted badges')

    #Queries - Staging
    events_cursor.execute('select cd_applications.id, cd_applications.ticket_id, cd_applications.session_id, cd_applications.event_id, cd_applications.dojo_id, cd_applications.user_id, dates, country, city from cd_applications inner join cd_events on cd_applications.event_id = cd_events.id')
    for row in events_cursor:
        staging(row)
    print('Populated staging')

    dw_cursor.execute('select badge_id, user_id from "dimBadges"')
    for row in dw_cursor.fetchall():
        addBadge(row)
    print('Badges added to staging')

    #Queries - Measures
    dw_cursor.execute('select staging.dojo_id, staging.ticket_id, staging.session_id, staging.event_id, staging.user_id, staging.time_id, staging.location_id, staging.badge_id from "zen-source"."staging" inner join "public"."dimDojos" on "zen-source"."staging".dojo_id = "public"."dimDojos".id inner join "public"."dimEvents" on "zen-source"."staging".event_id = "public"."dimEvents".event_id inner join "public"."dimUsers" on "zen-source"."staging".user_id = "public"."dimUsers".user_id inner join "public"."dimTime" on "zen-source"."staging".time_id = "public"."dimTime".time_id inner join "public"."dimLocation" on "zen-source"."staging".location_id = "public"."dimLocation".location_id inner join "public"."dimBadges" on "zen-source"."staging".badge_id = "public"."dimBadges".badge_id')
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
    country = row['country'] if (row['country'] is not None) and (
        len(row['country'])) > 0 else 'Unknown'
    city = row['place'] if (row['city'] is not None) and (
        len(row['city'])) > 0 else 'Unknown'
    county = row['county'] if (row['county'] is not None) and (
        len(row['county'])) > 0 else 'Unknown'
    state = row['state'] if (row['state'] is not None) and (
        len(row['state'])) > 0 else 'Unknown'
    continent = row['continent']
    tao_verified = row['tao_verified']
    expected_attendees = row['expected_attendees'] if (
        row['expected_attendees'] is not None) else 0  # Maybe something other than 0????
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

    insertDojo(
        dojo_id,
        created_at,
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


def transformEvent(row):  # Transform / Load for Event Dimension
    event_id = row['id']
    recurring_type = row['recurring_type']
    country = row['country'] if (row['country'] is not None) and (
        len(row['country'])) > 0 else 'Unknown'
    city = row['city'] if (row['city'] is not None) and (
        len(row['city'])) > 0 else 'Unknown'
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

    insertEvent(
        event_id,
        recurring_type,
        country,
        city,
        created_at,
        event_type,
        dojo_id,
        public,
        status)


def transformUser(row):  # Transform / Load for User Dimension
    user_id = row['user_id']
    dob = row['dob']
    country = row['country'] if (row['country'] is not None) and (
        len(row['country'])) > 0 else 'Unknown'
    continent = row['country'] if (row['continent'] is not None) and (
        len(row['continent'])) > 0 else 'Unknown'
    city = row['city'] if (row['city'] is not None) and (
        len(row['city'])) > 0 else 'Unknown'
    gender = row['gender'] if (row['gender'] is not None) and (
        len(row['gender'])) > 0 else 'Unknown'
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

    insertUser(
        user_id,
        dob,
        country,
        continent,
        city,
        gender,
        user_type,
        roles,
        mailing_list)


def transformTicket(row):
    ticket_id = row['ticket_id']
    ticket_type = row['type']
    quantity = row['quantity']
    deleted = row['deleted']

    insertTickets(ticket_id, ticket_type, quantity, deleted)


def insertDojo(
        dojo_id,
        created_at,
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
    data = (
        dojo_id,
        created_at,
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
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        print(e)


def insertEvent(
        event_id,
        recurring_type,
        country,
        city,
        created_at,
        event_type,
        dojo_id,
        public,
        status):
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
    data = (
        event_id,
        recurring_type,
        country,
        city,
        created_at,
        event_type,
        dojo_id,
        public,
        status)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        print(e)


def insertUser(
        user_id,
        dob,
        country,
        continent,
        city,
        gender,
        user_type,
        roles,
        mailing_list):
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
    data = (
        user_id,
        dob,
        country,
        continent,
        city,
        gender,
        user_type,
        roles,
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
            INSERT INTO "public"."dimTime"(
                year,
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

    country = row['country'] if (row['country'] is not None) and (
        len(row['country'])) > 0 else 'Unknown'
    city = row['city'] if (row['city'] is not None) and (
        len(row['city'])) > 0 else 'Unknown'

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
    data = (
        user_id,
        dojo_id,
        event_id,
        session_id,
        ticket_id,
        time_id,
        location_id,
        id)
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

    # #Active dojos
    # dojos_cursor.execute('select count(*) as cnt from cd_dojos where verified = 1 and deleted = 0 and stage != 4')
    # for row in dojos_cursor:
    #     active_dojos = row['cnt']

    # #Number of countries with active dojos
    # dojos_cursor.execute('SELECT COUNT(*) as cnt FROM (SELECT DISTINCT country::json->>\'countryName\' FROM cd_dojos where verified = 1 and deleted = 0 and stage != 4) AS temp')
    # for row in dojos_cursor:
    #     countries_with_active_dojos = row['cnt']

    # #Number of events in last thirty days
    # events_cursor.execute('SELECT count(*) from cd_events where created_at >  CURRENT_DATE - INTERVAL \'1 months\'')
    # for row in events_cursor:
    #     events_in_last_30_days = row['count']

    # #Number of dojos which have created more than 3 events in the last 3 months
    # events_cursor.execute('''
    #                         select count(*) as cnt
    #                         from (
    #                             SELECT count(*) as subcnt from cd_events where created_at > CURRENT_DATE - INTERVAL \'3 months\'
    #                             group by dojo_id having count(*) >= 3
    #                         ) subcnt
    #                     ''')
    # for row in events_cursor:
    #     dojos_3_events_in_3_months = row['cnt']

    # #Total champions
    # users_cursor.execute('select count(*) as cnt from cd_profiles where user_type = \'champion\'')
    # for row in users_cursor:
    #     total_champions = row['cnt']

    # #Total mentors
    # users_cursor.execute('select count(*) as cnt from cd_profiles where user_type = \'mentor\'')
    # for row in users_cursor:
    #     total_mentors = row['cnt']

    # #Total adults
    # users_cursor.execute('SELECT count(*) as cnt FROM cd_profiles WHERE dob < NOW() - INTERVAL \'18 years\'')
    # for row in users_cursor:
    #     total_adults = row['cnt']

    # #Total youth
    # users_cursor.execute('SELECT count(*) as cnt FROM cd_profiles WHERE user_type = \'attendee-u13\'')
    # for row in users_cursor:
    #     total_youth = row['cnt']

    # #U13 male
    # users_cursor.execute('SELECT count(*) as cnt FROM cd_profiles WHERE user_type = \'attendee-u13\' and gender = \'Male\'')
    # for row in users_cursor:
    #     u13_male = row['cnt']

    # #U13 female
    # users_cursor.execute('SELECT count(*) as cnt FROM cd_profiles WHERE user_type = \'attendee-u13\' and gender = \'Female\'')
    # for row in users_cursor:
    #     u13_female = row['cnt']

    # #O13 male
    # users_cursor.execute('SELECT count(*) as cnt FROM cd_profiles WHERE user_type = \'attendee-o13\' and gender = \'Male\'')
    # for row in users_cursor:
    #     o13_male = row['cnt']

    # #O13 female
    # users_cursor.execute('SELECT count(*) as cnt FROM cd_profiles WHERE user_type = \'attendee-o13\' and gender = \'Female\'')
    # for row in users_cursor:
    #     o13_female = row['cnt']

    # #Verified active dojos since 2017
    # dojos_cursor.execute('select count(*) as cnt from cd_dojos where date_part(\'year\', created) = 2017 and verified = 1 and deleted = 0 and stage != 4')
    # for row in dojos_cursor:
    #     verified_dojos_since_2017 = row['cnt']

    # #Number of dojos active login on zen
    # users_cursor.execute('''
    #                         select count(*) as cnt
    #                         from sys_user inner join cd_profiles
    #                         on sys_user.id = cd_profiles.user_id
    #                         where user_type = 'champion' and last_login >  CURRENT_DATE - INTERVAL \'12 months\'
    #                     ''')
    # for row in users_cursor:
    #     dojos_active_login = row['cnt']

    # sql = '''
    #             INSERT INTO "public"."factUsers"(
    #                 active_dojos,
    #                 countries_with_active_dojos,
    #                 events_in_last_30_days,
    #                 dojos_3_events_in_3_months,
    #                 total_champions,
    #                 total_mentors,
    #                 total_adults,
    #                 total_youth,
    #                 u13_male,
    #                 u13_female,
    #                 o13_male,
    #                 o13_female,
    #                 verified_dojos_since_2017,
    #                 dojos_active_login
    #             ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #     '''
    # data = (active_dojos, countries_with_active_dojos, events_in_last_30_days, dojos_3_events_in_3_months, total_champions, total_mentors, total_adults, total_youth, u13_male, u13_female, o13_male, o13_female, verified_dojos_since_2017, dojos_active_login)

    dojo_id = row['dojo_id']
    ticket_id = row['ticket_id']
    session_id = row['session_id']
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
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
    data = (
        dojo_id,
        ticket_id,
        event_id,
        user_id,
        time_id,
        location_id,
        id,
        badge_id)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        print(e)


if __name__ == '__main__':
    main()
