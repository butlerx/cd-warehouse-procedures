import uuid

import psycopg2
import psycopg2.extras
from databases import (dojos_cursor, dw_conn, dw_cursor, events_cursor,
                       users_cursor)


def measures(row):
    # Was commented before i got the script
    active_dojos = 0
    countries_with_active_dojos = 0
    events_in_last_30_days = 0
    dojos_3_events_in_3_months = 0
    total_champions = 0
    total_mentors = 0
    total_adults = 0
    total_youth = 0
    u13_male = 0
    u13_female = 0
    o13_male = 0
    o13_female = 0
    verified_dojos_since_2017 = 0
    dojos_active_login = 0

    # Active dojos
    dojos_cursor.execute('''
        SELECT COUNT(*) AS cnt
        FROM cd_dojos
        WHERE verified = 1 AND deleted = 0 AND stage != 4
    ''')
    for row in dojos_cursor:
        active_dojos = row['cnt']

    # Number of countries with active dojos
    dojos_cursor.execute('''
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT DISTINCT country::json->>\'countryName\'
            FROM cd_dojos
            WHERE verified = 1 AND deleted = 0 AND stage != 4
        ) AS temp
    ''')
    for row in dojos_cursor:
        countries_with_active_dojos = row['cnt']

    # Number of events in last thirty days
    events_cursor.execute('''
        SELECT COUNT(*)
        FROM cd_events
        WHERE created_at > CURRENT_DATE - INTERVAL \'1 months\'
    ''')
    for row in events_cursor:
        events_in_last_30_days = row['count']

    # Number of dojos which have created more than 3 events
    # in the last 3 months
    events_cursor.execute('''
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT COUNT(*) AS subcnt
            FROM cd_events
            WHERE created_at > CURRENT_DATE - INTERVAL \'3 months\'
            GROUP BY dojo_id HAVING COUNT(*) >= 3
        ) subcnt
    ''')
    for row in events_cursor:
        dojos_3_events_in_3_months = row['cnt']

    # Total champions
    users_cursor.execute('''
        SELECT COUNT(*) AS cnt
        FROM cd_profiles
        WHERE user_type = \'champion\''
    ''')
    for row in users_cursor:
        total_champions = row['cnt']

    # Total mentors
    users_cursor.execute('''
        SELECT COUNT(*) AS cnt
        FROM cd_profiles
        WHERE user_type = \'mentor\'
    ''')
    for row in users_cursor:
        total_mentors = row['cnt']

    # Total adults
    users_cursor.execute('''
        SELECT COUNT(*) AS cnt
        FROM cd_profiles
        WHERE dob < NOW() - INTERVAL \'18 years\''
    ''')
    for row in users_cursor:
        total_adults = row['cnt']

    # Total youth
    users_cursor.execute('''
        SELECT COUNT(*) AS cnt
        FROM cd_profiles
        WHERE user_type = \'attendee-u13\'
    ''')
    for row in users_cursor:
        total_youth = row['cnt']

    # U13 male
    users_cursor.execute('''
        SELECT COUNT(*) AS cnt
        FROM cd_profiles
        WHERE user_type = \'attendee-u13\' AND gender = \'Male\''
    ''')
    for row in users_cursor:
        u13_male = row['cnt']

    # U13 female
    users_cursor.execute('''
        SELECT COUNT(*) AS cnt
        FROM cd_profiles
        WHERE user_type = \'attendee-u13\' and gender = \'Female\''
    ''')
    for row in users_cursor:
        u13_female = row['cnt']

    # O13 male
    users_cursor.execute('''
        SELECT COUNT(*) AS cnt
        FROM cd_profiles
        WHERE user_type= \'attendee - o13\' and gender= \'Male\'
    ''')
    for row in users_cursor:
        o13_male = row['cnt']

    # O13 female
    users_cursor.execute('''
        SELECT COUNT(*) AS cnt
        FROM cd_profiles
        WHERE user_type= \'attendee - o13\' and gender= \'Female\'
    ''')
    for row in users_cursor:
        o13_female = row['cnt']

    # Verified active dojos since 2017
    dojos_cursor.execute('''
        SELECT COUNT(*) AS cnt
        FROM cd_dojos
        WHERE date_part(\'year\', created)=2017
        AND verified=1 AND deleted=0 AND stage != 4
    ''')
    for row in dojos_cursor:
        verified_dojos_since_2017 = row['cnt']

    # Number of dojos active login on zen
    users_cursor.execute('''
        SELECT COUNT(*) AS cnt
        FROM sys_user INNER JOIN cd_profiles
        ON sys_user.id=cd_profiles.user_id
        WHERE user_type='champion'
        AND last_login > CURRENT_DATE - INTERVAL \'12 months\'
    ''')
    for row in users_cursor:
        dojos_active_login = row['cnt']

    sql = '''
       INSERT INTO "public"."factUsers"(
           active_dojos,
           countries_with_active_dojos,
           events_in_last_30_days,
           dojos_3_events_in_3_months,
           total_champions,
           total_mentors,
           total_adults,
           total_youth,
           u13_male,
           u13_female,
           o13_male,
           o13_female,
           verified_dojos_since_2017,
           dojos_active_login
       ) VALUES(
       % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s, % s)
    '''
    data = (active_dojos, countries_with_active_dojos, events_in_last_30_days,
            dojos_3_events_in_3_months, total_champions, total_mentors,
            total_adults, total_youth, u13_male, u13_female, o13_male,
            o13_female, verified_dojos_since_2017, dojos_active_login)
    # End of comment section

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
        ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    data = (dojo_id, ticket_id, event_id, user_id, time_id, location_id, id,
            badge_id)
    try:
        dw_cursor.execute(sql, data)
        dw_conn.commit()
    except (psycopg2.Error) as e:
        print(e)
        pass
