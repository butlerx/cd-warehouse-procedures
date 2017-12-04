import uuid


def measure(dojos_cursor, users_cursor, events_cursor):
    def calculate(args):
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
        active_dojos = dojos_cursor.fetchone()['cnt']

        # Number of countries with active dojos
        dojos_cursor.execute('''
            SELECT COUNT(*) AS cnt
            FROM (
                SELECT DISTINCT country::json->>\'countryName\'
                FROM cd_dojos
                WHERE verified = 1 AND deleted = 0 AND stage != 4
            ) AS temp
        ''')
        countries_with_active_dojos = dojos_cursor.fetchone()['cnt']

        # Number of events in last thirty days
        events_cursor.execute('''
            SELECT COUNT(*)
            FROM cd_events
            WHERE created_at > CURRENT_DATE - INTERVAL \'1 months\'
        ''')
        events_in_last_30_days = events_cursor.fetchone()['count']

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
        dojos_3_events_in_3_months = events_cursor.fetchone()['cnt']

        # Total champions
        users_cursor.execute('''
            SELECT COUNT(*) AS cnt
            FROM cd_profiles
            WHERE user_type = \'champion\'
        ''')
        total_champions = users_cursor.fetchone()['cnt']

        # Total mentors
        users_cursor.execute('''
            SELECT COUNT(*) AS cnt
            FROM cd_profiles
            WHERE user_type = \'mentor\'
        ''')
        total_mentors = users_cursor.fetchone()['cnt']

        # Total adults
        users_cursor.execute('''
            SELECT COUNT(*) AS cnt
            FROM cd_profiles
            WHERE dob < NOW() - INTERVAL \'18 years\'
        ''')
        total_adults = users_cursor.fetchone()['cnt']

        # Total youth
        users_cursor.execute('''
            SELECT COUNT(*) AS cnt
            FROM cd_profiles
            WHERE user_type = \'attendee-u13\'
        ''')
        total_youth = users_cursor.fetchone()['cnt']

        # U13 male
        users_cursor.execute('''
            SELECT COUNT(*) AS cnt
            FROM cd_profiles
            WHERE user_type = \'attendee-u13\' AND gender = \'Male\'
        ''')
        u13_male = users_cursor.fetchone()['cnt']

        # U13 female
        users_cursor.execute('''
            SELECT COUNT(*) AS cnt
            FROM cd_profiles
            WHERE user_type = \'attendee-u13\' and gender = \'Female\'
        ''')
        u13_female = users_cursor.fetchone()['cnt']

        # O13 male
        users_cursor.execute('''
            SELECT COUNT(*) AS cnt
            FROM cd_profiles
            WHERE user_type= \'attendee - o13\' and gender= \'Male\'
        ''')
        o13_male = users_cursor.fetchone()['cnt']

        # O13 female
        users_cursor.execute('''
            SELECT COUNT(*) AS cnt
            FROM cd_profiles
            WHERE user_type= \'attendee - o13\' and gender= \'Female\'
        ''')
        o13_female = users_cursor.fetchone()['cnt']

        # Verified active dojos since 2017
        dojos_cursor.execute('''
            SELECT COUNT(*) AS cnt
            FROM cd_dojos
            WHERE date_part(\'year\', created)=2017
            AND verified=1 AND deleted=0 AND stage != 4
        ''')
        verified_dojos_since_2017 = dojos_cursor.fetchone()['cnt']

        # Number of dojos active login on zen
        users_cursor.execute('''
            SELECT COUNT(*) AS cnt
            FROM sys_user INNER JOIN cd_profiles
            ON sys_user.id=cd_profiles.user_id
            WHERE user_type='champion'
            AND last_login > CURRENT_DATE - INTERVAL \'12 months\'
        ''')
        dojos_active_login = users_cursor.fetchone()['cnt']

        data = (active_dojos, countries_with_active_dojos,
                events_in_last_30_days, dojos_3_events_in_3_months,
                total_champions, total_mentors, total_adults, total_youth,
                u13_male, u13_female, o13_male, o13_female,
                verified_dojos_since_2017, dojos_active_login)
        return data

    return calculate


def get_id(args):
    dojo_id = args['dojo_id']
    ticket_id = args['ticket_id']
    # session_id = args['session_id']
    event_id = args['event_id']
    user_id = args['user_id']
    time = args['time']
    location_id = args['location_id']
    id = str(uuid.uuid4())
    badge_id = args['badge_id']
    data = (dojo_id, ticket_id, event_id, user_id, time, location_id, id,
            badge_id)
    return data
