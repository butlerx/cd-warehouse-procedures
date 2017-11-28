import uuid

import psycopg2
import psycopg2.extras


def transform_badges(row):
    user_id = row['user_id']
    badges = row['badges']
    transformed_badges = []
    for element in badges:
        id = element['id']
        archived = element['archived']
        type = element['type']
        name = element['name']
        badge_id = str(uuid.uuid4())
        data = (id, archived, type, name, badge_id, user_id)
        transformed_badges.append(data)
    return transformed_badges


def add_Badge(cursor, rows):
    badges = []
    for row in rows:
        user_id = row['user_id']
        badge_id = row['badge_id']
        data = (badge_id, user_id)
        badges.append(data)
    try:
        sql = '''
            UPDATE "zen_source".staging
            SET badge_id=%s
            WHERE user_id=%s
        '''
        cursor.executemany(sql, badges)
    except (psycopg2.Error) as e:
        raise (e)
