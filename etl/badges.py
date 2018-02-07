import uuid


def transform_badges(row):
    user_id = row['user_id']
    badges = row['badges']
    transformed_badges = []
    for element in badges:
        id = element['id']
        archived = element['archived']
        type = element['type']
        name = element['name']
        issued_on = element['assertion']['issuedOn']
        badge_id = str(uuid.uuid4())
        data = (id, archived, type, name, badge_id, user_id, issued_on)
        transformed_badges.append(data)
    return transformed_badges


def add_badges(rows):
    badges = []
    for row in rows:
        user_id = row['user_id']
        badge_id = row['badge_id']
        data = (badge_id, user_id)
        badges.append(data)
    return badges
