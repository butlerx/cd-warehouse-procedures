def transform_user(row):  # Transform / Load for User Dimension
    user_id = row['user_id']
    dob = row['dob']
    created = row['when']
    country = row['country'] if (row['country'] is not None
                                 ) and (len(row['country'])) > 0 else 'Unknown'
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

    if city is not 'Unknown':
        city = city['nameWithHierarchy']

    if roles:
        roles = roles[0] if len(roles) > 0 else 'Unknown'

    return (user_id, dob, country, city, gender, user_type, roles,
            mailing_list, created)
