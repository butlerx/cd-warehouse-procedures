def transform_dojo(row):  # Transform / Load for Dojo Dimension
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

    return (dojo_id, created_at, verified_at, stage, country, city, county,
            state, continent, tao_verified, expected_attendees, verified,
            deleted)


def link_users(row):
    link_id = row['id']
    dojo_id = row['dojo_id']
    user_id = row['user_id']
    return (link_id, dojo_id, user_id)
