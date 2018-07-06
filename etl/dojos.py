def transform_dojo(row):  # Transform / Load for Dojo Dimension
    dojo_id = row['id']
    created_at = row['created']
    verified_at = row['verified_at']
    stage = row['stage']

    # These ones are ugly the data base stores them in json originally
    country = row['country']['countryName'] if (
        row['country'] is
        not None) and (len(row['country'])) > 0 else 'Unknown'
    city = row['place']['name'] if (
        row['city'] is not None) and (len(row['city'])) > 0 else 'Unknown'
    county = row['county']['toponymName'] if (
        row['county'] is not None) and (len(row['county'])) > 0 else 'Unknown'
    state = row['state']['toponymName'] if (
        row['state'] is not None) and (len(row['state'])) > 0 else 'Unknown'

    continent = row['continent']
    tao_verified = row['tao_verified']
    expected_attendees = row['expected_attendees'] if (
        row['expected_attendees'] is
        not None) else 0  # Maybe something other than 0????
    verified = row['verified']
    inactive = 1 if (row['stage'] == 4) else 0
    inactive_at = row['inactive_at']
    is_eb = 1 if row['eventbrite_token'] and row['eventbrite_wh_id'] else 0
    dojo_lead_id = row['dojo_lead_id']
    deleted = row['deleted']

    return (dojo_id, created_at, verified_at, stage, country, city, county,
            state, continent, tao_verified, expected_attendees, verified,
            deleted, inactive, inactive_at, is_eb, dojo_lead_id)


def link_users(row):
    link_id = row['id']
    dojo_id = row['dojo_id']
    user_id = row['user_id']
    user_type = row['user_type']
    return (link_id, user_id, dojo_id, user_type)
