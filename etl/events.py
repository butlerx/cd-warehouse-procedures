def transform_event(row):  # Transform / Load for Event Dimension
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
    is_eb = row['eventbrite_id'] is not None
    status = row['status']

    # For fields which zen prod dbs are storing as json
    if country is not 'Unknown':
        country = country['countryName']

    if city is not 'Unknown':
        if 'toponymName' in city:
            city = city['toponymName']
        else:
            city = city['nameWithHierarchy']

    return (event_id, recurring_type, country, city, created_at, event_type,
            dojo_id, public, status)
