def transform_lead(row):  # Transform / Load for Lead Dimension
    completion = 0
    if (row['champion_is_valid'] == 'true'):
        completion += 1
    if (row['dojo_is_valid'] == 'true'):
        completion += 10
    if (row['venue_is_valid'] == 'true'):
        completion += 100
    if (row['team_is_valid'] == 'true'):
        completion += 1000
    if (row['charter_is_valid'] == 'true'):
        completion += 10000
    return (
        row['id'],
        row['user_id'],
        row['confidence_coding'],
        row['confidence_mentoring'],
        row['venue_type'],
        row['alternative_venue_type'],
        row['referer'],
        row['alternative_referer'],
        True if (row['has_mentors'] == "exists") else False if (row['has_mentors'] == "solo") else None,
        bool(row['mentor_youth_workers']),
        bool(row['mentor_parents']),
        bool(row['mentor_it_professionals']),
        bool(row['mentor_venue_staff']),
        bool(row['mentor_students']),
        bool(row['mentor_teachers']),
        bool(row['mentor_youth_u18']),
        row['mentor_other'],
        True if ((row['email'] is not None and "coderdojo.com" in row['email']) or row['request_email'] == "true") else False,
        completion,
        row['created_at'],
        row['updated_at'],
        row['completed_at']
    ) 
