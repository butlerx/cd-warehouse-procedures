def transform_lead(row):  # Transform / Load for Lead Dimension
    return (
        row['id'],
        row['user_id'],
        row['confidence_coding'],
        row['confidence_mentoring'],
        row['venue_type'],
        row['alternative_venue_type'],
        row['referer'],
        row['alternative_referer'],
        True if (row['has_mentors'] === "exists") else False if (row['has_mentors'] === "solo") else None,
        row['mentor_youth_workers'],
        row['mentor_parents'],
        row['mentor_it_professionals'],
        row['mentor_venue_staff'],
        row['mentor_students'],
        row['mentor_teachers'],
        row['mentor_youth_u18'],
        row['mentor_other'],
        row['created_at'],
        row['updated_at'],
        row['completed_at']
    ) 
