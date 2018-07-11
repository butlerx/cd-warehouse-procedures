import uuid


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
    checked_in = args['checked_in']
    return (dojo_id, ticket_id, event_id, user_id, time, location_id, id,
            badge_id, checked_in)
