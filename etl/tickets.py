def transform_ticket(row):
    ticket_id = row['ticket_id']
    ticket_type = row['type']
    quantity = row['quantity']
    deleted = row['deleted']

    return (ticket_id, ticket_type, quantity, deleted)
