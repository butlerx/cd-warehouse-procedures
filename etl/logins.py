def transform_login(row):  # Transform / Load for Login Dimension
    login_id = row['id']
    user_id = row['user']
    active = row['active']
    logged_in_at = row['when']

    return (login_id, user_id, active, logged_in_at)
