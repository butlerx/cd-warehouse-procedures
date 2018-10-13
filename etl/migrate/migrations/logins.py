from asyncio import gather


def transform_login(row):
    """ Transform / Load for Login Dimension"""
    return (row["id"], row["user"], row["active"], row["when"])


async def migrate_logins(users, dw_cursor):
    users.execute("SELECT * FROM sys_logins")
    dw_cursor.executemany(
        """INSERT INTO "public"."dimLogins"(
                login_id,
                user_id,
                active,
                logged_in_at)
            VALUES (%s, %s, %s, %s)""",
        await gather(*(transform_login(users) for users in users.fetchall())),
    )
    print("Inserted all users")
