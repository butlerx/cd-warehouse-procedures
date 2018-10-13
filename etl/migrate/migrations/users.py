from asyncio import gather


async def transform_user(row):
    """ Transform / Load for User Dimension"""

    return (
        row["user_id"],
        row["dob"],
        row["country"]
        if (row["country"] is not None)
        and row["country"]
        and ("countryName" in row["country"])
        else "Unknown",
        row["city"]["nameWithHierarchy"]
        if (row["city"] is not None)
        and row["city"]
        and ("nameWithHierarchy" in row["city"])
        else "Unknown",
        row["gender"] if (row["gender"] is not None) and row["gender"] else "Unknown",
        row["user_type"],
        row["roles"][0]
        if row["roles"]
        else "Unknown"
        if row["roles"]
        else row["roles"],
        row["mailing_list"],
        row["when"],
    )


async def migrate_users(cursor, dw_cursor):
    cursor.execute(
        """SELECT *
        FROM cd_profiles
        INNER JOIN sys_user ON cd_profiles.user_id = sys_user.id;"""
    )
    dw_cursor.executemany(
        """INSERT INTO "public"."dimUsers"(
            user_id,
            dob,
            country,
            city,
            gender,
            user_type,
            roles,
            mailing_list,
            created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        await gather(*(transform_user(users) for users in cursor.fetchall())),
    )
    print("Inserted all users")
