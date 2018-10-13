import json


class Config:
    def __init__(self, path: str) -> None:
        with open(path) as data:
            self.data = json.load(data)
            self.databases = Databases(self.data["databases"])
            self.postgres = Postgres(self.data["postgres"])
            self.aws_s3 = AWS_S3(self.data["s3"])


class Databases:
    def __init__(self, databases: dict) -> None:
        self.databases = databases
        self.data = [self.dojos, self.users, self.events, self.warehouse]
        self.index = len(self.data)

    @property
    def dojos(self):
        return self.databases["dojos"]

    @property
    def users(self):
        return self.databases["users"]

    @property
    def events(self):
        return self.databases["events"]

    @property
    def warehouse(self):
        return self.databases["dw"]

    def _asdict(self):
        return self.data

    def __iter__(self):
        return self

    def __next__(self):
        if self.index == len(self.data):
            raise StopIteration
        self.index += 1
        return self.data[self.index]


class Postgres:
    def __init__(self, database: dict) -> None:
        self.database = database

    @property
    def host(self):
        return self.database["host"]

    @property
    def user(self):
        return self.database["user"]

    @property
    def password(self):
        return self.database["password"]


class AWS_S3:
    def __init__(self, conf: dict) -> None:
        self.conf = conf

    @property
    def bucket(self):
        return self.conf["bucket"]

    @property
    def access(self):
        return self.conf["access"]

    @property
    def secret(self):
        return self.conf["secret"]
