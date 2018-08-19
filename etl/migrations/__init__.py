"""reads and writes to database"""
from typing import List, Tuple

from .badges import badge, staged_badge
from .dojos import DojoMigration, UserDojoMigration
from .events import event
from .leads import lead
from .measures import measure
from .staging import staging
from .tickets import ticket
from .users import user

MIGRATIONS: List[Tuple] = [
    ("dojos", lambda _: DojoMigration),
    ("dojos", lambda _: UserDojoMigration),
    ("events", event),
    ("users", user),
    ("events", ticket),
    ("users", badge),
    ("dojos", lead),
    ("events", staging),
    ("dw", staged_badge),
    ("dw", measure),
]
