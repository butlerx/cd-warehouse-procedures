"""reads and writes to database"""
from typing import List, Tuple

from .badges import badge
from .dojos import dojo
from .events import event
from .leads import lead
from .measures import measure
from .staged_badges import staged_badge
from .staging import staging
from .tickets import ticket
from .user_dojos import user_dojo
from .users import user

MIGRATIONS: List[Tuple] = [
    ("dojos", dojo),
    ("dojos", user_dojo),
    ("events", event),
    ("users", user),
    ("events", ticket),
    ("users", badge),
    ("dojos", lead),
    ("events", staging),
    ("dw", staged_badge),
    ("dw", measure),
]
