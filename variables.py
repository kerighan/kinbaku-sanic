import os

VERSION = "0.0.0"
DATABASE = os.environ.get("KINBAKU_DB", "db/graph.db")
TTL = os.environ.get("KINBAKU_TTL", 60)
