"""Alembic migrations for pq database schema."""

# Key of the advisory lock that one migrating process holds at a time (see
# ``env.py``). The bytes of "pq_migra": a value an application's own advisory
# locks are unlikely to use.
MIGRATION_LOCK_KEY = 0x70715F6D69677261
