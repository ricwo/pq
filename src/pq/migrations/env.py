from logging.config import fileConfig

from sqlalchemy import Connection, engine_from_config, pool, text

from alembic import context

from pq.migrations import MIGRATION_LOCK_KEY
from pq.models import Base

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# pq models metadata for autogenerate support
target_metadata = Base.metadata

# Custom version table name to match pq_ naming convention
VERSION_TABLE = "pq_schema_version"

# The tables of the models, locked before a migration in name order. A worker
# statement that reads two of them takes them in the same order.
_LOCKED_TABLES = sorted(target_metadata.tables)


def _will_migrate() -> bool:
    """Whether this run changes the schema or its version.

    False for a command that only reads (``current``, ``check``) and for an
    upgrade of a database that is already at its target, which is what a
    worker start is on every start but the first after a release.
    """
    try:
        target = context.get_revision_argument()
    except KeyError:
        return False
    if target is None:
        wanted: set[str] = set()
    elif isinstance(target, str):
        wanted = {target}
    else:
        wanted = set(target)
    return set(context.get_context().get_current_heads()) != wanted


def _lock_for_migration(connection: Connection) -> None:
    """Take every lock the migration needs before it changes anything.

    A migration that locks ``pq_tasks`` step by step (``SHARE`` for an index,
    then ``ACCESS EXCLUSIVE`` for a column) deadlocks with a transaction that
    read the table before the first step and writes to it before the second,
    and with another process running the same migration.

    The advisory lock lets one process migrate at a time. The others wait
    here, then read the version and find nothing to apply. The table locks
    are the strongest a migration takes, requested while it holds none on
    those tables, so it waits for running transactions and they do not wait
    for it. All locks end with the migration's transaction.
    """
    connection.execute(
        text("SELECT pg_advisory_xact_lock(:key)"), {"key": MIGRATION_LOCK_KEY}
    )
    for table in _LOCKED_TABLES:
        exists = connection.execute(
            text("SELECT to_regclass(:name) IS NOT NULL"), {"name": table}
        ).scalar_one()
        if exists:
            connection.execute(text(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table=VERSION_TABLE,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # Support both ini file config and programmatic config
    config_section = config.get_section(config.config_ini_section, {})
    url = config.get_main_option("sqlalchemy.url")

    if url and not config_section.get("sqlalchemy.url"):
        # URL was set programmatically, add it to config section
        config_section["sqlalchemy.url"] = url

    connectable = engine_from_config(
        config_section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table=VERSION_TABLE,
        )

        with context.begin_transaction():
            if _will_migrate():
                _lock_for_migration(connection)
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
