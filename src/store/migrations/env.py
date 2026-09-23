import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool, text

# Importing every model is what makes `Base.metadata` the whole `smart_files` schema;
# `include_object` below trusts it to decide what Alembic owns.
import store.models  # noqa: F401
from db import Base

config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata
managed_tables = {(table.schema, table.name) for table in target_metadata.tables.values()}
version_table_schema = "smart_files"


def sqlalchemy_url(url: str) -> str:
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


if database_url := os.getenv("DATABASE_URL"):
    config.set_main_option("sqlalchemy.url", sqlalchemy_url(database_url))


def include_object(object_, name, type_, _reflected, _compare_to) -> bool:
    """Keep tables owned by other storage modules out of generated migrations."""
    if type_ == "table":
        return (object_.schema, name) in managed_tables
    return True


def run_migrations_offline() -> None:
    context.configure(
        url=sqlalchemy_url(config.get_main_option("sqlalchemy.url")),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_object=include_object,
        version_table_schema=version_table_schema,
    )
    context.execute("CREATE SCHEMA IF NOT EXISTS smart_files")
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    supplied_connection = config.attributes.get("connection")
    if supplied_connection is not None:
        supplied_connection.execute(text("CREATE SCHEMA IF NOT EXISTS smart_files"))
        context.configure(
            connection=supplied_connection,
            target_metadata=target_metadata,
            include_schemas=True,
            include_object=include_object,
            version_table_schema=version_table_schema,
        )
        with context.begin_transaction():
            context.run_migrations()
        return

    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = sqlalchemy_url(section["sqlalchemy.url"])
    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        # The role is also called `smart_files`, so the default `"$user", public`
        # search_path makes `smart_files` the current schema. Reflection then drops the
        # qualification from anything in it, and autogenerate reads every foreign key as
        # moved between schemas. Every migration names its schema, so taking the managed
        # schema off the path costs nothing and keeps the comparison honest.
        connect_args={"options": "-csearch_path=public"},
    )
    with connectable.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS smart_files"))
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            include_object=include_object,
            version_table_schema=version_table_schema,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
