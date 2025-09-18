import os
from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
import urllib.parse

from db.db_sqlserver import Base  # import your models
# optional: import other model modules so they are registered

config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url():
    conn = os.environ.get("SQLSERVER_CONN") or os.environ.get("DATABASE_URL")
    if not conn:
        raise ValueError("Set SQLSERVER_CONN (or pass conn_str). Example: mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+18+for+SQL+Server")
    params = urllib.parse.quote_plus(conn)
    sanitized_conn = "mssql+pyodbc:///?odbc_connect=%s" % params
    return sanitized_conn

def include_object(object, name, type_, reflected, compare_to):
    # Ignore specific columns by name
    if type_ == "column" and name in {"ValidFrom", "ValidTo"}:
        return False
    if type_ == "table" and name in {"email_verifications", "email_subscriptions"}:
        return False
    return True


def run_migrations_offline():
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        compare_server_default=True,
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    configuration = config.get_section(config.config_ini_section)
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            include_object=include_object,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
