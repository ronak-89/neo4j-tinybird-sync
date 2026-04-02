"""
Neo4j and PostgreSQL connections for the backfill job.

Uses env vars only (same DB_* pattern as migrate_notification_error_logs).
"""
import os

import psycopg2
from neo4j import GraphDatabase

DB_CONNECTION_TIMEOUT = int(os.getenv("DB_CONNECTION_TIMEOUT", "30"))


def get_db_conn_sync():
    """Return a synchronous PostgreSQL connection (caller closes)."""
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_DATABASE")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    missing = [n for n, v in (
        ("DB_HOST", host),
        ("DB_DATABASE", database),
        ("DB_USER", user),
        ("DB_PASSWORD", password),
    ) if not v]
    if missing:
        raise RuntimeError(f"Missing required PostgreSQL env vars: {', '.join(missing)}")

    return psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=os.getenv("DB_PORT", "5432"),
        connect_timeout=DB_CONNECTION_TIMEOUT,
    )


class Neo4jConnection:
    """Thin wrapper so callers can use neo4j_connection.connect() like shared libs."""

    def connect(self):
        uri = os.getenv("NEO4J_URI")
        if not uri:
            raise RuntimeError("NEO4J_URI is required")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "")
        if not password:
            raise RuntimeError("NEO4J_PASSWORD is required")

        return GraphDatabase.driver(uri, auth=(user, password))


neo4j_connection = Neo4jConnection()
