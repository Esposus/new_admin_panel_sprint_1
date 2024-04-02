import sqlite3
from contextlib import contextmanager


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def sqlite_extractor(table_name):
    with conn_context('db.sqlite') as conn:
        with conn.cursor() as curs:
            curs.execute(f"SELECT * FROM {table_name};")
            return curs.fetchmany(size=1000)
