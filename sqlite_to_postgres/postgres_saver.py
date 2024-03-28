import os
from dataclasses import fields

from dataclass_models import (Filmwork, Genre, GenreFilmwork, Person,
                              PersonFilmwork)
from dotenv import load_dotenv
from icecream import ic
from psycopg2.pool import SimpleConnectionPool
from sqlite_saver import sqlite_extractor

load_dotenv()

dsn_ = {
    'dbname': 'movies_database',
    'user': 'app',
    'password': '123qwe',
    'host': 'localhost',
    'port': 5432,
    'options': '-c search_path=content',
}

dsn = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432),
    'options': os.environ.get('DB_OPTIONS', '-c search_path=content'),
}


def save_to_postgres():
    models = {
        'film_work': Filmwork,
        'genre': Genre,
        'person': Person,
        'genre_film_work': GenreFilmwork,
        'person_film_work': PersonFilmwork,
    }
    pool = SimpleConnectionPool(**dsn)
    with pool.getconn() as conn, conn.cursor() as cursor:
        for table_name, model in models.items():
            try:
                data = sqlite_extractor(table_name)
                instances = (model(**dict(row)) for row in data)
                for instance in instances:
                    column_names = [field.name for field in fields(instance)]
                    placeholders = ', '.join(['%s'] * len(column_names))
                    args = [cursor.mogrify(f'({placeholders})', row).decode() for row in data]
                    query = f'''INSERT INTO {table_name} ({', '.join(column_names)})
                                VALUES %s ON CONFLICT (id) DO NOTHING;'''
                    cursor.execute(query, args)
            except Exception as e:
                ic(f'Ошибка при записи в таблицу {table_name}: {e}')
