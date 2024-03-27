import os
from dataclasses import fields

import psycopg2
from dotenv import load_dotenv

from dataclass_models import (
    Filmwork,
    Genre,
    Person,
    GenreFilmwork,
    PersonFilmwork,
)
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
    data_models = {
        'film_work': Filmwork,
        'genre': Genre,
        'person': Person,
        'genre_film_work': GenreFilmwork,
        'person_film_work': PersonFilmwork,
    }
    with psycopg2.connect(**dsn) as conn, conn.cursor() as cursor:
        for table_name, model in data_models.items():
            data = sqlite_extractor(table_name)
            instance = model(**dict(data[0]))
            column_names = [field.name for field in fields(instance)]
            column_names_str = ', '.join(column_names)
            placeholders = ', '.join(['%s'] * len(column_names))
            args = ','.join(cursor.mogrify(f'({placeholders})', row).decode() for row in data)
            cursor.execute(
                f'INSERT INTO {table_name} ({column_names_str}) VALUES {args};'
                f'ON CONFLICT (id) DO NOTHING;'
            )
