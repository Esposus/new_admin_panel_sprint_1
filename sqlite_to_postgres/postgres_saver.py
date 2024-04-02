from dataclasses import fields

import psycopg2

from dataclass_models import (Filmwork, Genre, GenreFilmwork, Person,
                              PersonFilmwork)
from db_settings import DSN
from logger import logger
from sqlite_saver import sqlite_extractor


def save_to_postgres():
    models = {
        'film_work': Filmwork,
        'genre': Genre,
        'person': Person,
        'genre_film_work': GenreFilmwork,
        'person_film_work': PersonFilmwork,
    }
    with psycopg2.connect(**DSN) as conn:
        with conn.cursor() as cursor:
            for table_name, model in models.items():
                try:
                    data = sqlite_extractor(table_name)
                    instance = model(**dict(data[0]))
                    column_names = [field.name for field in fields(instance)]
                    placeholders = ', '.join(['%s'] * len(column_names))
                    args = [cursor.mogrify(f'({placeholders})', row).decode() for row in data]
                    query = f'''INSERT INTO {table_name} ({', '.join(column_names)})
                                VALUES %s ON CONFLICT (id) DO NOTHING;'''
                    cursor.execute(query, args)
                except Exception as e:
                    logger.exception(f'Ошибка при записи в таблицу {table_name}: {e}')


if __name__ == '__main__':
    save_to_postgres()
