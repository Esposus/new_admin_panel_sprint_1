CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation date DATE,
    rating FLOAT,
    type TEXT NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name TEXT NOT NULL
    description TEXT,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    genre_id uuid NOT NULL REFERENCES content.genre (id) ON DELETE CASCADE,
    film_work_id uuid NOT NULL REFERENCES content.film_work (id) ON DELETE CASCADE,
    created_at timestamp with time zone,
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    film_work_id uuid REFERENCES content.film_work (id) ON DELETE CASCADE,
    person_id uuid REFERENCES content.person (id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    created_at timestamp with time zone,
);

CREATE INDEX film_work_creation_date_idx ON content.film_work(creation_date);
CREATE UNIQUE INDEX genre_film_work_idx ON content.genre_film_work(film_work_id, genre_id);
CREATE UNIQUE INDEX film_work_person_idx ON content.person_film_work (film_work_id, person_id, role);