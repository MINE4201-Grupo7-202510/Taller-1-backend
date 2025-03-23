# Pasos para crear la base de datos con Postgres

psql -U postgres

CREATE USER user_taller1_mine4201 WITH PASSWORD 'password';

CREATE DATABASE db_taller1_mine4201 OWNER user_taller1_mine4201;

\c db_taller1_mine4201

-- Tabla User
CREATE TABLE "User" (
    id SERIAL PRIMARY KEY
);

-- Tabla Movie
CREATE TABLE Movie (
    id SERIAL PRIMARY KEY,
    title VARCHAR(1000) NOT NULL
);

-- Tabla Genre
CREATE TABLE Genre (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

-- Tabla Movie_Genre (relación muchos a muchos)
CREATE TABLE Movie_Genre (
    movie_id INT REFERENCES Movie(id) ON DELETE CASCADE,
    genre_id INT REFERENCES Genre(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

-- Tabla Rating
CREATE TABLE Rating (
    user_id INT REFERENCES "User"(id) ON DELETE CASCADE,
    movie_id INT REFERENCES Movie(id) ON DELETE CASCADE,
    value DECIMAL,
    PRIMARY KEY (user_id, movie_id)
);


GRANT INSERT, SELECT, UPDATE, DELETE ON "User" TO user_taller1_mine4201;

GRANT INSERT, SELECT, UPDATE, DELETE ON Movie TO user_taller1_mine4201;

GRANT INSERT, SELECT, UPDATE, DELETE ON Genre TO user_taller1_mine4201;

GRANT INSERT, SELECT, UPDATE, DELETE ON Movie_Genre TO user_taller1_mine4201;

GRANT INSERT, SELECT, UPDATE, DELETE ON Rating TO user_taller1_mine4201;


GRANT USAGE, SELECT, UPDATE ON SEQUENCE "User_id_seq" TO user_taller1_mine4201;

GRANT USAGE, SELECT, UPDATE ON SEQUENCE movie_id_seq TO user_taller1_mine4201;

GRANT USAGE, SELECT, UPDATE ON SEQUENCE genre_id_seq TO user_taller1_mine4201;




-- Ejecutar upload_data.py para insertar los datos de data.csv


-- Pruebas para verificar el resultado
-- Contar usuarios
SELECT COUNT(*) FROM "User";

-- Contar películas
SELECT COUNT(*) FROM Movie;

-- Contar géneros
SELECT COUNT(*) FROM Genre;

-- Contar relaciones película-género
SELECT COUNT(*) FROM Movie_Genre;

-- Contar calificaciones
SELECT COUNT(*) FROM Rating;
