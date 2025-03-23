from sqlalchemy import create_engine, text
import pandas as pd

# Configuración de la conexión a PostgreSQL
DATABASE_URL = "postgresql://user_taller1_mine4201:password@localhost:5432/db_taller1_mine4201"

# Crear el motor de conexión - configurado para cerrar conexiones después de cada uso
engine = create_engine(DATABASE_URL, pool_recycle=3600, pool_pre_ping=True)

# -----------------------------------------------------------------------------

# OBTENER DATOS DE LA BASE DE DATOS:

def fetch_data_from_db():
    query = """
    SELECT 
        r.user_id AS user_id,
        r.movie_id AS movie_id,
        r.value AS rating,
        m.title AS title,
        STRING_AGG(g.name, '|') AS genres
    FROM 
        Rating r
    JOIN 
        Movie m ON r.movie_id = m.id
    JOIN 
        Movie_Genre mg ON m.id = mg.movie_id
    JOIN 
        Genre g ON mg.genre_id = g.id
    GROUP BY 
        r.user_id, r.movie_id, r.value, m.title;
    """
    # Usar chunksize para procesar datos en lotes en vez de cargar todo en memoria
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)

# -----------------------------------------------------------------------------

# USUARIOS:

# Crear un nuevo usuario
def create_user(user_id: int):
    with engine.connect() as conn:
        query = text("INSERT INTO \"User\" (id) VALUES (:user_id) ON CONFLICT (id) DO NOTHING")
        conn.execute(query, {"user_id": user_id})
        conn.commit()

# Leer todos los usuarios
def get_all_users():
    with engine.connect() as conn:
        query = "SELECT * FROM \"User\""
        return pd.read_sql_query(query, conn)

# Eliminar un usuario
def delete_user(user_id: int):
    with engine.connect() as conn:
        query = text("DELETE FROM \"User\" WHERE id = :user_id")
        conn.execute(query, {"user_id": user_id})
        conn.commit()

def get_next_user_id():
    """
    Retorna el próximo id de usuario disponible (maximo actual + 1).
    Si no existen usuarios, retorna 1.
    """
    with engine.connect() as conn:
        query = text('SELECT COALESCE(MAX(id), 0) + 1 AS next_user_id FROM "User"')
        result = conn.execute(query).fetchone()
        return result[0] if result else 1

# -----------------------------------------------------------------------------

# PELÍCULAS:

# Crear una nueva película
def create_movie(movie_id: int, title: str):
    with engine.connect() as conn:
        query = text("INSERT INTO Movie (id, title) VALUES (:movie_id, :title) ON CONFLICT (id) DO NOTHING")
        conn.execute(query, {"movie_id": movie_id, "title": title})
        conn.commit()

# Leer todas las películas
def get_all_movies():
    with engine.connect() as conn:
        query = "SELECT * FROM Movie"
        return pd.read_sql_query(query, conn)

# Actualizar el título de una película
def update_movie(movie_id: int, new_title: str):
    with engine.connect() as conn:
        query = text("UPDATE Movie SET title = :new_title WHERE id = :movie_id")
        conn.execute(query, {"movie_id": movie_id, "new_title": new_title})
        conn.commit()

# Eliminar una película
def delete_movie(movie_id: int):
    with engine.connect() as conn:
        query = text("DELETE FROM Movie WHERE id = :movie_id")
        conn.execute(query, {"movie_id": movie_id})
        conn.commit()

# -----------------------------------------------------------------------------

# GÉNEROS:

# Crear un nuevo género
def create_genre(genre_name: str):
    with engine.connect() as conn:
        query = text("INSERT INTO Genre (name) VALUES (:genre_name) ON CONFLICT (name) DO NOTHING")
        conn.execute(query, {"genre_name": genre_name})
        conn.commit()

# Leer todos los géneros
def get_all_genres():
    with engine.connect() as conn:
        query = "SELECT * FROM Genre"
        return pd.read_sql_query(query, conn)

# Eliminar un género
def delete_genre(genre_name: str):
    with engine.connect() as conn:
        query = text("DELETE FROM Genre WHERE name = :genre_name")
        conn.execute(query, {"genre_name": genre_name})
        conn.commit()

# -----------------------------------------------------------------------------

# Movie-Genre:

# Asociar una película con un género
def link_movie_to_genre(movie_id: int, genre_name: str):
    with engine.connect() as conn:
        # Obtener el ID del género
        genre_query = text("SELECT id FROM Genre WHERE name = :genre_name")
        genre_result = conn.execute(genre_query, {"genre_name": genre_name}).fetchone()
        if not genre_result:
            raise ValueError(f"Género '{genre_name}' no encontrado")
        genre_id = genre_result[0]

        # Insertar la relación
        query = text("INSERT INTO Movie_Genre (movie_id, genre_id) VALUES (:movie_id, :genre_id) ON CONFLICT DO NOTHING")
        conn.execute(query, {"movie_id": movie_id, "genre_id": genre_id})
        conn.commit()

# Leer todas las relaciones película-género
def get_all_movie_genres():
    with engine.connect() as conn:
        query = """
        SELECT m.title AS movie_title, g.name AS genre_name
        FROM Movie_Genre mg
        JOIN Movie m ON mg.movie_id = m.id
        JOIN Genre g ON mg.genre_id = g.id
        """
        return pd.read_sql_query(query, conn)

# Eliminar una relación película-género
def unlink_movie_from_genre(movie_id: int, genre_name: str):
    with engine.connect() as conn:
        # Obtener el ID del género
        genre_query = text("SELECT id FROM Genre WHERE name = :genre_name")
        genre_result = conn.execute(genre_query, {"genre_name": genre_name}).fetchone()
        if not genre_result:
            raise ValueError(f"Género '{genre_name}' no encontrado")
        genre_id = genre_result[0]

        # Eliminar la relación
        query = text("DELETE FROM Movie_Genre WHERE movie_id = :movie_id AND genre_id = :genre_id")
        conn.execute(query, {"movie_id": movie_id, "genre_id": genre_id})
        conn.commit()

# -----------------------------------------------------------------------------

# RATINGS:

# Crear una nueva calificación
def create_rating(user_id: int, movie_id: int, value: float):
    with engine.connect() as conn:
        query = text("INSERT INTO Rating (user_id, movie_id, value) VALUES (:user_id, :movie_id, :value) ON CONFLICT (user_id, movie_id) DO UPDATE SET value = EXCLUDED.value")
        conn.execute(query, {"user_id": user_id, "movie_id": movie_id, "value": value})
        conn.commit()

# Leer todas las calificaciones
def get_all_ratings():
    with engine.connect() as conn:
        query = "SELECT * FROM Rating"
        return pd.read_sql_query(query, conn)

# Actualizar una calificación
def update_rating(user_id: int, movie_id: int, new_value: float):
    with engine.connect() as conn:
        query = text("UPDATE Rating SET value = :new_value WHERE user_id = :user_id AND movie_id = :movie_id")
        conn.execute(query, {"user_id": user_id, "movie_id": movie_id, "new_value": new_value})
        conn.commit()

# Eliminar una calificación
def delete_rating(user_id: int, movie_id: int):
    with engine.connect() as conn:
        query = text("DELETE FROM Rating WHERE user_id = :user_id AND movie_id = :movie_id")
        conn.execute(query, {"user_id": user_id, "movie_id": movie_id})
        conn.commit()

# Función para obtener los ratings de un usuario junto a la información de la película
def get_ratings_by_user(user_id: int):
    with engine.connect() as conn:
        query = """
        SELECT 
            r.user_id,
            r.movie_id,
            r.value AS rating,
            m.title,
            STRING_AGG(g.name, ' | ') AS genres
        FROM Rating r
        JOIN Movie m ON r.movie_id = m.id
        LEFT JOIN Movie_Genre mg ON m.id = mg.movie_id
        LEFT JOIN Genre g ON mg.genre_id = g.id
        WHERE r.user_id = :user_id
        GROUP BY r.user_id, r.movie_id, r.value, m.title;
        """
        return pd.read_sql_query(text(query), conn, params={"user_id": user_id})



def get_movie_genres_by_id(movie_id: int):
    with engine.connect() as conn:
        query = """
        SELECT g.name
        FROM Movie_Genre mg
        JOIN Genre g ON mg.genre_id = g.id
        WHERE mg.movie_id = :movie_id
        """
        result = conn.execute(text(query), {"movie_id": movie_id}).fetchall()
        return [row[0] for row in result]