import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Configuración de la conexión
conn = psycopg2.connect(
    dbname='db_taller1_mine4201',
    user='user_taller1_mine4201',
    password='password',
    host='localhost',
    port='5432'
)
cur = conn.cursor()

# Leer el CSV asegurando UTF-8
df = pd.read_csv('data.csv', encoding='utf-8')

# Insertar Users (únicos)
users = df['userId'].unique()
cur.executemany(
    'INSERT INTO "User"(id) VALUES (%s) ON CONFLICT (id) DO NOTHING;',
    [(int(u),) for u in users]
)

# Insertar Movies (únicos)
movies = df[['movieId', 'title']].drop_duplicates()
cur.executemany(
    'INSERT INTO Movie(id, title) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING;',
    [ (int(row.movieId), row.title) for _, row in movies.iterrows() ]
)

# Insertar Genres únicos
genre_set = set()
for genres in df['genres']:
    genre_set.update(genres.split('|'))

genre_list = list(genre_set)
cur.executemany(
    'INSERT INTO Genre(name) VALUES (%s) ON CONFLICT (name) DO NOTHING;',
    [(genre,) for genre in genre_list if genre != '(no genres listed)']
)

# Crear diccionario genre_name -> genre_id
cur.execute('SELECT id, name FROM Genre;')
genre_dict = {name: gid for gid, name in cur.fetchall()}

# Insertar relaciones Movie_Genre
movie_genre_pairs = []
for _, row in df.iterrows():
    movie_id = int(row['movieId'])
    genres = row['genres'].split('|')
    for genre in genres:
        if genre != '(no genres listed)':
            movie_genre_pairs.append((movie_id, genre_dict[genre]))

execute_values(
    cur,
    'INSERT INTO Movie_Genre(movie_id, genre_id) VALUES %s ON CONFLICT DO NOTHING;',
    movie_genre_pairs
)

# Insertar Ratings
ratings = df[['userId', 'movieId', 'rating']].drop_duplicates()
cur.executemany(
    'INSERT INTO Rating(user_id, movie_id, value) VALUES (%s, %s, %s) ON CONFLICT (user_id, movie_id) DO NOTHING;',
    [ (int(row.userId), int(row.movieId), float(row.rating)) for _, row in ratings.iterrows() ]
)

# Commit y cerrar
conn.commit()
cur.close()
conn.close()

print("✅ Datos cargados exitosamente.")
