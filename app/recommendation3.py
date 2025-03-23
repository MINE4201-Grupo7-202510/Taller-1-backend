#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pickle
import pandas as pd
import heapq
from surprise import Dataset, Reader, KNNBasic
from app.database import fetch_data_from_db

algo = None
data = None
movie_info = None
rating_matrix = None
movie_genres_cache = {}


def load_data():
    """
    Carga los datos desde la base de datos y los preprocesa.
    
    Este método obtiene los datos de interacción usuario-película desde la base 
    de datos, los preprocesa para adaptarlos al formato requerido, y prepara
    estructuras de datos para acceso rápido durante la generación de recomendaciones.
    
    Returns:
        tuple: Una tupla que contiene:
            - df (pandas.DataFrame): DataFrame con los datos de ratings (userId, movieId, rating)
            - movie_info_local (dict): Diccionario con información de películas {movieId: {title, genres}}
            - rating_matrix_local (pandas.DataFrame): Matriz pivotada de ratings (filas=usuarios, columnas=películas)
    """
    df = fetch_data_from_db()
    df = df.rename(columns={"user_id": "userId", "movie_id": "movieId", "value": "rating"})
    df["userId"] = df["userId"].astype(int)
    df["movieId"] = df["movieId"].astype(int)

    movie_info_local = (
        df.drop_duplicates(subset=["movieId"])
          .set_index("movieId")[["title", "genres"]]
          .to_dict(orient="index")
    )

    rating_matrix_local = df.pivot_table(index="userId", columns="movieId", values="rating")
    
    global movie_genres_cache
    for movie_id, info in movie_info_local.items():
        if "genres" in info:
            movie_genres_cache[movie_id] = set(info["genres"].split("|"))
            
    return df, movie_info_local, rating_matrix_local



def retrain_model() -> KNNBasic:
    """
    Reentrena el modelo usando los datos actuales de la base de datos.
    
    Este método realiza las siguientes operaciones:
    1. Recarga los datos actualizados de la BD mediante load_data()
    2. Configura un Reader para interpretar los ratings (escala 0.5-5.0)
    3. Prepara los datos en formato compatible con Surprise
    4. Configura y entrena un modelo KNNBasic item-based con correlación de Pearson
    5. Serializa el modelo entrenado para uso futuro
    
    Los hiperparámetros del modelo son:
    - k=10: Número máximo de vecinos a considerar para cada ítem
    - min_k=3: Número mínimo de vecinos requeridos para hacer una predicción
    - sim_options: Configuración de la métrica de similitud (Pearson, item-based)
    
    Returns:
        KNNBasic: Modelo de recomendación entrenado con los datos actuales
        
    Side effects:
        - Actualiza las variables globales data, movie_info, rating_matrix y algo
        - Guarda el modelo entrenado en el archivo 'recommender.pkl'
    """
    global data, movie_info, rating_matrix, algo, movie_genres_cache

    data, movie_info, rating_matrix = load_data()

    reader = Reader(rating_scale=(0.5, 5.0))
    surprise_data = Dataset.load_from_df(data[["userId", "movieId", "rating"]], reader)
    trainset = surprise_data.build_full_trainset()
    sim_options = {
        "name": "pearson",
        "user_based": False 
    }

    algo = KNNBasic(k=10, min_k=3, sim_options=sim_options, verbose=False)
    algo.fit(trainset)
    pkl_filename = "recommender.pkl"
    with open(pkl_filename, "wb") as f:
        pickle.dump(algo, f)
    
    print("Modelo entrenado y guardado correctamente en recommender.pkl")
    return algo



def load_model():
    """
    Carga el modelo desde el archivo recommender.pkl o entrena uno nuevo si no existe.
    
    Este método intenta cargar un modelo previamente entrenado desde el archivo
    recommender.pkl para evitar el costoso proceso de reentrenamiento. Si el archivo
    no existe o hay errores durante la carga, se entrena un modelo nuevo.
    
    El proceso sigue estos pasos:
    1. Verifica si existe el archivo recommender.pkl
    2. Si existe, intenta cargar el modelo desde el archivo
    3. Si no existe o hay error en la carga, entrena un nuevo modelo con retrain_model()
    
    Side effects:
        - Actualiza la variable global 'algo' con el modelo cargado o entrenado
        - Imprime mensajes informativos sobre el proceso de carga/entrenamiento
    """
    global algo
    pkl_filename = "recommender.pkl"
    
    if os.path.exists(pkl_filename):
        try:
            print(f"Cargando modelo desde {pkl_filename}")
            with open(pkl_filename, "rb") as f:
                algo = pickle.load(f)
            print("Modelo cargado correctamente")
        except Exception as e:
            print(f"Error al cargar el modelo: {str(e)}")
            print("Entrenando nuevo modelo...")
            algo = retrain_model()
    else:
        print(f"No se encontró el archivo {pkl_filename}")
        print("Entrenando nuevo modelo...")
        algo = retrain_model()



def get_recommendations(user_id: int, top_n: int = 5) -> list:
    """
    Genera recomendaciones personalizadas con explicaciones para un usuario específico.
    
    Esta función predice qué películas podrían gustarle a un usuario basándose en sus
    valoraciones previas y en el comportamiento de usuarios similares. Además, genera
    explicaciones personalizadas para cada recomendación basadas en:
    1. Películas similares que le gustaron al usuario
    2. Géneros que el usuario ha mostrado preferir
    
    El proceso de recomendación sigue estos pasos:
    1. Verifica que el modelo esté cargado y el usuario exista en el dataset
    2. Identifica las películas que el usuario no ha valorado
    3. Predice ratings para estas películas no valoradas
    4. Selecciona las top_n películas con mayor predicción de rating
    5. Genera explicaciones personalizadas para cada recomendación
    
    Parameters:
        user_id (int): Identificador del usuario para el que se generan las recomendaciones
        top_n (int, optional): Número de recomendaciones a generar. Por defecto es 5.
        
    Returns:
        list: Lista de diccionarios con las recomendaciones. Cada diccionario contiene:
            - movieId (int): Identificador de la película
            - title (str): Título de la película
            - genres (str): Géneros de la película separados por '|'
            - predicted_rating (float): Rating predicho redondeado a 2 decimales
            - explanation (str): Explicación personalizada de la recomendación
            
    Raises:
        ValueError: Si el usuario no se encuentra en el dataset de ratings
    """
    global algo, rating_matrix, movie_info
    
    if algo is None:
        load_model()
        
    if user_id not in rating_matrix.index:
        raise ValueError("Usuario no encontrado en el dataset de ratings")
    
    user_ratings = rating_matrix.loc[user_id]
    rated_movies = user_ratings.dropna()
    movies_no_valoradas = user_ratings[user_ratings.isna()].index.tolist()
    
    liked_movies = rated_movies[rated_movies >= 3.5]
    liked_movie_ids = liked_movies.index.tolist()
    
    predictions = []
    for movie_id in movies_no_valoradas:
        try:
            pred = algo.predict(user_id, movie_id)
            if pred.est >= 3.0:
                predictions.append((movie_id, pred.est))
        except:
            continue
    
    top_recommendations = heapq.nlargest(top_n, predictions, key=lambda x: x[1])
    
    best_similarities = {} 
    
    recommendations = []
    for movie_id, pred_rating in top_recommendations:
        explanation = ""
        
        try:
            inner_movie_id = algo.trainset.to_inner_iid(movie_id)
            
            if movie_id not in best_similarities:
                movie_sims = []
                for liked_id in liked_movie_ids:
                    try:
                        inner_liked_id = algo.trainset.to_inner_iid(liked_id)
                        sim = algo.sim[inner_movie_id, inner_liked_id]
                        if sim > 0.1:
                            movie_sims.append((liked_id, sim))
                    except:
                        continue
                
                best_similarities[movie_id] = heapq.nlargest(3, movie_sims, key=lambda x: x[1])
            
            similar_movies = best_similarities[movie_id]
            
            if similar_movies:
                if len(similar_movies) == 1:
                    similar_id, sim = similar_movies[0]
                    sim_title = movie_info.get(similar_id, {}).get("title", "una película similar")
                    sim_rating = rated_movies[similar_id]
                    explanation = f"Te recomendamos esta película porque te gustó {sim_title} (valoración: {sim_rating:.1f})."
                else:
                    sim_texts = []
                    for similar_id, _ in similar_movies:
                        sim_title = movie_info.get(similar_id, {}).get("title", "una película similar")
                        sim_rating = rated_movies[similar_id]
                        sim_texts.append(f"{sim_title} (valoración: {sim_rating:.1f})")
                    
                    explanation = f"Te recomendamos esta película porque te gustaron películas similares como {', '.join(sim_texts[:-1])} y {sim_texts[-1]}."
            
            if not explanation:
                movie_genres = movie_genres_cache.get(movie_id, set())
                if not movie_genres and movie_id in movie_info:
                    genres_str = movie_info[movie_id].get("genres", "")
                    movie_genres = set(genres_str.split("|")) if genres_str else set()
                    movie_genres_cache[movie_id] = movie_genres
                
                if movie_genres:
                    user_genre_ratings = {}
                    for rated_id in liked_movie_ids:
                        if rated_id in movie_genres_cache:
                            for genre in movie_genres_cache[rated_id]:
                                if genre:  # Ignorar géneros vacíos
                                    user_genre_ratings[genre] = user_genre_ratings.get(genre, 0) + 1
                    
                    matching_genres = [genre for genre in movie_genres if genre in user_genre_ratings and genre]
                    
                    if matching_genres:
                        explanation = f"Te recomendamos esta película porque contiene géneros que sueles disfrutar: {', '.join(matching_genres)}."
        except:
            pass
        
        if not explanation:
            explanation = "Esta película ha sido bien valorada por usuarios con gustos similares a los tuyos."
        
        info = movie_info.get(movie_id, {"title": "Desconocido", "genres": ""})
        
        recommendations.append({
            "movieId": movie_id,
            "title": info["title"],
            "genres": info["genres"],
            "predicted_rating": round(pred_rating, 2),
            "explanation": explanation
        })
    
    return recommendations

data, movie_info, rating_matrix = load_data()
load_model()


# -----------------------------------------------------------------------------
# Código de prueba para ejecución directa del módulo
# -----------------------------------------------------------------------------

if __name__ == "__main__":

    user_test = 1 
    try:
        recomendaciones = get_recommendations(user_test, top_n=5)
        print(f"Recomendaciones para el usuario {user_test}:")
        for rec in recomendaciones:
            print(f"- {rec['title']} (Rating predicho: {rec['predicted_rating']})")
            print(f"  Géneros: {rec['genres']}")
            print(f"  Explicación: {rec['explanation']}")
            print()
    except ValueError as ve:
        print(ve)