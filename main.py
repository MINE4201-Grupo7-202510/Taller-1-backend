#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Archivo principal de FastAPI que expone los endpoints para el sistema de
recomendación y gestión de datos.

Este módulo implementa una API RESTful utilizando FastAPI para:
- Gestionar usuarios, películas, géneros y calificaciones
- Proporcionar recomendaciones personalizadas de películas basadas en el historial de usuarios
- Administrar las relaciones entre películas y géneros
- Entrenar y actualizar el modelo de recomendación
"""
from fastapi import FastAPI, HTTPException, Body
import uvicorn
from typing import List
from pydantic import BaseModel
from app.recommendation3 import get_recommendations, retrain_model
from app.database import (
    fetch_data_from_db, 
    create_user, get_all_users, delete_user, 
    create_movie, get_all_movies,
    get_ratings_by_user, update_movie, delete_movie,
    create_genre, get_all_genres, delete_genre,
    link_movie_to_genre, get_all_movie_genres, unlink_movie_from_genre,
    create_rating, get_all_ratings, update_rating, delete_rating,
    get_next_user_id, get_movie_genres_by_id
)
from fastapi.middleware.cors import CORSMiddleware

# MODELOS

class User(BaseModel):
    """
    Modelo para representar un usuario en el sistema.
    
    Attributes:
        id (int): Identificador único del usuario.
    """
    id: int

class Movie(BaseModel):
    """
    Modelo para representar una película en el sistema.
    
    Attributes:
        id (int): Identificador único de la película.
        title (str): Título de la película.
    """
    id: int
    title: str

class Genre(BaseModel):
    """
    Modelo para representar un género cinematográfico.
    
    Attributes:
        name (str): Nombre del género (p.ej. Acción, Comedia, Drama, etc.).
    """
    name: str

class MovieGenre(BaseModel):
    """
    Modelo para representar la relación entre una película y un género.
    
    Attributes:
        movie_id (int): Identificador de la película.
        genre_name (str): Nombre del género al que pertenece la película.
    """
    movie_id: int
    genre_name: str

class Rating(BaseModel):
    """
    Modelo para representar una calificación dada por un usuario a una película.
    
    Attributes:
        user_id (int): Identificador del usuario que califica.
        movie_id (int): Identificador de la película calificada.
        value (float): Valor de la calificación (normalmente entre 0.5 y 5.0).
    """
    user_id: int
    movie_id: int
    value: float

class RatingResponse(BaseModel):
    """
    Modelo para representar la respuesta de una calificación con información adicional.
    
    Attributes:
        user_id (int): Identificador del usuario que calificó.
        movie_id (int): Identificador de la película calificada.
        rating (float): Valor de la calificación otorgada.
        title (str): Título de la película calificada.
        genres (str): Cadena de texto con los géneros asociados a la película.
    """
    user_id: int
    movie_id: int
    rating: float
    title: str
    genres: str




app = FastAPI(title="Sistema de Recomendación con Surprise y FastAPI")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/recommend/{user_id}")
def recommend(user_id: int):
    """
    Recibe el user_id y retorna toda la información necesaria para el frontend:
    - Historial de ratings del usuario.
    - Recomendaciones item-based (lista de películas con su predicted_rating y explicación).
    
    Parameters:
        user_id (int): Identificador único del usuario para el que se generarán recomendaciones.
        
    Returns:
        dict: Un diccionario con el ID del usuario, su historial de calificaciones, 
              las recomendaciones generadas y un indicador de si son personalizadas.
              
    Raises:
        HTTPException: 404 si el usuario no existe, 500 si ocurre algún otro error.
    """
    try:
        user_df = get_all_users()
        if not any(user_df['id'] == user_id):
            raise HTTPException(status_code=404, detail="Usuario no encontrado en la base de datos")
        
        ratings_df = get_ratings_by_user(user_id)
        rating_history = ratings_df.to_dict(orient="records") if not ratings_df.empty else []
        
        if len(rating_history) == 0:
            movies_df = get_all_movies().head(5)
            non_personalized_recs = []
            
            for _, movie in movies_df.iterrows():
                movie_genres = get_movie_genres_by_id(movie['id'])
                genres_str = " | ".join(movie_genres) if movie_genres else "Sin géneros"
                
                non_personalized_recs.append({
                    "movieId": movie['id'],
                    "title": movie['title'],
                    "genres": genres_str,
                    "predicted_rating": 3.5,
                    "non_personalized": True,
                    "explanation": "Esta es una recomendación general no personalizada. Califica más películas para obtener recomendaciones personalizadas."
                })
            
            return {
                "userId": user_id,
                "ratingsHistory": [],
                "recommendations": non_personalized_recs,
                "personalized": False
            }
        
        try:
            recs = get_recommendations(user_id, top_n=5)
            for rec in recs:
                rec["non_personalized"] = False
        except ValueError as ve:
            raise HTTPException(status_code=404, detail=str(ve))
        
        return {
            "userId": user_id,
            "ratingsHistory": rating_history,
            "recommendations": recs,
            "personalized": True
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener recomendaciones: {str(e)}")
    


@app.post("/retrain")
def retrain():
    """
    Endpoint para reentrenar el modelo con los datos actualizados de la BD.
    
    Este proceso toma todos los datos actuales de calificaciones y reconstruye 
    el modelo de recomendación para reflejar los cambios recientes en la base de datos.
    
    Returns:
        dict: Un mensaje indicando el estado de la operación.
        
    Raises:
        HTTPException: 500 si ocurre un error durante el reentrenamiento.
    """
    try:
        retrain_model()
        return {"status": "success", "message": "Modelo reentrenado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al reentrenar el modelo: {str(e)}")


@app.post("/users/", response_model=dict)
def add_user(user: User):
    """
    Crear un nuevo usuario en el sistema.
    
    Parameters:
        user (User): Objeto con la información del usuario a crear.
        
    Returns:
        dict: Un mensaje indicando el resultado de la operación.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la creación.
    """
    try:
        create_user(user.id)
        return {"status": "success", "message": f"Usuario {user.id} creado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al crear usuario: {str(e)}")

@app.get("/users/", response_model=List[dict])
def get_users():
    """
    Obtener todos los usuarios registrados en el sistema.
    
    Returns:
        List[dict]: Lista de diccionarios, cada uno con la información de un usuario.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la consulta.
    """
    try:
        users_df = get_all_users()
        return users_df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener usuarios: {str(e)}")

@app.delete("/users/{user_id}", response_model=dict)
def remove_user(user_id: int):
    """
    Eliminar un usuario del sistema por su ID.
    
    Parameters:
        user_id (int): Identificador del usuario a eliminar.
        
    Returns:
        dict: Un mensaje indicando el resultado de la operación.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la eliminación.
    """
    try:
        delete_user(user_id)
        return {"status": "success", "message": f"Usuario {user_id} eliminado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al eliminar usuario: {str(e)}")
    
@app.get("/users/next-id", response_model=dict)
def next_user_id():
    """
    Retorna el próximo id de usuario disponible para crear un nuevo usuario.
    
    Returns:
        dict: Un diccionario con el próximo ID disponible.
        Ejemplo de respuesta: { "next_id": 5 }
        
    Raises:
        HTTPException: 500 si ocurre un error al obtener el ID.
    """
    try:
        next_id = get_next_user_id()
        return {"next_id": next_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener el próximo id de usuario: {str(e)}")


# Endpoints para gestión de películas
@app.post("/movies/", response_model=dict)
def add_movie(movie: Movie):
    """
    Crear una nueva película en el sistema.
    
    Parameters:
        movie (Movie): Objeto con ID y título de la película a crear.
        
    Returns:
        dict: Un mensaje indicando el resultado de la operación.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la creación.
    """
    try:
        create_movie(movie.id, movie.title)
        return {"status": "success", "message": f"Película '{movie.title}' creada correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al crear película: {str(e)}")

@app.get("/movies/", response_model=List[dict])
def get_movies():
    """
    Obtener todas las películas registradas en el sistema.
    
    Returns:
        List[dict]: Lista de diccionarios, cada uno con la información de una película.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la consulta.
    """
    try:
        movies_df = get_all_movies()
        return movies_df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener películas: {str(e)}")

@app.put("/movies/{movie_id}", response_model=dict)
def modify_movie(movie_id: int, title: str = Body(..., embed=True)):
    """
    Actualizar el título de una película existente.
    
    Parameters:
        movie_id (int): Identificador de la película a modificar.
        title (str): Nuevo título para la película.
        
    Returns:
        dict: Un mensaje indicando el resultado de la operación.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la actualización.
    """
    try:
        update_movie(movie_id, title)
        return {"status": "success", "message": f"Película {movie_id} actualizada a '{title}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al actualizar película: {str(e)}")

@app.delete("/movies/{movie_id}", response_model=dict)
def remove_movie(movie_id: int):
    """
    Eliminar una película del sistema por su ID.
    
    Parameters:
        movie_id (int): Identificador de la película a eliminar.
        
    Returns:
        dict: Un mensaje indicando el resultado de la operación.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la eliminación.
    """
    try:
        delete_movie(movie_id)
        return {"status": "success", "message": f"Película {movie_id} eliminada correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al eliminar película: {str(e)}")

# Endpoints para gestión de géneros
@app.post("/genres/", response_model=dict)
def add_genre(genre: Genre):
    """
    Crear un nuevo género cinematográfico en el sistema.
    
    Parameters:
        genre (Genre): Objeto con el nombre del género a crear.
        
    Returns:
        dict: Un mensaje indicando el resultado de la operación.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la creación.
    """
    try:
        create_genre(genre.name)
        return {"status": "success", "message": f"Género '{genre.name}' creado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al crear género: {str(e)}")

@app.get("/genres/", response_model=List[dict])
def get_genres():
    """
    Obtener todos los géneros cinematográficos registrados en el sistema.
    
    Returns:
        List[dict]: Lista de diccionarios, cada uno con la información de un género.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la consulta.
    """
    try:
        genres_df = get_all_genres()
        return genres_df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener géneros: {str(e)}")

@app.delete("/genres/{genre_name}", response_model=dict)
def remove_genre(genre_name: str):
    """
    Eliminar un género cinematográfico del sistema por su nombre.
    
    Parameters:
        genre_name (str): Nombre del género a eliminar.
        
    Returns:
        dict: Un mensaje indicando el resultado de la operación.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la eliminación.
    """
    try:
        delete_genre(genre_name)
        return {"status": "success", "message": f"Género '{genre_name}' eliminado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al eliminar género: {str(e)}")

# Endpoints para gestión de relaciones película-género
@app.post("/movie-genres/", response_model=dict)
def add_movie_genre(movie_genre: MovieGenre):
    """
    Asociar una película con un género cinematográfico.
    
    Parameters:
        movie_genre (MovieGenre): Objeto con el ID de película y nombre de género a asociar.
        
    Returns:
        dict: Un mensaje indicando el resultado de la operación.
        
    Raises:
        HTTPException: 404 si la película o género no existen.
                      500 si ocurre algún otro error durante la asociación.
    """
    try:
        link_movie_to_genre(movie_genre.movie_id, movie_genre.genre_name)
        return {"status": "success", "message": f"Película {movie_genre.movie_id} asociada con género '{movie_genre.genre_name}'"}
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al asociar película y género: {str(e)}")

@app.get("/movie-genres/", response_model=List[dict])
def get_movie_genres():
    """
    Obtener todas las relaciones película-género registradas en el sistema.
    
    Returns:
        List[dict]: Lista de diccionarios, cada uno con una relación película-género.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la consulta.
    """
    try:
        movie_genres_df = get_all_movie_genres()
        return movie_genres_df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener relaciones película-género: {str(e)}")

@app.delete("/movie-genres/", response_model=dict)
def remove_movie_genre(movie_genre: MovieGenre):
    """
    Eliminar una relación película-género del sistema.
    
    Parameters:
        movie_genre (MovieGenre): Objeto con el ID de película y nombre de género a desvincular.
        
    Returns:
        dict: Un mensaje indicando el resultado de la operación.
        
    Raises:
        HTTPException: 404 si la relación no existe.
                      500 si ocurre algún otro error durante la eliminación.
    """
    try:
        unlink_movie_from_genre(movie_genre.movie_id, movie_genre.genre_name)
        return {"status": "success", "message": f"Relación entre película {movie_genre.movie_id} y género '{movie_genre.genre_name}' eliminada"}
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al eliminar relación película-género: {str(e)}")

# Endpoints para gestión de calificaciones
@app.post("/ratings/", response_model=dict)
def add_rating(rating: Rating):
    """
    Crear una nueva calificación de película por un usuario.
    
    Parameters:
        rating (Rating): Objeto con el ID de usuario, ID de película y valor de calificación.
        
    Returns:
        dict: Un mensaje indicando el resultado de la operación.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la creación.
    """
    try:
        create_rating(rating.user_id, rating.movie_id, rating.value)
        return {"status": "success", "message": f"Calificación creada correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al crear calificación: {str(e)}")

@app.get("/ratings/", response_model=List[dict])
def get_ratings():
    """
    Obtener todas las calificaciones registradas en el sistema.
    
    Returns:
        List[dict]: Lista de diccionarios, cada uno con información de una calificación.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la consulta.
    """
    try:
        ratings_df = get_all_ratings()
        return ratings_df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener calificaciones: {str(e)}")

@app.put("/ratings/", response_model=dict)
def modify_rating(rating: Rating):
    """
    Actualizar una calificación existente de un usuario a una película.
    
    Parameters:
        rating (Rating): Objeto con el ID de usuario, ID de película y nuevo valor de calificación.
        
    Returns:
        dict: Un mensaje indicando el resultado de la operación.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la actualización.
    """
    try:
        update_rating(rating.user_id, rating.movie_id, rating.value)
        return {"status": "success", "message": f"Calificación actualizada correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al actualizar calificación: {str(e)}")

@app.delete("/ratings/", response_model=dict)
def remove_rating(user_id: int = Body(...), movie_id: int = Body(...)):
    """
    Eliminar una calificación de un usuario a una película.
    
    Parameters:
        user_id (int): ID del usuario que realizó la calificación.
        movie_id (int): ID de la película calificada.
        
    Returns:
        dict: Un mensaje indicando el resultado de la operación.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la eliminación.
    """
    try:
        delete_rating(user_id, movie_id)
        return {"status": "success", "message": f"Calificación eliminada correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al eliminar calificación: {str(e)}")

# Endpoint para obtener todos los datos para el sistema de recomendación
@app.get("/fetch-data/", response_model=dict)
def fetch_all_data():
    """
    Obtener todos los datos necesarios para el sistema de recomendación.
    
    Este endpoint recopila y devuelve toda la información relevante para el 
    entrenamiento y funcionamiento del sistema de recomendación.
    
    Returns:
        dict: Un diccionario con el estado de la operación, los datos recopilados
              y el número total de registros.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la consulta.
    """
    try:
        data_df = fetch_data_from_db()
        return {
            "status": "success", 
            "data": data_df.to_dict(orient="records"),
            "count": len(data_df)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener datos: {str(e)}")
    
@app.get("/users/{user_id}/ratings", response_model=List[RatingResponse])
def get_user_ratings(user_id: int):
    """
    Obtiene todos los ratings de un usuario junto con la información de la película.
    
    Esta función consulta todas las calificaciones realizadas por un usuario específico
    y enriquece los datos con información detallada de cada película calificada.
    
    Parameters:
        user_id (int): ID del usuario del que se quieren obtener las calificaciones.
        
    Returns:
        List[RatingResponse]: Lista de calificaciones con información detallada.
        Si no se encuentra ninguna calificación, retorna una lista vacía.
        
    Raises:
        HTTPException: 500 si ocurre un error durante la consulta.
    """
    try:
        ratings_df = get_ratings_by_user(user_id)
        if ratings_df.empty:
            return []
        return ratings_df.to_dict(orient="records")
    except Exception as e:
        print(f"Error al obtener ratings: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener ratings: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)