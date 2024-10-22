import requests

def obtener_peliculas_taquilleras():
    url = f"https://api.themoviedb.org/3/movie/upcoming?api_key={api_key}&language=es-ES&page=1"
    response = requests.get(url, verify=False)  # Agrega verify=False

    if response.status_code == 200:
        data = response.json()
        peliculas = data['results']
        highlyPopularMovies = []
        if peliculas:
            for pelicula in peliculas:
                if pelicula['popularity'] > 800:
                    highlyPopularMovies.append(pelicula)
        return highlyPopularMovies
    else:
        raise Exception(f"Error en la solicitud: {response.status_code}")

def obtener_series_populares():
    url = f"https://api.themoviedb.org/3/tv/popular?api_key={api_key}&language=es-ES&page=1"
    response = requests.get(url, verify=False)  # Agrega verify=False

    if response.status_code == 200:
        data = response.json()
        series = data['results']
        highlyPopularSeries = []
        if series:
            for serie in series:
                if serie['popularity'] > 800:
                    highlyPopularSeries.append(serie)
        return highlyPopularSeries
    else:
        raise Exception(f"Error en la solicitud: {response.status_code}")

api_key='41ec89427fd8a7ed5d11dc44ba3e5f6d'