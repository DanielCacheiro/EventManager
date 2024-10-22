import requests
import os
import json


def obtener_tweets_modas(bearer_token, query):
    url = "https://api.twitter.com/2/tweets/search/recent"
    headers = {
        "Authorization": f"Bearer {bearer_token}"
    }
    params = {
        'query': query,  # Palabras clave para buscar tweets relacionados con moda
        'max_results': 10  # Número máximo de tweets a recuperar
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json().get('data', [])
    else:
        raise Exception(f"Error al obtener tweets: {response.status_code}")

bearer_token_twitter = 'tu_bearer_token_twitter'
query = 'fashion event'