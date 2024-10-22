import requests
import http.client
import json

def obtener_eventos_moda(end_date,countryiso):
    conn = http.client.HTTPSConnection("app.ticketmaster.com")
    endpoint = f"/discovery/v2/events.json?&apikey={api_key_eventbrite}&classificationName=fashion&endDateTime={end_date}&sort=relevance,desc&size=100&countryCode={countryiso}"

    conn.request("GET", endpoint)
    response = conn.getresponse()
    data = response.read()
    conn.close()

    if response.status == 200:
        data = json.loads(data)
        eventos = data.get('_embedded', {}).get('events', [])
        return eventos
    else:
        raise Exception(f"Error al obtener eventos de Eventbrite: {response.status}")

def obtener_eventos_musica(end_date, countryiso):
    conn = http.client.HTTPSConnection("app.ticketmaster.com")
    endpoint = f"/discovery/v2/events.json?&apikey={api_key_eventbrite}&classificationName=music&endDateTime={end_date}&sort=relevance,desc&size=100&countryCode={countryiso}"

    conn.request("GET", endpoint)
    response = conn.getresponse()
    data = response.read()
    conn.close()

    if response.status == 200:
        data = json.loads(data)
        eventos = data.get('_embedded', {}).get('events', [])
        return eventos
    else:
        raise Exception(f"Error al obtener eventos de Eventbrite: {response.status}")

def obtener_eventos_festivales(end_date, countryiso):
    conn = http.client.HTTPSConnection("app.ticketmaster.com")
    endpoint = f"/discovery/v2/events.json?&apikey={api_key_eventbrite}&classificationName=festival&endDateTime={end_date}&sort=relevance,desc&size=100&countryCode={countryiso}"

    conn.request("GET", endpoint)
    response = conn.getresponse()
    data = response.read()
    conn.close()

    if response.status == 200:
        data = json.loads(data)
        eventos = data.get('_embedded', {}).get('events', [])
        return eventos
    else:
        raise Exception(f"Error al obtener eventos de Eventbrite: {response.status}")

def obtener_eventos_hobbies(end_date, countryiso):
    conn = http.client.HTTPSConnection("app.ticketmaster.com")
    endpoint = f"/discovery/v2/events.json?&apikey={api_key_eventbrite}&classificationName=Hobby/SpecialInterestExpos&endDateTime={end_date}&sort=relevance,desc&size=100&countryCode={countryiso}"

    conn.request("GET", endpoint)
    response = conn.getresponse()
    data = response.read()
    conn.close()

    if response.status == 200:
        data = json.loads(data)
        eventos = data.get('_embedded', {}).get('events', [])
        return eventos
    else:
        raise Exception(f"Error al obtener eventos de Eventbrite: {response.status}")

def obtener_eventos_cultural(end_date,countryiso):
    conn = http.client.HTTPSConnection("app.ticketmaster.com")
    endpoint = f"/discovery/v2/events.json?&apikey={api_key_eventbrite}&segmentName=Arts&Theatre&endDateTime={end_date}&sort=relevance,desc&size=100&countryCode={countryiso}"

    conn.request("GET", endpoint)
    response = conn.getresponse()
    data = response.read()
    conn.close()

    if response.status == 200:
        data = json.loads(data)
        eventos = data.get('_embedded', {}).get('events', [])
        return eventos
    else:
        raise Exception(f"Error al obtener eventos de Eventbrite: {response.status}")

def obtener_eventos_deportes(end_date, countryiso):
    conn = http.client.HTTPSConnection("app.ticketmaster.com")
    endpoint = f"/discovery/v2/events.json?&apikey={api_key_eventbrite}&segmentName=Sports&sort=relevance,desc&size=100&countryCode={countryiso}"

    conn.request("GET", endpoint)
    response = conn.getresponse()
    data = response.read()
    conn.close()

    if response.status == 200:
        data = json.loads(data)
        eventos = data.get('_embedded', {}).get('events', [])
        return eventos
    else:
        raise Exception(f"Error al obtener eventos de Eventbrite: {response.status}")

api_key_eventbrite = 'KGd3tkLAZaZ6ALEletQLHgQKzUIJkD07'