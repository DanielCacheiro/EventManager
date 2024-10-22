import requests
from math import sqrt

class GeonamesServiceException(Exception):
    """Base class for exceptions in this module."""
    pass


class CountryNotFoundException(GeonamesServiceException):
    """Exception raised when a country is not found."""

    def __init__(self, country):
        self.country = country
        self.message = f"Country '{country}' not found."
        super().__init__(self.message)


class StateNotFoundException(GeonamesServiceException):
    """Exception raised when a state is not found."""

    def __init__(self, state):
        self.state = state
        self.message = f"State '{state}' not found."
        super().__init__(self.message)


class ProvinceNotFoundException(GeonamesServiceException):
    """Exception raised when a province is not found."""

    def __init__(self, province):
        self.province = province
        self.message = f"Province '{province}' not found."
        super().__init__(self.message)


class PostalCodeNotFoundException(GeonamesServiceException):
    """Exception raised when postal codes are not found."""

    def __init__(self, province):
        self.province = province
        self.message = f"Postal codes for province '{province}' not found."
        super().__init__(self.message)


class CoordinatesNotFoundException(GeonamesServiceException):
    """Exception raised when coordinates are not found."""

    def __init__(self, location):
        self.location = location
        self.message = f"Coordinates for location '{location}' not found."
        super().__init__(self.message)


class GeonameIdNotFoundException(GeonamesServiceException):
    """Exception raised when a geoname ID is not found."""

    def __init__(self, location):
        self.location = location
        self.message = f"Geoname ID for location '{location}' not found."
        super().__init__(self.message)


username = 'cache'  # Reemplaza con tu nombre de usuario


def get_countries():
    url = f'http://api.geonames.org/countryInfoJSON?username={username}'
    response = requests.get(url)
    if response.status_code != 200:
        raise GeonamesServiceException(f"Error fetching countries: {response.status_code}")

    data = response.json()
    if 'geonames' not in data:
        raise CountryNotFoundException('All countries')

    countries = {item['countryCode']: item['countryName'] for item in data['geonames']}
    return countries


def get_states(country,country_code):
    url = f'http://api.geonames.org/childrenJSON?geonameId={country}&username={username}&country={country_code}&featureCode=ADM3'
    response = requests.get(url)
    if response.status_code != 200:
        raise GeonamesServiceException(f"Error fetching states: {response.status_code}")

    data = response.json()
    states = response.json().get('geonames', [])
    if not states:
        print(StateNotFoundException(country))

    return states


def get_provinces(state_geoname,country_code):
    url = f"http://api.geonames.org/childrenJSON?geonameId={state_geoname}&username={username}&country={country_code}&featureCode=ADM1"
    response = requests.get(url)
    if response.status_code != 200:
        raise GeonamesServiceException(f"Error fetching provinces: {response.status_code}")

    provincias = response.json().get('geonames', [])
    if not provincias:
        print(ProvinceNotFoundException(state_geoname))

    return provincias

def get_district(state_geoname):
    url = f"http://api.geonames.org/childrenJSON?geonameId={state_geoname}&username={username}&featureCode=ADM4"
    response = requests.get(url)
    if response.status_code != 200:
        raise GeonamesServiceException(f"Error fetching provinces: {response.status_code}")

    provincias = response.json().get('geonames', [])
    if not provincias:
        print(ProvinceNotFoundException(state_geoname))

    return provincias

def obtener_codigos_postales(nombre_provincia, countryISO):
    url = f"http://api.geonames.org/postalCodeSearchJSON?placename={nombre_provincia}&country={countryISO}&maxRows=500&username={username}"
    response = requests.get(url)
    if response.status_code != 200:
        raise GeonamesServiceException(f"Error fetching postal codes: {response.status_code}")

    codigos_postales = response.json().get('postalCodes', [])
    if not codigos_postales:
        print(PostalCodeNotFoundException(nombre_provincia))

    coords_to_return = [[codigo_postal['lat'], codigo_postal['lng']] for codigo_postal in codigos_postales]
    return filtrar_coordenadas(coords_to_return)


def get_coords(country):
    url = f'http://api.geonames.org/searchJSON?q={country}&username={username}'
    response = requests.get(url)
    if response.status_code != 200:
        raise GeonamesServiceException(f"Error fetching coordinates: {response.status_code}")

    data = response.json()
    if 'geonames' not in data or not data['geonames']:
        print(CoordinatesNotFoundException(country))

    coords = [data["geonames"][0]["lat"], data["geonames"][0]["lng"]]
    return coords


def get_geonames(country_name):
    url = f"http://api.geonames.org/searchJSON?q={country_name}&username={username}&featureClass=A&featureCode=PCLI"
    response = requests.get(url)
    if response.status_code != 200:
        raise GeonamesServiceException(f"Error fetching geoname ID: {response.status_code}")

    data = response.json()
    if 'geonames' not in data or not data['geonames']:
        print(GeonameIdNotFoundException(country_name))

    geoname = {item['geonameId']: item['countryName'] for item in data['geonames']}
    return geoname

def get_geonames_by_province(province_name):
    url = f"http://api.geonames.org/searchJSON?q={province_name}&username={username}&featureCode=ADM2"
    response = requests.get(url)
    if response.status_code != 200:
        raise GeonamesServiceException(f"Error fetching geoname ID: {response.status_code}")

    data = response.json()
    if 'geonames' not in data or not data['geonames']:
        print(GeonameIdNotFoundException(province_name))

    return data['geonames']

def get_hierarchy(state_geonameId):
    url = f"http://api.geonames.org/hierarchyJSON?geonameId={state_geonameId}&username={username}"
    response = requests.get(url)
    if response.status_code != 200:
        raise GeonamesServiceException(f"Error fetching geoname ID: {response.status_code}")

    data = response.json()
    if 'geonames' not in data or not data['geonames']:
        print(GeonameIdNotFoundException(state_geonameId))

    return data['geonames']



def get_coords_by_city(city):
    url = f'http://api.geonames.org/searchJSON?q={city}&maxRows=1&username={username}'
    response = requests.get(url)
    if response.status_code != 200:
        raise GeonamesServiceException(f"Error fetching coordinates by city: {response.status_code}")

    data = response.json()
    if 'geonames' not in data or not data['geonames']:
        print(CoordinatesNotFoundException(city))

    coords = [data["geonames"][0]["lat"], data["geonames"][0]["lng"]]
    return coords


def calcular_distancia(lat1, lng1, lat2, lng2):
    return sqrt((lat1 - lat2) ** 2 + (lng1 - lng2) ** 2)


def reverse_geocode(latitude, longitude):
    url = f'http://api.geonames.org/findNearbyJSON?lat={latitude}&lng={longitude}&username={username}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data.get('geonames', [])
    else:
        return []

def filtrar_coordenadas(allCoords, umbral=0.4):
    coordenadas_filtradas = []
    for coords in allCoords:
        duplicado = False
        for coord in coordenadas_filtradas:
            lat_filtrado = float(coord[0])
            lng_filtrado = float(coord[1])
            distancia = calcular_distancia(coords[0], coords[1], lat_filtrado, lng_filtrado)
            if distancia < umbral:
                duplicado = True
                break
        if not duplicado:
            coordenadas_filtradas.append((coords[0], coords[1]))
    return coordenadas_filtradas