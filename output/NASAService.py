import requests
from requests.exceptions import ConnectionError, Timeout, RequestException
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime, timedelta
import sys
sys.path.append('./code/output')
import geonamesService


def obtener_ciudades_afectadas(ciudades_afectadas,zone):
    cityAdded = False
    if zone and zone[0] and zone[0].get('adminName1', zone[0].get('name')) and zone[0].get('adminName1', zone[0].get('name')) not in ciudades_afectadas:
        ciudades_afectadas.add(zone[0].get('adminName1', zone[0].get('name')))
        cityAdded = True
        print(f"Añadida ciudad afectada: {zone[0].get('adminName1', zone[0].get('name'))}")
    elif not zone:
        print(f"Zona no encontrada")
    else:
        print(f"Ciudad {zone[0].get('adminName1', zone[0].get('name'))} ya añadida o no encontrada")

    return cityAdded

def get_natural_events():
    start_date = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = (datetime.today() + timedelta(days=7)).strftime('%Y-%m-%d')
    url = f'https://eonet.gsfc.nasa.gov/api/v3/events?status=open&start={start_date}&end={end_date}'
    timeout = 10
    try:
        response = requests.get(url, timeout)

        if response.status_code == 200:
            data = response.json()
            filtered_events = []
            ciudades_afectadas = set()
            for event in data['events']:
                zone = geonamesService.reverse_geocode(event['geometry'][0]['coordinates'][1], event['geometry'][0]['coordinates'][0])
                if not zone:
                    for geometry in event['geometry']:
                        zone = geonamesService.reverse_geocode(geometry['coordinates'][1],
                                                           geometry['coordinates'][0])
                        if zone:
                            break;

                cityAdded = obtener_ciudades_afectadas(ciudades_afectadas,zone)

                if cityAdded and zone and zone[0]:
                    filtered_events.append({
                    'local_start_date': event['geometry'][0]['date'],
                    'local_end_date': '',
                    'country': zone[0].get('countryCode',''),
                    'state': zone[0].get('adminName1',''),
                    'town': zone[0].get('name',''),
                    'alertType': "Natural disaster",
                    'alertSubType': event['categories'][0]['id'],
                    'description': event['categories'][0]['title']
                    })
                elif cityAdded:
                    print(f"No se como ubicar zone {zone}")
            return filtered_events
        else:
            return {'error': 'Unable to fetch natural events'}
    except ConnectionError as ce:
        return {'error': f'Connection error. Please check your network or the service. {str(ce)}'}
    except Timeout:
        return {'error': 'The request timed out.'}
    except RequestException as e:
        return {'error': f'An request error occurred: {str(e)}'}
    except Exception as e:
        print(f"error': f'An error occurred: {str(e)}")


def haversine(coord1, old_lat,old_lon):
    # Coordenadas en radianes
    lat1, lon1 = radians(coord1[1]), radians(coord1[0])
    lat2, lon2 = radians(old_lat), radians(old_lon)

    # Diferencias entre coordenadas
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Fórmula de Haversine
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # Radio de la Tierra en km
    R = 6371.0
    return R * c

# Ejemplo de uso
nasa_api_key = 'NASA'
