import requests
import json
import datetime

def get_weather_by_zip(zip_code):
    url = f"http://api.openweathermap.org/data/2.5/weather?zip={zip_code}&appid={api_key}&units=metric"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        return {'error': 'Unable to fetch weather data'}

def get_city_weather(city_name):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching weather data for {city_name}: {response.status_code}")
        return None

def get_forecast_geoweather(lat,lon, stateIso3166_2,state,town):
    url = f"http://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()

        ciudades_alertas = {}

        if 'list' in data and data['list']:
            for intervalo in data['list']:
                if 'wind' in intervalo and intervalo['wind'] and intervalo['wind']['gust'] > 20:
                    alertToReturn = createAlert(data, intervalo, state, stateIso3166_2, town,"Meteo","Wind Gust",f"{intervalo['wind']['gust']*3.6} km/h",data['city']['coord']['lat'],data['city']['coord']['lon'])
                    addAlert(alertToReturn, town, ciudades_alertas)
                if 'rain' in intervalo and intervalo.get('rain', {}).get('3h', 0) > 15:
                    alertToReturn = createAlert(data, intervalo, state, stateIso3166_2, town,"Meteo","Rain",f"{int(intervalo['rain']['3h'] / 3)} mm/h",data['city']['coord']['lat'],data['city']['coord']['lon'])
                    addAlert(alertToReturn, town, ciudades_alertas)
                if 'snow' in intervalo and intervalo.get('snow', {}).get('3h', 0) > 29:
                    alertToReturn = createAlert(data, intervalo, state, stateIso3166_2, town,"Meteo","Snow",f"{int(intervalo['snow']['3h'] / 3)} cm",data['city']['coord']['lat'],data['city']['coord']['lon'])
                    addAlert(alertToReturn, town, ciudades_alertas)
                if intervalo['main']['temp_max'] > 37:
                    alertToReturn = createAlert(data, intervalo, state, stateIso3166_2, town,"Meteo","High Temperature",f"{intervalo['main']['temp_max']} °C",data['city']['coord']['lat'],data['city']['coord']['lon'])
                    addAlert(alertToReturn, town, ciudades_alertas)
                elif intervalo['main']['temp_min'] < 0:
                    alertToReturn = createAlert(data, intervalo, state, stateIso3166_2, town,"Meteo","Low Temperature",f"{intervalo['main']['temp_min']} °C",data['city']['coord']['lat'],data['city']['coord']['lon'])
                    addAlert(alertToReturn, town, ciudades_alertas)
                if intervalo['weather'][0]['id'] in (201, 202, 232, 231, 221, 211, 210):
                    alertToReturn = createAlert(data, intervalo, state, stateIso3166_2, town,"Meteo","Thunderstorm",intervalo['weather']['description'],data['city']['coord']['lat'],data['city']['coord']['lon'])
                    addAlert(alertToReturn, town, ciudades_alertas)
                if str(intervalo['weather'][0]['id']).startswith('7'):
                    alertToReturn = createAlert(data, intervalo, state, stateIso3166_2, town,"Meteo","Atmosphere",intervalo['weather']['description'],data['city']['coord']['lat'],data['city']['coord']['lon'])
                    addAlert(alertToReturn, town, ciudades_alertas)
            alertsToReturn = []
            for alerta in ciudades_alertas.values():
                alertsToReturn.append(alerta)
            return alertsToReturn
        else:
            print(f"Error fetching weather data for latitude : {lat} and longitude: {lon}: {response.status_code}")
            return None


def createAlert(data, intervalo, state, stateIso3166_2, town, alertType, alertSubType,description,lat,lon):
    alertToReturn = {
        'first_date': intervalo['dt_txt'],
        'last_date': intervalo['dt_txt'],
        'country': data['city']['country'],
        'stateIso3166_2': stateIso3166_2,
        'state': state,
        'town': town,
        'alertType': alertType,
        'alertSubType': alertSubType,
        'description': description,
        'lat': lat,
        'lon': lon
    }
    return alertToReturn


def addAlert(alertToReturn, town, ciudades_alertas):
    if town in ciudades_alertas:
        alerta_existente = ciudades_alertas[town]

        # Obtener las fechas mínima y máxima actuales
        fecha_minima = alerta_existente['local_start_date']
        fecha_maxima = alerta_existente['local_end_date']

        # Actualizamos la fecha mínima y máxima
        alerta_existente['local_start_date'] = min(fecha_minima, alertToReturn['first_date'])
        alerta_existente['local_end_date'] = max(fecha_maxima, alertToReturn['last_date'])

        if float(alertToReturn['description'].split()[0]) > float(
                alerta_existente['description'].split()[0]):
            alerta_existente['description'] = alertToReturn['description']
    else:
        # Si no existe alerta para la ciudad, la añadimos con las fechas iniciales
        alertToReturn['local_start_date'] = alertToReturn['first_date']
        alertToReturn['local_end_date'] = alertToReturn['last_date']
        ciudades_alertas[town] = alertToReturn


def get_actual_geoweather(lat,lon):
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    #response = requests.get(url)
    #if response.status_code == 200:
        #data = response.json()
    data = {
        'coord': {
            'lon': -8.5056,
            'lat': 43.3144
        },
        'weather': [
            {
                'id': 802,
                'main': 'Clouds',
                'description': 'scattered clouds',
                'icon': '03d'
            }
        ],
        'base': 'stations',
        'main': {
            'temp': 20.31,
            'feels_like': 20.12,
            'temp_min': 19.21,
            'temp_max': 20.79,
            'pressure': 1000,
            'humidity': 66,
            'sea_level': 1000,
            'grnd_level': 980
        },
        'visibility': 10000,
        'wind': {
            'speed': 7.72,
            'deg': 220
        },
        'clouds': {
            'all': 40
        },
        'dt': 1728312155,
        'sys': {
            'type': 2,
            'id': 2011211,
            'country': 'ES',
            'sunrise': 1728283110,
            'sunset': 1728324283
        },
        'timezone': 7200,
        'id': 3129329,
        'name': 'Arteixo',
        'cod': 200
    }
    alertsToReturn = set()  # Asegúrate de que alertsToReturn está definido antes de usarlo.
    if 'alerts' in data and data['alerts']:
        for alert in data['alerts']:
            alertToReturn = {
                'alertName': alert['event'],
                'description': alert['description'],  # Cambiado para acceder al alert actual
                'lat': data['lat'],  # Asegúrate de que 'lat' y 'lon' están en 'coord'
                'lon': data['lon'],
            }
            alertsToReturn.append(alertToReturn)
        return alertsToReturn
    #else:
        #print(f"Error fetching weather data for latitude : {lat} and longitude: {lon}: {response.status_code}")
        #return None

api_key = 'fc8c4686a855f46257697a41c80c4359'

