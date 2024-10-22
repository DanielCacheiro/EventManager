import requests
import http.client
import json

def get_holidays_by_country(country_code, year):
    url = f'https://calendarific.com/api/v2/holidays?&api_key={calendarific_api_key}&country={country_code}&year={year}'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        holidays = []
        for holiday in data['response']['holidays']:
            holidays.append({
                'name': holiday['name'],
                'date': holiday['date']['iso'],
                'description': holiday['description']
            })
        return holidays
    else:
        return {'error': 'Unable to fetch holidays'}

def get_holidays_by_state(state, stateIso,country_code, year):
    conn = http.client.HTTPSConnection("calendarific.com")

    endpoint = f"/api/v2/holidays?&api_key={calendarific_api_key}&country={country_code}&year={year}&location={stateIso}"

    conn.request("GET", endpoint)
    response = conn.getresponse()
    data = response.read()
    conn.close()

    #if data:
    if response.status == 200:
        # Parsear los datos JSON
        data = json.loads(data)
        provincias_alertas = {}
        for holiday in data['response']['holidays']:
            if any('local holiday' in t.lower() or 'national holiday' in t.lower() for t in holiday['type']):
                alertToReturn = {
                    'local_start_date': holiday['date']['iso'],
                    'local_end_date': holiday['date']['iso'],
                    'country': country_code,
                    'state': state,
                    'town': '',
                    'alertType': "Holiday",
                    'alertSubType': holiday['name'],
                    'description': holiday['description'],
                    'lat': None,  # Placeholder, update with correct data if available
                    'lon': None   # Placeholder, update with correct data if available
                }
                addAlert(alertToReturn, state, provincias_alertas)
        alertsToReturn = []
        for alerta in provincias_alertas.values():
            alertsToReturn.append(alerta)
        return alertsToReturn
    else:
        return {'error': 'Unable to fetch holidays'}


def addAlert(alertToReturn, province_code, provincias_alertas):
    if province_code not in provincias_alertas:
        provincias_alertas[province_code] = []
    provincias_alertas[province_code].append(alertToReturn)

# Ejemplo de uso
calendarific_api_key = '6yKn3XVfIfcuhNT5fQw6d8pADJ7f71aM'
country_code = 'ES'  # España
year = 2024
