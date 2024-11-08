from kafka.producer import kafka

import influxVisualization
from output import openWeatherService,geonamesService,calendarificService, NASAService, ticketmasterService, TMDbService, betfairService, alphaVantageService, gdeltService
import json
import time
from datetime import timedelta, datetime
from geopy.distance import geodesic
import random


# Límite de llamadas y contadores
API_LIMITS = {
    'openweather': {'limit': 30, 'interval': 30, 'counter': 0, 'reset': datetime.now() + timedelta(minutes=1)},
    'calendarific': {'limit': 1000, 'interval': 2592000, 'counter': 0, 'reset': datetime.now() + timedelta(days=30)},
    'nasa': {'limit': 1000, 'interval': 3600, 'counter': 0, 'reset': datetime.now() + timedelta(hours=1)},
    'ticketmaster': {'limit': 1000, 'interval': 86400, 'counter': 0, 'reset': datetime.now() + timedelta(days=1)},
    'tmdb': {'limit': 20, 'interval': 5, 'counter': 0, 'reset': datetime.now() + timedelta(seconds=10)},
    'gdelt': {'limit': 3600, 'interval': 3600, 'counter': 0, 'reset': datetime.now() + timedelta(hours=1)},
    'alphavantage': {'limit': 5, 'interval': 60, 'counter': 0, 'reset': datetime.now() + timedelta(minutes=1)},
    'geonames': {'limit': 2000, 'interval': 3600, 'counter': 0, 'reset': datetime.now() + timedelta(hours=1)}
}

countries = {'AD': 'Andorra', 'AE': 'United Arab Emirates', 'AF': 'Afghanistan', 'AG': 'Antigua and Barbuda',
             'AI': 'Anguilla', 'AL': 'Albania', 'AM': 'Armenia', 'AO': 'Angola', 'AR': 'Argentina',
             'AS': 'American Samoa', 'AT': 'Austria', 'AU': 'Australia', 'AW': 'Aruba', 'AZ': 'Azerbaijan',
             'BA': 'Bosnia and Herzegovina', 'BB': 'Barbados', 'BD': 'Bangladesh', 'BE': 'Belgium',
             'BF': 'Burkina Faso', 'BG': 'Bulgaria', 'BH': 'Bahrain', 'BJ': 'Benin', 'BM': 'Bermuda', 'BN': 'Brunei',
             'BO': 'Bolivia', 'BR': 'Brazil', 'BS': 'Bahamas', 'BT': 'Bhutan', 'BV': 'Bouvet Island', 'BW': 'Botswana',
             'BY': 'Belarus', 'BZ': 'Belize', 'CA': 'Canada', 'CD': 'DR Congo', 'CF': 'Central African Republic',
             'CG': 'Congo Republic', 'CH': 'Switzerland', 'CI': 'Ivory Coast', 'CK': 'Cook Islands', 'CL': 'Chile',
             'CM': 'Cameroon', 'CN': 'China', 'CO': 'Colombia', 'CR': 'Costa Rica', 'CU': 'Cuba', 'CV': 'Cabo Verde',
             'CX': 'Christmas Island', 'CY': 'Cyprus', 'CZ': 'Czechia', 'DE': 'Germany', 'DJ': 'Djibouti',
             'DK': 'Denmark', 'DM': 'Dominica', 'DO': 'Dominican Republic', 'DZ': 'Algeria', 'EC': 'Ecuador',
             'EE': 'Estonia', 'EG': 'Egypt', 'EH': 'Western Sahara', 'ER': 'Eritrea', 'ES': 'Spain', 'ET': 'Ethiopia',
             'FI': 'Finland', 'FJ': 'Fiji', 'FK': 'Falkland Islands', 'FM': 'Micronesia', 'FO': 'Faroe Islands',
             'FR': 'France', 'GA': 'Gabon', 'GB': 'United Kingdom', 'GD': 'Grenada', 'GE': 'Georgia',
             'GF': 'French Guiana', 'GH': 'Ghana', 'GI': 'Gibraltar', 'GL': 'Greenland', 'GM': 'The Gambia',
             'GN': 'Guinea', 'GP': 'Guadeloupe', 'GQ': 'Equatorial Guinea', 'GR': 'Greece',
             'GS': 'South Georgia and South Sandwich Islands', 'GT': 'Guatemala', 'GW': 'Guinea-Bissau', 'GY': 'Guyana',
             'HK': 'Hong Kong', 'HN': 'Honduras', 'HR': 'Croatia', 'HT': 'Haiti', 'HU': 'Hungary', 'ID': 'Indonesia',
             'IE': 'Ireland', 'IL': 'Israel', 'IM': 'Isle of Man', 'IN': 'India',
             'IO': 'British Indian Ocean Territory', 'IQ': 'Iraq', 'IR': 'Iran', 'IS': 'Iceland', 'IT': 'Italy',
             'JE': 'Jersey', 'JM': 'Jamaica', 'JO': 'Jordan', 'JP': 'Japan', 'KE': 'Kenya', 'KG': 'Kyrgyzstan',
             'KH': 'Cambodia', 'KN': 'St Kitts and Nevis', 'KP': 'North Korea', 'KR': 'South Korea', 'KW': 'Kuwait',
             'KY': 'Cayman Islands', 'KZ': 'Kazakhstan', 'LA': 'Laos', 'LB': 'Lebanon', 'LI': 'Liechtenstein',
             'LK': 'Sri Lanka', 'LR': 'Liberia', 'LS': 'Lesotho', 'LT': 'Lithuania', 'LU': 'Luxembourg', 'LV': 'Latvia',
             'LY': 'Libya', 'MA': 'Morocco', 'MC': 'Monaco', 'MD': 'Moldova', 'ME': 'Montenegro', 'MF': 'Saint Martin',
             'MG': 'Madagascar', 'MH': 'Marshall Islands', 'MK': 'North Macedonia', 'ML': 'Mali', 'MM': 'Myanmar',
             'MN': 'Mongolia', 'MO': 'Macao', 'MP': 'Northern Mariana Islands', 'MQ': 'Martinique', 'MR': 'Mauritania',
             'MS': 'Montserrat', 'MT': 'Malta', 'MU': 'Mauritius', 'MV': 'Maldives', 'MW': 'Malawi', 'MX': 'Mexico',
             'MY': 'Malaysia', 'MZ': 'Mozambique', 'NA': 'Namibia', 'NC': 'New Caledonia', 'NE': 'Niger',
             'NF': 'Norfolk Island', 'NG': 'Nigeria', 'NI': 'Nicaragua', 'NL': 'The Netherlands', 'NO': 'Norway',
             'NP': 'Nepal', 'NZ': 'New Zealand', 'OM': 'Oman', 'PA': 'Panama', 'PE': 'Peru', 'PF': 'French Polynesia',
             'PG': 'Papua New Guinea', 'PH': 'Philippines', 'PK': 'Pakistan', 'PL': 'Poland',
             'PM': 'Saint Pierre and Miquelon', 'PR': 'Puerto Rico', 'PS': 'Palestine', 'PT': 'Portugal',
             'PY': 'Paraguay', 'QA': 'Qatar', 'RE': 'Réunion', 'RO': 'Romania', 'RS': 'Serbia', 'RU': 'Russia',
             'RW': 'Rwanda', 'SA': 'Saudi Arabia', 'SB': 'Solomon Islands', 'SC': 'Seychelles', 'SD': 'Sudan',
             'SE': 'Sweden', 'SG': 'Singapore', 'SI': 'Slovenia', 'SK': 'Slovakia', 'SL': 'Sierra Leone',
             'SM': 'San Marino', 'SN': 'Senegal', 'SO': 'Somalia', 'SR': 'Suriname', 'SS': 'South Sudan',
             'ST': 'São Tomé and Príncipe', 'SV': 'El Salvador', 'SX': 'Sint Maarten', 'SY': 'Syria', 'SZ': 'Eswatini',
             'TC': 'Turks and Caicos Islands', 'TG': 'Togo', 'TH': 'Thailand', 'TJ': 'Tajikistan', 'TL': 'Timor-Leste',
             'TM': 'Turkmenistan', 'TN': 'Tunisia', 'TR': 'Türkiye', 'TT': 'Trinidad and Tobago', 'TW': 'Taiwan',
             'TZ': 'Tanzania', 'UA': 'Ukraine', 'UG': 'Uganda', 'UM': 'U.S. Outlying Islands', 'US': 'United States',
             'UY': 'Uruguay', 'UZ': 'Uzbekistan', 'VC': 'St Vincent and Grenadines', 'VE': 'Venezuela',
             'VG': 'British Virgin Islands', 'VI': 'U.S. Virgin Islands', 'VN': 'Vietnam', 'VU': 'Vanuatu',
             'WS': 'Samoa', 'XK': 'Kosovo', 'YE': 'Yemen', 'YT': 'Mayotte', 'ZA': 'South Africa', 'ZM': 'Zambia',
             'ZW': 'Zimbabwe'}

LAST_CALLS = {
    'media': None,
    'betfair': None,
    'alphavantage': None,
    'gdelt': None,
    'ticketmaster': None
}

def reset_counters():
    """Restablece los contadores de las APIs según sus intervalos de tiempo."""
    current_time = datetime.now()
    for api, limit_data in API_LIMITS.items():
        if current_time >= limit_data['reset']:
            API_LIMITS[api]['counter'] = 0
            API_LIMITS[api]['reset'] = current_time + timedelta(seconds=limit_data['interval'])


def can_call_api(api_name):
    """Verifica si una API se puede llamar respetando sus límites."""
    limit_data = API_LIMITS[api_name]
    if limit_data['counter'] < limit_data['limit']:
        API_LIMITS[api_name]['counter'] += 1
        return True
    else:
        print(f"Hemos llegado al limite de llamadas al api para {api_name}")
        return False

def should_call_daily(api_name):
    """Determina si la API debe llamarse según el registro diario."""
    last_call = LAST_CALLS.get(api_name)
    if last_call is None or (datetime.now() - last_call).days >= 1:
        LAST_CALLS[api_name] = datetime.now()
        return True
    return False

pruebas = {'ES':'Spain','IN': 'India','PH': 'Philippines','PL': 'Poland','TH': 'Thailand','KZ': 'Kazakhstan','UZ': 'Uzbekistan','US': 'United States','IT': 'Italy','JP': 'Japan','MY': 'Malaysia','KR': 'Korea','PT':'Portugal','TR':'Turkey','GB': 'United Kingdom','CN': 'China','MX':'Mexico','FR':'France','CA': 'Canada'}

def main_loop():
    while True:
        reset_counters()

        try:

            keys = list(pruebas.keys())
            #random.shuffle(keys)

            try:
                for codeIso in keys:
                    getAccurateMeteo(codeIso, pruebas[codeIso])
            except Exception as e:
                print(f"Error al obtener alertas de openweather: {e}")

            try:
                for codeIso in keys:
                    getHolidays(codeIso, pruebas[codeIso])
            except Exception as e:
                print(f"Error al obtener alertas de calendarific: {e}")
            try:
                if can_call_api('nasa'):
                    disaster_data = NASAService.get_natural_events()
                    if disaster_data:
                        print(json.dumps(disaster_data, indent=4))
                        influxVisualization.processWeatherDataToSupabase(disaster_data)
            except Exception as e:
                print(f"Error al obtener datos de NASA: {e}")

            if should_call_daily('alphavantage') and can_call_api('alphavantage'):
                try:
                    news_data = alphaVantageService.fetch_financial_news()
                    if news_data:
                        for article in news_data:
                            if article['topics']:
                                for topic in article['topics']:
                                    if float(topic['relevance_score']) > 0.85 and article['time_published']:
                                        alert = {
                                            "time_published": datetime.strptime(article['time_published'], '%Y%m%dT%H%M%S').strftime('%Y-%m-%d'),
                                            "alertType": "Financial News",
                                            "alertSubType": topic['topic'],
                                            "description": article['title'],
                                            "url": article['url']
                                        }
                                        print(alert)
                                        influxVisualization.processNewsToSupabase(alert)
                except Exception as e:
                    print(f"Error al obtener datos de AlphaVantage: {e}")

            if should_call_daily('media'):
                try:
                    if can_call_api('tmdb'):
                        peliculas = TMDbService.obtener_peliculas_taquilleras()
                        for pelicula in peliculas:
                            movie_alert = {
                                "release_date": pelicula['release_date'],
                                "inserted_date": datetime.today().strftime('%Y-%m-%d'),
                                "country": '',
                                "state": '',
                                "town": '',
                                "alertType": 'Movie',
                                "alertSubType": pelicula['original_title'],
                                "popularity": round(pelicula['popularity'])
                            }
                            influxVisualization.processMediaDataToSupabase(movie_alert)

                    if can_call_api('tmdb'):
                        series = TMDbService.obtener_series_populares()
                        for serie in series:
                            serie_alert = {
                                "release_date": serie['first_air_date'],
                                "inserted_date": datetime.today().strftime('%Y-%m-%d'),
                                "country": serie['origin_country'][0],
                                "state": '',
                                "town": '',
                                "alertType": 'Serie',
                                "alertSubType": serie['name'],
                                "popularity": round(serie['popularity'])
                            }
                            influxVisualization.processMediaDataToSupabase(serie_alert)
                except Exception as e:
                    print(f"Error al obtener datos de TMDB: {e}")

            if should_call_daily('betfair'):
                try:
                    betting_events = betfairService.obtener_apuestas_especiales()
                    for prediction in betting_events:
                        influxVisualization.insertPredictionsDataToSupabase(prediction)
                except Exception as e:
                    print(f"Error al obtener datos de Betfair: {e}")

            if should_call_daily('gdelt') and can_call_api('gdelt'):
                try:
                    gdelt_news = gdeltService.fetch_gdelt_news()
                    for everyAlert in gdelt_news:
                        print(everyAlert)
                        influxVisualization.processNewsToSupabase(everyAlert)
                except Exception as e:
                    print(f"Error al obtener datos de GDELT: {e}")

            if should_call_daily('ticketmaster'):
                for codeIso, countryName in pruebas.items():
                    for api_call, event_type, process_func in [
                        ('ticketmaster', 'Festival', influxVisualization.processFestivalDataToSupabase),
                        ('ticketmaster', 'Cultural', influxVisualization.processCulturalDataToSupabase),
                        ('ticketmaster', 'Soccer', influxVisualization.processSportsDataToSupabase),
                        ('ticketmaster', 'Sports', influxVisualization.processSportsDataToSupabase),
                        ('ticketmaster', 'Music', influxVisualization.processMusicDataToSupabase),
                        ('ticketmaster', 'Fashion', influxVisualization.processFashionDataToSupabase),
                    ]:
                        try:
                            if can_call_api(api_call):
                                eventos = ticketmasterService.obtener_eventos(event_type, codeIso)
                                for evento in eventos:
                                    alert = {
                                        "local_start_date": evento['dates']['start']['localDate'],
                                        "local_end_date": evento['dates']['start'].get('localDate'),
                                        "country": codeIso,
                                        "state": evento['_embedded']['venues'][0].get('state', {}).get('name'),
                                        "town": evento['_embedded']['venues'][0]['city']['name'],
                                        "alertType": event_type,
                                        "alertSubType": evento['classifications'][0].get('genre', {}).get('name'),
                                        "description": evento['name'].replace('"', '\\"'),
                                        "url": evento['url']
                                    }
                                    process_func(alert)
                        except Exception as e:
                            print(f"Error al obtener datos de {event_type}: {e}")

            # Espera un segundo entre cada ciclo para no saturar
            time.sleep(0.2)

        except Exception as e:
            print(f"Error occurred: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(1)





def main():
    main_loop()


def getAccurateMeteo(codeIso, countryName):
    print(f"Consultando geonames para: {countryName}")

    # Obtener geonames del país
    if can_call_api('geonames'):
        geonames = geonamesService.get_geonames(countryName)

        # Filtrar y procesar los resultados de geonames
        for geonameid, geoname_name in geonames.items():
            if countries.get(codeIso) == geoname_name:
                # Caso especial para Irlanda (IE): Filtrar subdivisiones que pertenezcan al Reino Unido
                if codeIso == 'IE' and can_call_api('geonames'):
                    geonameid = geonamesService.get_geonames('Ireland')

                # Obtener las primeras divisiones (comunidades o estados)
                firstDivision = geonamesService.get_states(geonameid, codeIso)
                random.shuffle(firstDivision)

                for first in firstDivision:
                    if can_call_api('geonames'):
                        state_names = geonamesService.get_provinces(first['geonameId'], codeIso)
                        alreadyProcessedLocation = set()

                        random.shuffle(state_names)
                        for state_name in state_names:
                            thirdDivision = geonamesService.get_states(state_name['geonameId'], codeIso)
                            stateIso3166_2 = state_name['adminCodes1'].get('ISO3166_2')
                            cached_locations = []
                            postalCodes = []
                            toponymName = state_name['toponymName']
                            if not thirdDivision:
                                state = state_name['adminName1']

                                if state_name['toponymName'] not in alreadyProcessedLocation:
                                    postalCodes = geonamesService.obtener_codigos_postales(state_name['name'], codeIso)
                                    alreadyProcessedLocation.add(state_name['name'])
                                    if not postalCodes and state_name['toponymName'] != state_name['name']:
                                        postalCodes = geonamesService.obtener_codigos_postales(state_name['toponymName'], codeIso)
                                        alreadyProcessedLocation.add(state_name['toponymName'])
                                    if not postalCodes and (state_name['toponymName'] != state_name['adminName1'] and state_name['adminName1'] not in alreadyProcessedLocation):
                                        postalCodes = geonamesService.obtener_codigos_postales(state_name['adminName1'], codeIso)
                                        alreadyProcessedLocation.add(state_name['adminName1'])
                                    if not postalCodes:
                                        postalCodes = [[first['lat'], first['lng']]]
                                if postalCodes:
                                    random.shuffle(postalCodes)
                                    for postalCode in postalCodes:
                                        coords = (postalCode[0], postalCode[1])
                                        if coords not in alreadyProcessedLocation:
                                            alreadyProcessedLocation.add(coords)
                                            if not is_nearby(coords, cached_locations):
                                                time.sleep(2.9)
                                                weather_data = openWeatherService.get_forecast_geoweather(
                                                    postalCode[0],
                                                    postalCode[1],
                                                    stateIso3166_2,
                                                    state,
                                                    toponymName
                                                )
                                                if weather_data:
                                                    print(json.dumps(weather_data, indent=4))
                                                    cached_locations.append(coords)
                                                    alreadyProcessedLocation.add(toponymName)
                                                    influxVisualization.processWeatherDataToSupabase(weather_data)
                            else:
                                filtered_thirdDivision = [item for item in thirdDivision if
                                                          item.get('population', 0) > 1000]
                                thirdDivision_reduced = random.sample(thirdDivision, min(len(filtered_thirdDivision), 20))
                                for province in thirdDivision_reduced:
                                    toponymName = province['toponymName']
                                    if province['toponymName'] not in alreadyProcessedLocation:
                                        postalCodes = [[province['lat'], first['lng']]]
                                        alreadyProcessedLocation.add(province['toponymName'])
                                    if postalCodes:
                                        for postalCode in postalCodes:
                                            coords = (postalCode[0], postalCode[1])
                                            if coords not in alreadyProcessedLocation:
                                                alreadyProcessedLocation.add(coords)
                                                if not is_nearby(coords, cached_locations):
                                                    time.sleep(2.9)
                                                    weather_data = openWeatherService.get_forecast_geoweather(
                                                                postalCode[0],
                                                                postalCode[1],
                                                                stateIso3166_2,
                                                                state_name['adminName1'],
                                                                toponymName
                                                            )
                                                    if weather_data:
                                                        print(json.dumps(weather_data, indent=4))
                                                        cached_locations.append(coords)
                                                        alreadyProcessedLocation.add(toponymName)
                                                        influxVisualization.processWeatherDataToSupabase(weather_data)


def getHolidays(codeIso,countryName):

        # Obtener geonames de ese país
        if can_call_api('geonames'):
            geonames = geonamesService.get_geonames(countryName)

            # Filtrar y procesar los resultados de geonames
            for geonameid, geoname_name in geonames.items():
                if countries.get(codeIso) == geoname_name:
                    # Caso especial para Irlanda (IE): Filtrar subdivisiones que pertenezcan al Reino Unido
                    if codeIso == 'IE' and can_call_api('geonames'):
                        geonameid = geonamesService.get_geonames('Ireland')
                    if can_call_api('geonames'):
                        firstDivision = geonamesService.get_states(geonameid,codeIso)
                        random.shuffle(firstDivision)
                        for first in firstDivision:
                            if not influxVisualization.check_holidays_added(first['adminName1'],codeIso):
                                if can_call_api('calendarific'):
                                    holiday_data = calendarificService.get_holidays_by_state(first['adminName1'],
                                                                                             str(first[
                                                                                                     'countryCode']).lower() + "-" + str(
                                                                                                 first['adminCodes1'].get(
                                                                                                     'ISO3166_2')).lower(),
                                                                                             codeIso,
                                                                                             2025)
                                    if holiday_data :
                                        print(json.dumps(holiday_data, indent=4))
                                        influxVisualization.processHolidayDataToSupabase(holiday_data)


def is_nearby(new_coords, cached_coords, threshold_km=10):
    for coords in cached_coords:
        if geodesic(new_coords, coords).km < threshold_km:
            return True
    return False

if __name__ == "__main__":
    main()
