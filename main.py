from kafka.producer import kafka

from output import openWeatherService,geonamesService,calendarificService, NASAService, ticketmasterService, TMDbService, betfairService
import json
from kafka import KafkaProducer
from pyflink.datastream import StreamExecutionEnvironment
import grafanaVisualization
import opsgenie
import time
from datetime import timedelta,datetime

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


def main():

    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',  # Cambia a tu servidor de Kafka
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print("No se puede conectar al servidor de Kafka. Esperando 2 segundos...", e)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Establecer paralelismo si es necesario

    betfairService.obtener_apuestas_especiales()

    pruebas = {'ES': 'Spain', 'CN': 'China'}

    #for codeIso, countryName in countries.items():
    for codeIso, countryName in pruebas.items():

        eventos_cultural = ticketmasterService.obtener_eventos_cultural((datetime.today() + timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ"),codeIso)
        for evento in eventos_cultural:
                cultural_alert = {
                    "local_start_date": evento['dates']['start']['localDate'],
                    "local_end_date": evento['dates']['start'].get('localDate'),  # Puedes ajustarlo si hay un campo de fin
                    "country": codeIso,
                    "state": evento['_embedded']['venues'][0].get('state', {}).get('name'),
                    "town": evento['_embedded']['venues'][0]['city']['name'],
                    "alertType": "Cultural",
                    "alertSubType": "",
                    "description": evento['name'],
                    "url": evento['url']
                }
                print(cultural_alert)
                grafanaVisualization.processCulturalDataToInfluxDB(cultural_alert)

        eventos_deportes = ticketmasterService.obtener_eventos_deportes((datetime.today() + timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ"),codeIso)
        for evento in eventos_deportes:
                sports_alert = {
                    "local_start_date": evento['dates']['start']['localDate'],
                    "local_end_date": evento['dates']['start'].get('localDate'),  # Puedes ajustarlo si hay un campo de fin
                    "country": codeIso,
                    "state": evento['_embedded']['venues'][0].get('state', {}).get('name'),
                    "town": evento['_embedded']['venues'][0]['city']['name'],
                    "alertType": "Cultural",
                    "alertSubType": "",
                    "description": evento['name'],
                    "url": evento['url']
                }
                print(sports_alert)
                grafanaVisualization.processSportsDataToInfluxDB(sports_alert)

        eventos_musica = ticketmasterService.obtener_eventos_musica((datetime.today() + timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ"),codeIso)
        for evento in eventos_musica:
                music_alert = {
                    "local_start_date": evento['dates']['start']['localDate'],
                    "local_end_date": evento['dates']['start'].get('localDate'),  # Puedes ajustarlo si hay un campo de fin
                    "country": codeIso,
                    "state": evento['_embedded']['venues'][0].get('state', {}).get('name'),
                    "town": evento['_embedded']['venues'][0]['city']['name'],
                    "alertType": "Music",
                    "alertSubType": "",
                    "description": evento['name'],
                    "url" : evento['url']
                }
                print(music_alert)
                grafanaVisualization.processMusicDataToInfluxDB(music_alert)

        eventos_miscelanea = ticketmasterService.obtener_eventos_miscelanea(
            (datetime.today() + timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ"), codeIso)
        for evento in eventos_miscelanea:
            alert = {
                "local_start_date": evento['dates']['start']['localDate'],
                "local_end_date": evento['dates']['start'].get('localDate'),  # Puedes ajustarlo si hay un campo de fin
                "country": codeIso,
                "state": evento['_embedded']['venues'][0].get('state', {}).get('name'),
                "town": evento['_embedded']['venues'][0]['city']['name'],
                "alertType": "Miscellaneous",
                "alertSubType": "",
                "description": evento['name'],
                "url": evento['url']
            }
            print(alert)
            grafanaVisualization.processMiscDataToInfluxDB(alert)

        eventos_ferias = ticketmasterService.obtener_eventos_festivales(
            (datetime.today() + timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ"), codeIso)
        for evento in eventos_ferias:
            ferial_alert = {
                "local_start_date": evento['dates']['start']['localDate'],
                "local_end_date": evento['dates']['start'].get('localDate'),  # Puedes ajustarlo si hay un campo de fin
                "country": codeIso,
                "state": evento['_embedded']['venues'][0].get('state', {}).get('name'),
                "town": evento['_embedded']['venues'][0]['city']['name'],
                "alertType": "Festival",
                "alertSubType": "",
                "description": evento['name'],
                "url": evento['url']
            }
            print(ferial_alert)
            grafanaVisualization.processFestivalDataToInfluxDB(ferial_alert)

        eventos_moda = ticketmasterService.obtener_eventos_moda(
            (datetime.today() + timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ"), codeIso)
        for evento in eventos_moda:
            alert = {
                "local_start_date": evento['dates']['start']['localDate'],
                "local_end_date": evento['dates']['start'].get('localDate'),  # Puedes ajustarlo si hay un campo de fin
                "country": codeIso,
                "state": evento['_embedded']['venues'][0].get('state', {}).get('name'),
                "town": evento['_embedded']['venues'][0]['city']['name'],
                "alertType": "Fashion",
                "alertSubType": "Big Venue",
                "description": evento['name'],
                "url": evento['url']
            }
            print(alert)
            grafanaVisualization.processFashionDataToInfluxDB(alert)



    peliculas = TMDbService.obtener_peliculas_taquilleras()

    for pelicula in peliculas:
        movie_alert = {
            "local_start_date": pelicula['release_date'],
            "local_end_date": '',
            "country": '',
            "state": '',
            "town": '',
            "alertType": 'Movie',
            "alertSubType": pelicula['original_title'],
            "description": 'Popularity Index: ' + str(pelicula['popularity'])
        }
        print(movie_alert)
        grafanaVisualization.processMediaDataToInfluxDB(movie_alert)


    series = TMDbService.obtener_series_populares()

    for serie in series:
        serie_alert = {
            "local_start_date": serie['first_air_date'],
            "local_end_date": '',
            "country": serie['origin_country'][0],
            "state": '',
            "town": '',
            "alertType": 'Serie',
            "alertSubType": serie['name'],
            "description": 'Popularity Index: ' + str(serie['popularity'])
        }
        print(serie_alert)
        grafanaVisualization.processMediaDataToInfluxDB(serie_alert)




    countryNames = list(countries.values())

    print("¿Qué información deseas consultar?")
    print("1. Vacaciones")
    print("2. Meteorología")
    opcion = input("Introduce el número correspondiente (1 o 2): ")

    if opcion == "1":
        consulta = "vacaciones"
    elif opcion == "2":
        consulta = "meteorología"
    else:
        print("Opción no válida. Saliendo del programa.")
        return

    print("\n¿Deseas consultar un país en concreto o todos los países?")
    print("1. Un país en concreto")
    print("2. Todos los países")
    pais_opcion = input("Introduce el número correspondiente (1 o 2): ")

    if pais_opcion == "1":
        # Consultar un país en específico
        print("\nElige un país:")
        for code, name in countries.items():
            print(f"{code}: {name}")
        country_code = input("Introduce el país (ejemplo: ES): ")

        if country_code not in countries.keys():
            print("País no válido. Saliendo del programa.")
            return

        if consulta == "vacaciones":
            print(f"Consultando vacaciones para {countries[country_code]}")
            getHolidays(country_code, countries.get(country_code))


        elif consulta == "meteorología":
            print(f"Consultando meteorología para {countries[country_code]}")
            getMeteo(country_code, countries.get(country_code))

    elif pais_opcion == "2":
        if consulta == "vacaciones":
            print("Consultando vacaciones para todos los países...")
            for codeIso, countryName in countries.items():
                print(f"Consultando vacaciones para {countryName}")
                getHolidays(codeIso,countryName)

        elif consulta == "meteorología":
            print("Consultando meteorología para todos los países...")
            for codeIso, countryName in countries.items():
                print(f"Consultando meteorología para {countryName}")
                getMeteo(codeIso,countryName)


    else:
        print("Opción no válida. Saliendo del programa.")
        return

    disaster_data = NASAService.get_natural_events()
    if disaster_data:
        print(json.dumps(disaster_data, indent=4))
        grafanaVisualization.processWeatherDataToInfluxDB(disaster_data)

    # Logistic regression para tener datos de probabilidades de ventas en festivos

    """if countries:
        # Obtener festivos locales si no los tenemos ya guardados
        for country in geonamesService.get_countries() or []:
            for state in geonamesService.get_states(country) or []:
                holiday_data = calendarificService.get_holidays_by_state(state, country, 2025)
                if holiday_data:
                    print(f"Festivos en {state} del país {country} para el año 2024:")
                    print(json.dumps(holiday_data, indent=4))
                    producer.send('holiday_topic', holiday_data)  # Enviar datos a Kafka """
    """
    news_data = alphaVantageService.fetch_financial_news()
    
    relevant_financial_news = []
    if news_data:
        for article in news_data:
            # Example of what data might look like
            if article['topics']:
                for topic in article['topics']:
                    if topic['relevance_score'] > 0.85:
                        news_item = {
                            'title': article['title'],
                            'url': article['url'],
                            'summary': article.get('summary', ''),
                            'date': article['time_published']
                        }
                        print(f"Sending news item to Kafka: {news_item}")
                        relevant_financial_news.add(news_item);
                        # Clasificar el evento
                        category = categorization.classify_event(news_item)

                        if category:
                            # Enviar el evento a los departamentos correspondientes
                            productor(category, news_item)
                        else:
                            print("No se pudo clasificar el evento.")


    else:
        print("No news data available")

    # Obtener comentarios de Reddit
    #Sin entrenamiento, no necesitamos predecir nada aqui
    Zaracomments = redditService.get_reddit_news_about_brand('Zara')
    productor('Zara_topic', Zaracomments)
    
    pyFlinkConsumer.process_stream(env)
    """
    #grafanaVisualization.processDataToInfluxDB(disaster_data)
    #grafanaVisualization.processDataToInfluxDB(relevant_financial_news)
    #grafanaVisualization.processDataToInfluxDB(Zaracomments)


def getMeteo(codeIso,countryName):
        print(f"Consultando geonames para: {countryName}")

        # Obtener geonames de ese país
        geonames = geonamesService.get_geonames(countryName)

        # Filtrar y procesar los resultados de geonames
        for geonameid, geoname_name in geonames.items():
            if countries.get(codeIso) == geoname_name:
                # Caso especial para Irlanda (IE): Filtrar subdivisiones que pertenezcan al Reino Unido
                if codeIso == 'IE':
                    geonameid = geonamesService.get_geonames('Ireland')
                firstDivision = geonamesService.get_states(geonameid,codeIso)
                for first in firstDivision:
                    if first['name'] == 'Catalonia':
                        state_names = geonamesService.get_provinces(first['geonameId'],codeIso)
                        #STATE_NAMES son las comunidades (Galicia, Andalucía...)
                        for state_name in state_names:
                                state = state_name['adminName1']
                                stateIso3166_2 = state_name['adminCodes1'].get('ISO3166_2')
                                alreadyProcessedLocation = []
                                if state_name['name'] not in alreadyProcessedLocation:
                                    provinces = geonamesService.get_states(state_name['geonameId'],codeIso)
                                    if not provinces or provinces == []:
                                        postalCodes = geonamesService.obtener_codigos_postales(state_name['name'], codeIso)
                                        alreadyProcessedLocation.append(state_name['name'])
                                        if postalCodes:
                                            for postalCode in postalCodes:
                                                weather_data = openWeatherService.get_forecast_geoweather(postalCode[0],
                                                                                                          postalCode[1],
                                                                                                          stateIso3166_2, state,
                                                                                                          state_name['name'])
                                                time.sleep(2.9)  # 1.000.000 llamadas al mes, permite unas 20 al minuto
                                                if weather_data:
                                                    print(json.dumps(weather_data, indent=4))
                                                    grafanaVisualization.processWeatherDataToInfluxDB(weather_data)
                                    else:
                                        for province in provinces:
                                            if province['name'] not in alreadyProcessedLocation:
                                                town_names = geonamesService.get_district(province['geonameId'])
                                                if not town_names or town_names == [] :
                                                    alreadyProcessedLocation.append(province['name'])
                                                    postalCodes = geonamesService.obtener_codigos_postales(province['name'], codeIso)
                                                    if postalCodes:
                                                        for postalCode in postalCodes:
                                                            weather_data = openWeatherService.get_forecast_geoweather(postalCode[0],
                                                                                                                      postalCode[1],
                                                                                                                      stateIso3166_2, state,
                                                                                                                      province['name'])
                                                            time.sleep(2.9)  # 1.000.000 llamadas al mes, permite unas 20 al minuto
                                                            if weather_data:
                                                                print(json.dumps(weather_data, indent=4))
                                                                grafanaVisualization.processWeatherDataToInfluxDB(weather_data)
                                                else:
                                                    for town in town_names:
                                                        if town['name'] not in alreadyProcessedLocation:
                                                            postalCodes = geonamesService.obtener_codigos_postales(town['name'], codeIso)
                                                            alreadyProcessedLocation.append(town['name'])
                                                            if postalCodes:
                                                                for postalCode in postalCodes:
                                                                    weather_data = openWeatherService.get_forecast_geoweather(postalCode[0], postalCode[1],
                                                                                                                                          stateIso3166_2,state,town['name'])
                                                                    time.sleep(2.9)  # 1.000.000 llamadas al mes, permite unas 20 al minuto
                                                                    if weather_data:
                                                                        print(json.dumps(weather_data, indent=4))
                                                                        grafanaVisualization.processWeatherDataToInfluxDB(weather_data)
                                                                                    # productor(producer,'weather_topic', weather_data)  # Enviar datos a Kafka


def getHolidays(codeIso,countryName):

        # Obtener geonames de ese país
        geonames = geonamesService.get_geonames(countryName)

        # Filtrar y procesar los resultados de geonames
        for geonameid, geoname_name in geonames.items():
            if countries.get(codeIso) == geoname_name:
                # Caso especial para Irlanda (IE): Filtrar subdivisiones que pertenezcan al Reino Unido
                if codeIso == 'IE':
                    geonameid = geonamesService.get_geonames('Ireland')
                firstDivision = geonamesService.get_states(geonameid,codeIso)
                for first in firstDivision:
                    holiday_data = calendarificService.get_holidays_by_state(first['adminName1'],
                                                                             str(first[
                                                                                     'countryCode']).lower() + "-" + str(
                                                                                 first['adminCodes1'].get(
                                                                                     'ISO3166_2')).lower(),
                                                                             codeIso,
                                                                             2025)
                    if holiday_data :
                        print(json.dumps(holiday_data, indent=4))
                        grafanaVisualization.processHolidayDataToInfluxDB(holiday_data)
                                        # productor('holiday_topic', holiday_data)


def productor(producer, topic, data):
    producer.send(topic,data)
    opsgenie.monitor_system(data)

if __name__ == "__main__":
    main()
