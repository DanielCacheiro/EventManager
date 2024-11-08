from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS,ASYNCHRONOUS

# Configurar conexión a InfluxDB
client = InfluxDBClient(url="http://localhost:8086", token="6Z12VztefRHkpNvIAVFhw_UR4epLuYcka8vrDiHyPeVoEZq8asp-9FcksgJfVOjaCMK2N2xqi1kcRBEYYjc36Q==", org="Inditex")
write_api = client.write_api(write_options=ASYNCHRONOUS)

# Formatear y escribir los datos procesados a InfluxDB
def processWeatherDataToInfluxDB(alert_data_list):
    if isinstance(alert_data_list, list):
        for alert_data in alert_data_list:  # Iteramos sobre la lista de alertas
            if not check_weather_added(alert_data['town'], alert_data['alertSubType'], alert_data['local_start_date'], alert_data['local_end_date'], alert_data['country']):
                point = Point("weather_alerts") \
                    .tag("town", alert_data['town']) \
                    .tag("country", alert_data['country']) \
                    .tag("state", alert_data['state']) \
                    .tag("alertType", alert_data['alertType']) \
                    .tag("alertSubType", alert_data['alertSubType']) \
                    .field("description", alert_data['description']) \
                    .field("start_date", alert_data['local_start_date']) \
                    .field("end_date", alert_data['local_end_date'])
                try:
                    write_api.write(bucket="Primeras pruebas", record=point)
                except Exception as e:
                    print(f"Error al insertar datos: {e}")

# Formatear y escribir datos de días festivos a InfluxDB
def processHolidayDataToInfluxDB(alert_data_list):
    if isinstance(alert_data_list, list):
        for alert_data in alert_data_list:
            for alert in alert_data:
                if not check_holiday_added(alert['state'], alert['local_start_date'], alert['country']):
                    point = Point("holiday_alerts") \
                        .tag("town", alert['town']) \
                        .tag("country", alert['country']) \
                        .tag("state", alert['state']) \
                        .tag("alertType", alert['alertType']) \
                        .tag("alertSubType", alert['alertSubType']) \
                        .field("description", alert['description']) \
                        .field("start_date", alert['local_start_date']) \
                        .field("end_date", alert['local_end_date'])
                    try:
                        write_api.write(bucket="Primeras pruebas", record=point)
                    except Exception as e:
                        print(f"Error al insertar datos: {e}")

def processMediaDataToInfluxDB(alert):
    if isinstance(alert, dict):
        if not check_media_added(alert['alertSubType']):
                    point = Point("media_alerts") \
                        .tag("town", alert['town']) \
                        .tag("country", alert['country']) \
                        .tag("state", alert['state']) \
                        .tag("alertType", alert['alertType']) \
                        .tag("alertSubType", alert['alertSubType']) \
                        .field("description", alert['description']) \
                        .field("start_date", alert['local_start_date']) \
                        .field("end_date", alert['local_end_date'])
                    try:
                        write_api.write(bucket="Primeras pruebas", record=point)
                    except Exception as e:
                        print(f"Error al insertar datos: {e}")

def processMusicDataToInfluxDB(alert):
                if not check_music_added(alert['local_start_date'], alert['country'], alert['description']):
                    point = Point("music_alerts") \
                        .tag("town", alert['town']) \
                        .tag("country", alert['country']) \
                        .tag("state", alert['state']) \
                        .tag("alertType", alert['alertType']) \
                        .tag("alertSubType", alert['alertSubType']) \
                        .field("description", alert['description']) \
                        .field("start_date", alert['local_start_date']) \
                        .field("end_date", alert['local_end_date']) \
                        .field("url", alert['url'])
                    try:
                        write_api.write(bucket="Primeras pruebas", record=point)
                    except Exception as e:
                        print(f"Error al insertar datos: {e}")

def processSportsDataToInfluxDB(alert):
    if not check_music_added(alert['local_start_date'], alert['country'], alert['description']):
        point = Point("sports_alerts") \
            .tag("town", alert['town']) \
            .tag("country", alert['country']) \
            .tag("state", alert['state']) \
            .tag("alertType", alert['alertType']) \
            .tag("alertSubType", alert['alertSubType']) \
            .field("description", alert['description']) \
            .field("start_date", alert['local_start_date']) \
            .field("end_date", alert['local_end_date']) \
            .field("url", alert['url'])
        try:
            write_api.write(bucket="Primeras pruebas", record=point)
        except Exception as e:
            print(f"Error al insertar datos: {e}")

def processMiscDataToInfluxDB(alert):
    if not check_music_added(alert['local_start_date'], alert['country'], alert['description']):
        point = Point("misc_alerts") \
            .tag("town", alert['town']) \
            .tag("country", alert['country']) \
            .tag("state", alert['state']) \
            .tag("alertType", alert['alertType']) \
            .tag("alertSubType", alert['alertSubType']) \
            .field("description", alert['description']) \
            .field("start_date", alert['local_start_date']) \
            .field("end_date", alert['local_end_date']) \
            .field("url", alert['url'])
        try:
            write_api.write(bucket="Primeras pruebas", record=point)
        except Exception as e:
            print(f"Error al insertar datos: {e}")

def processCulturalDataToInfluxDB(alert):
                if not check_music_added(alert['local_start_date'], alert['country'], alert['description']):
                    point = Point("cultural_alerts") \
                        .tag("town", alert['town']) \
                        .tag("country", alert['country']) \
                        .tag("state", alert['state']) \
                        .tag("alertType", alert['alertType']) \
                        .tag("alertSubType", alert['alertSubType']) \
                        .field("description", alert['description']) \
                        .field("start_date", alert['local_start_date']) \
                        .field("end_date", alert['local_end_date']) \
                        .field("url", alert['url'])
                    try:
                        write_api.write(bucket="Primeras pruebas", record=point)
                    except Exception as e:
                        print(f"Error al insertar datos: {e}")


def insertPredictionsDataToInfluxDB(alert):
        point = Point("prediction_alerts") \
            .tag("alertType", alert['alertType']) \
            .tag("alertSubType", alert['alertSubType']) \
            .field("description", alert['description']) \
            .field("selection", alert['selection']) \
            .field("probability", alert['probability'])
        try:
            write_api.write(bucket="Primeras pruebas", record=point, method="UPSERT")
        except Exception as e:
            print(f"Error al insertar datos: {e}")


def processNewsToInfluxDB(alert):
    point = Point("news_alerts") \
        .tag("alertType", alert['alertType']) \
        .tag("alertSubType", alert['alertSubType']) \
        .field("description", alert['description']) \
        .field("url", alert['url']) \
        .field("time_published", alert['time_published'])
    try:
        write_api.write(bucket="Primeras pruebas", record=point, method="UPSERT")
    except Exception as e:
        print(f"Error al insertar datos: {e}")

def processFestivalDataToInfluxDB(alert):
                #if not check_festival_added(alert['state'], alert['local_start_date'], alert['country'], alert['description']):
                    point = Point("festival_alerts") \
                        .tag("town", alert['town']) \
                        .tag("country", alert['country']) \
                        .tag("state", alert['state']) \
                        .tag("alertType", alert['alertType']) \
                        .tag("alertSubType", alert['alertSubType']) \
                        .field("description", alert['description']) \
                        .field("start_date", alert['local_start_date']) \
                        .field("end_date", alert['local_end_date']) \
                        .field("url", alert['url'])
                    try:
                        write_api.write(bucket="Primeras pruebas", record=point)
                    except Exception as e:
                        print(f"Error al insertar datos: {e}")

def processFashionDataToInfluxDB(alert):
                if not check_fashion_added(alert['state'], alert['local_start_date'], alert['country'], alert['description']):
                    point = Point("fashion_alerts") \
                        .tag("town", alert['town']) \
                        .tag("country", alert['country']) \
                        .tag("state", alert['state']) \
                        .tag("alertType", alert['alertType']) \
                        .tag("alertSubType", alert['alertSubType']) \
                        .field("description", alert['description']) \
                        .field("start_date", alert['local_start_date']) \
                        .field("end_date", alert['local_end_date']) \
                        .field("url", alert['url'])
                    try:
                        write_api.write(bucket="Primeras pruebas", record=point)
                    except Exception as e:
                        print(f"Error al insertar datos: {e}")

def check_weather_added(state, alertSubType, start_date, end_date, country):
    query_api = client.query_api()

    query = f'''
        from(bucket: "Primeras pruebas")
          |> range(start: -10d)
          |> filter(fn: (r) => r["_alertType"] == "Weather")
          |> filter(fn: (r) => r["_alertSubType"] == "{alertSubType}")
          |> filter(fn: (r) => r["state"] == "{state}")
          |> filter(fn: (r) => r["country"] == "{country}")
          |> filter(fn: (r) => r["_field"] == "start_date")
          |> filter(fn: (r) => r["_value"] == "{start_date}")
    '''

    if end_date != '':
        query += f'''
          |> filter(fn: (r) => r["_field"] == "end_date")
          |> filter(fn: (r) => r["_value"] == "{end_date}")
    '''

    query += '''
          |> limit(n: 1)
    '''

    result = query_api.query(query=query)

    if result:
        return True
    else:
        return False

def check_fashion_added(town, start_date, country, description):
    query = f'''
    from(bucket: "Primeras pruebas")
      |> range(start: -2y)  // Ajusta el rango de tiempo si es necesario
      |> filter(fn: (r) => r["_alertType"] == "Fashion")
      |> filter(fn: (r) => r["town"] == "{town}")
      |> filter(fn: (r) => r["country"] == "{country}")
      |> filter(fn: (r) => r["_field"] == "start_date")
      |> filter(fn: (r) => r["_value"] == "{start_date}")
      |> filter(fn: (r) => r["_field"] == "description")
      |> filter(fn: (r) => r["_value"] == "{description}")
      |> limit(n: 1)
    '''


    result = client.query_api().query(query)

    if result:
        return True
    else:
        return False

def check_music_added(start_date, country, description):
    query = f'''
    from(bucket: "Primeras pruebas")
      |> range(start: -2y)  // Ajusta el rango de tiempo si es necesario
      |> filter(fn: (r) => r["_alertType"] == "Music")
      |> filter(fn: (r) => r["country"] == "{country}")
      |> filter(fn: (r) => r["_field"] == "start_date")
      |> filter(fn: (r) => r["_value"] == "{start_date}")
      |> filter(fn: (r) => r["_field"] == "description")
      |> filter(fn: (r) => r["_value"] == "{description}")
      |> limit(n: 1)
    '''

    result = client.query_api().query(query)

    if result:
        return True
    else:
        return False
def check_holiday_added(town, start_date, country):
    query = f'''
    from(bucket: "Primeras pruebas")
      |> range(start: -2y)  // Ajusta el rango de tiempo si es necesario
      |> filter(fn: (r) => r["_alertType"] == "Holiday")
      |> filter(fn: (r) => r["town"] == "{town}")
      |> filter(fn: (r) => r["country"] == "{country}")
      |> filter(fn: (r) => r["_field"] == "start_date")
      |> filter(fn: (r) => r["_value"] == "{start_date}")
      |> limit(n: 1)
    '''


    result = client.query_api().query(query)

    if result:
        return True
    else:
        return False

def check_media_added(subtype):
    query = f'''
    from(bucket: "Primeras pruebas")
      |> range(start: -2y)  // Ajusta el rango de tiempo si es necesario
      |> filter(fn: (r) => r["_alertType"] == "Media")
      |> filter(fn: (r) => r["alertSubType"] == "{subtype}")
      |> limit(n: 1)
    '''


    result = client.query_api().query(query)

    if result:
        return True
    else:
        return False

def check_cultural_added(subtype):
    query = f'''
    from(bucket: "Primeras pruebas")
      |> range(start: -2y)  // Ajusta el rango de tiempo si es necesario
      |> filter(fn: (r) => r["_alertType"] == "Cultural")
      |> filter(fn: (r) => r["alertSubType"] == "{subtype}")
      |> limit(n: 1)
    '''


    result = client.query_api().query(query)

    if result:
        return True
    else:
        return False

def check_festival_added(town, start_date, country, description):
    query = f'''
    from(bucket: "Primeras pruebas")
      |> range(start: -2y)  // Ajusta el rango de tiempo si es necesario
      |> filter(fn: (r) => r["_alertType"] == "Festival")
      |> filter(fn: (r) => r["town"] == "{town}")
      |> filter(fn: (r) => r["country"] == "{country}")
      |> filter(fn: (r) => r["start_date"] == "{start_date}")
      |> filter(fn: (r) => r["description"] == "{description}")
      |> limit(n: 1)
    '''

    result = client.query_api().query(query)

    if result:
        return True
    else:
        return False