from supabase import create_client, Client
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
import requests


# Configuración de Supabase
url = "https://ghwbaagimbnzyfiloues.supabase.co"  # Cambia esto por tu URL de Supabase
key = "supabase_key"  # Cambia esto por tu clave anónima de Supabase
supabase: Client = create_client(url, key)

# Montar el adaptador en la sesión HTTP
session = requests.Session()
session.verify = False  # Disable SSL verification
# Función para insertar datos en Supabase
def insert_data_to_supabase(table_name, data):
    response = supabase.table(table_name).insert(data).execute()

# Procesar y escribir datos en Supabase
def processWeatherDataToSupabase(alert_data_list):
    if isinstance(alert_data_list, list):
        for alert_data in alert_data_list:
            data = {
                "town": alert_data['town'],
                "country": alert_data['country'],
                "state": alert_data['state'],
                "alerttype": alert_data['alertType'],
                "alertsubtype": alert_data['alertSubType'],
                "description": alert_data['description'],
                "start_date": alert_data['local_start_date'],
                "end_date": alert_data['local_end_date']
            }
            if not check_weather_added(alert_data['state'], alert_data['alertSubType'], alert_data['local_start_date'], alert_data['country']):
                insert_data_to_supabase("weather_alerts", data)

def processHolidayDataToSupabase(alert_data_list):
    if isinstance(alert_data_list, list):
        for alert_data in alert_data_list:
            for alert in alert_data:
                data = {
                    "town": alert['town'],
                    "country": alert['country'],
                    "state": alert['state'],
                    "alerttype": alert['alertType'],
                    "alertsubtype": alert['alertSubType'],
                    "description": alert['description'],
                    "start_date": alert['local_start_date'],
                    "end_date": alert['local_end_date']
                }
                if not check_holiday_added(alert['state'], alert['local_start_date'], alert['country']):
                    insert_data_to_supabase("holiday_alerts", data)

def processMediaDataToSupabase(alert):
    if isinstance(alert, dict):
        data = {
            "town": alert['town'],
            "country": alert['country'],
            "state": alert['state'],
            "alerttype": alert['alertType'],
            "alertsubtype": alert['alertSubType'],
            "popularity": alert['popularity'],
            "release_date": alert['release_date'],
            "inserted_date": alert['inserted_date']
        }
        if alert['alertType'] == "Movie" and not check_movie_added(alert['alertSubType']):
            insert_data_to_supabase("media_alerts", data)
        elif alert['alertType'] == "Serie" and not check_serie_added(alert['alertSubType']):
            insert_data_to_supabase("media_alerts", data)

def processMusicDataToSupabase(alert):
    data = {
        "town": alert['town'],
        "country": alert['country'],
        "state": alert['state'],
        "alerttype": alert['alertType'],
        "alertsubtype": alert['alertSubType'],
        "description": alert['description'],
        "start_date": alert['local_start_date'],
        "end_date": alert['local_end_date'],
        "url": alert['url']
    }
    if not check_music_added(alert['local_start_date'], alert['country'], alert['description']):
        insert_data_to_supabase("music_alerts", data)

def processInstaDataToSupabase(alert):
    data = {
        "alerttype": alert['alertType'],
        "alertsubtype": alert['alertSubType'],
        "description": alert['description'],
        "likes": alert['local_start_date'],
        "time_published": alert['local_end_date'],
        "url": alert['url']
    }
    if not check_post_added(alert['alertsubtype'], alert['description']):
        insert_data_to_supabase("influencer_alerts", data)

def processSportsDataToSupabase(alert):
    data = {
        "town": alert['town'],
        "country": alert['country'],
        "state": alert['state'],
        "alerttype": alert['alertType'],
        "alertsubtype": alert['alertSubType'],
        "description": alert['description'],
        "start_date": alert['local_start_date'],
        "end_date": alert['local_end_date'],
        "url": alert['url']
    }
    if not check_sports_added(alert['local_start_date'], alert['country'], alert['description']):
        insert_data_to_supabase("sports_alerts", data)

def processCulturalDataToSupabase(alert):
    data = {
        "town": alert['town'],
        "country": alert['country'],
        "state": alert['state'],
        "alerttype": alert['alertType'],
        "alertsubtype": alert['alertSubType'],
        "description": alert['description'],
        "start_date": alert['local_start_date'],
        "end_date": alert['local_end_date'],
        "url": alert['url']
    }
    if not check_cultural_added(alert['alertSubType']):
        insert_data_to_supabase("cultural_alerts", data)

def insertPredictionsDataToSupabase(alert):
    data = {
        "alerttype": alert['alertType'],
        "alertsubtype": alert['alertSubType'],
        "description": alert['description'],
        "selection": alert['selection'],
        "probability": alert['probability']
    }
    if not check_prediction_added(alert['alertSubType'], alert['probability'], alert['selection'], alert['description']):
        insert_data_to_supabase("prediction_alerts", data)

def processNewsToSupabase(alert):
    data = {
        "alerttype": alert['alertType'],
        "alertsubtype": alert['alertSubType'],
        "description": alert['description'],
        "url": alert['url'],
        "time_published": alert['time_published']
    }
    if not check_news_added(alert['time_published'], alert['country'], alert['description']):
        insert_data_to_supabase("news_alerts", data)

def processFestivalDataToSupabase(alert):
    data = {
        "town": alert['town'],
        "country": alert['country'],
        "state": alert['state'],
        "alerttype": alert['alertType'],
        "alertsubtype": alert['alertSubType'],
        "description": alert['description'],
        "start_date": alert['local_start_date'],
        "end_date": alert['local_end_date'],
        "url": alert['url']
    }
    if not check_festival_added(alert['town'], alert['local_start_date'], alert['country'], alert['description']):
        insert_data_to_supabase("festival_alerts", data)

def processFashionDataToSupabase(alert):
    data = {
        "town": alert['town'],
        "country": alert['country'],
        "state": alert['state'],
        "alerttype": alert['alertType'],
        "alertsubtype": alert['alertSubType'],
        "description": alert['description'],
        "start_date": alert['local_start_date'],
        "end_date": alert['local_end_date'],
        "url": alert['url']
    }
    if not check_fashion_added(alert['town'], alert['local_start_date'], alert['country'], alert['description']):
        insert_data_to_supabase("fashion_alerts", data)

def check_weather_added(state, alertsubtype, start_date, country):
    response = supabase.table("weather_alerts").select("*").eq("state", state).eq("alertsubtype", alertsubtype).eq("start_date", start_date).eq("country", country).execute()
    return len(response.data) > 0

def check_post_added(alertsubtype, description):
    response = supabase.table("influencer_alerts").select("*").eq("alertsubtype", alertsubtype).eq("description", description).execute()
    return len(response.data) > 0
def check_fashion_added(town, start_date, country, description):
    response = supabase.table("festival_alerts").select("*").eq("town", town).eq("start_date", start_date).eq("country", country).eq("description", description).execute()
    return len(response.data) > 0

def check_music_added(start_date, country, description):
    response = supabase.table("music_alerts").select("*").eq("start_date", start_date).eq("country", country).eq("description", description).execute()
    return len(response.data) > 0

def check_sports_added(start_date, country, description):
    response = supabase.table("sports_alerts").select("*").eq("start_date", start_date).eq("country", country).eq("description", description).execute()
    return len(response.data) > 0

def check_holiday_added(state, start_date, country):
    response = supabase.table("holiday_alerts").select("*").eq("state", state).eq("start_date", start_date).eq("country", country).execute()
    return len(response.data) > 0

def check_holidays_added(state, country):
    response = supabase.table("holiday_alerts").select("*").eq("state", state).eq("country", country).execute()
    return len(response.data) > 0

def check_movie_added(subtype):
    response = supabase.table("media_alerts").select("*").eq("alertsubtype", subtype).eq("alerttype", "Movie").execute()
    return len(response.data) > 0

def check_serie_added(subtype):
    response = supabase.table("media_alerts").select("*").eq("alertsubtype", subtype).eq("alerttype", "Serie").execute()
    return len(response.data) > 0

def check_cultural_added(subtype):
    response = supabase.table("cultural_alerts").select("*").eq("alertsubtype", subtype).execute()
    return len(response.data) > 0

def check_festival_added(town, start_date, country, description):
    response = supabase.table("festival_alerts").select("*").eq("town", town).eq("start_date", start_date).eq("country", country).eq("description", description).execute()
    return len(response.data) > 0

def check_news_added(time_published, country, description):
    response = supabase.table("news_alerts").select("*").eq("time_published", time_published).eq("country", country).eq("description", description).execute()
    return len(response.data) > 0

def check_prediction_added(alertSubType, probability, selection, description):
    response = supabase.table("prediction_alerts").select("*").eq("alertsubtype", alertSubType).eq("probability", probability).eq("selection", selection).eq("description", description).execute()
    return len(response.data) > 0
