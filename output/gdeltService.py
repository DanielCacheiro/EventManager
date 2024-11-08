import time

import requests
from datetime import datetime
from urllib.parse import quote
from gdeltdoc import near

def fetch_gdelt_news():
    all_alerts = []
    categories = {
        "influencer_promotion": ["near8:\"influencer endorsement\"", "near10:\"brand ambassador\"",
                                 "near10:\"celebrity promotion\""],
        "election": ["\"presidential election\"", "\"general election\""],
        "regulations": ["\"new regulation\"", "\"law change\""],
        "environmental_regulations": [
            "near10:\"environmental regulation\"" , "\"green policy\"" , "\"sustainability law\""],
        "pandemic": ["near10:\"public health emergency\"", "\"pandemic\"" , "\"epidemic\""],
        "tax_change": ["\"tax change\"" , "\"tax increase\""],
        "competitors_launches": ["near10:\"TEMU launch\"", "near10:\"SHEIN launch\"", "near10:\"MANGO launch\"",
                           "near10:\"UNIQLO launch\""],
        "INDITEX": ["near10:\"ZARA inditex\"", "near10:\"lanza inditex\"", "near10:\"ZARA lanza\""],
        "port_blockade": ["near10:\"port blockade disruption\"", "near10:\"shipping disruption\"", "near10:\"port closure\""],
        "transport_strike": ["near10:\"transport strike\"", "near10:\"logistics strike\"", "near10:\"supply chain disruption\""]
    }

    gdelt_url = "http://api.gdeltproject.org/api/v2/doc/doc"
    all_events = []
    printed_titles = set()  # Conjunto para almacenar títulos ya impresos

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    for category, queries in categories.items():
        for query in queries:
            params = {
                "query": query,
                "mode": "ArtList",
                "format": "json",
                "maxrecords": 5,
                "sort": "HybridRel",
                "timespan": "1w"
            }
            response = requests.get(gdelt_url, params=params, headers=headers)
            time.sleep(6)
            if response.status_code == 200:
                if 'application/json' in response.headers.get('Content-Type'):
                    data = response.json()
                    if 'articles' in data:
                        all_events.extend(data['articles'])
                        for event in all_events:
                            if event.get('title') not in printed_titles:
                                alert = {
                                    "time_published": datetime.strptime(event.get('seendate'), '%Y%m%dT%H%M%SZ').strftime(
                                        '%Y-%m-%d'),
                                    "alertType": "Event News",
                                    "alertSubType": category,
                                    "country": event.get('sourcecountry'),
                                    "description": event.get('title'),
                                    "url": event.get('url')
                                }
                                all_alerts.append(alert)
                                printed_titles.add(event.get('title'))
                else:
                    print(f"La respuesta no es un JSON válido: {response.headers.get('Content-Type')}")
            else:
                print(f"Error al obtener noticias para la palabra clave {query}: {response.status_code}")

    return all_alerts