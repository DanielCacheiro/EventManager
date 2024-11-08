import time

import requests

# Your Alpha Vantage API key
ALPHA_VANTAGE_API_KEY = 'av'


# Alpha Vantage news endpoint
def fetch_financial_news():
    keywords = ['ECONOMY_FISCAL', 'FINANCE', 'ECONOMY_MONETARY', 'ECONOMY_MACRO', 'ECONOMY_GLOBAL', 'RETAIL', 'WHOLESALE', 'TECHNOLOGY']
    all_news_items = []

    for keyword in keywords:
        url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&topics={keyword}&sort=RELEVANCE&limit=100&apikey={ALPHA_VANTAGE_API_KEY}'
        response = requests.get(url, verify=False)
        data = response.json()
        time.sleep(5)
        if 'feed' in data:
            news_items = data['feed']
            all_news_items.extend(news_items)  # Añadir noticias de cada consulta
        else:
            print(f"No se encontraron noticias para el tema: {keyword}")

    return all_news_items if all_news_items else None
