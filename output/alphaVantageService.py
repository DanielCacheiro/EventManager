import requests
from kafka import KafkaProducer
import json

# Your Alpha Vantage API key
ALPHA_VANTAGE_API_KEY = 'V6U5M5YG95AK3DQP'


# Alpha Vantage news endpoint
def fetch_financial_news():
    keywords = 'economy_fiscal,finance,economy_monetary,economy_macro,retail_wholesale,technology'
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&topics={keywords}&apikey={ALPHA_VANTAGE_API_KEY}'
    response = requests.get(url)
    data = response.json()

    if 'feed' in data:
        news_items = data['feed']
        return news_items
    else:
        return None


# Kafka Producer setup
def send_to_kafka(news_data):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send('financial_news', news_data)
