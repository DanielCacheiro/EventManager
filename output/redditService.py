import praw
from transformers import pipeline
import json

# Configurar Reddit API
reddit = praw.Reddit(client_id='YKvPVAkruEtrsNVJ1z__MA',
                     client_secret='reddit',
                     user_agent='Revisador de noticias')

# Inicializar los pipelines
sentiment_analyzer = pipeline('sentiment-analysis')
text_classifier = pipeline('text-classification', model='your_custom_model')


def analyze_news(news):
    analyzed_news = []

    for article in news:
        sentiment_result = sentiment_analyzer(article)[0]
        classification_result = text_classifier(article)[0]

        # Condiciones para considerar la noticia importante
        if sentiment_result['label'] in ['NEGATIVE', 'POSITIVE'] or classification_result['label'] == 'important':
            analyzed_news.append({
                'article': article,
                'sentiment': sentiment_result['label'],
                'classification': classification_result['label'],
                'important': True
            })
        else:
            analyzed_news.append({
                'article': article,
                'sentiment': sentiment_result['label'],
                'classification': classification_result['label'],
                'important': False
            })

    return analyzed_news


def send_to_kafka(producer, topic, data):
    producer.send(topic, value=json.dumps(data).encode('utf-8'))


def process_and_send_events(news_classifications, producer, topic):
    for classified_news in news_classifications:
        if classified_news['important']:
            event_data = {
                'news': classified_news['article'],
                'sentiment': classified_news['sentiment'],
                'classification': classified_news['classification'],
                'event': 'Important news about Adolfo Domínguez'
            }
            send_to_kafka(producer, topic, event_data)

def get_reddit_news_about_brand(brand_name):
    subreddit = reddit.subreddit('all')
    search_results = subreddit.search(brand_name, sort='new', limit=10)
    titles = [submission.title for submission in search_results]
    return analyze_news(titles)


