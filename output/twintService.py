import twint
from datetime import datetime

# Lista de nombres de usuario de influencers a monitorear
influencers = ["influencer1", "influencer2", "influencer3"]
, "near3:\"Chiara Ferragni\"", "near3:\"Leonie Hanne\"","near3:\"Lorna Luxe\"","near3:\"Xenia Adonts\"","near3:\"Marta Díaz\"","near3:\"María Pombo\"","near3:\"Sara Baceiredo\""
search_terms = ["sponsored", "ad", "promo"]

# Lista para almacenar las alertas generadas
alerts = []

# Función para crear y ejecutar la búsqueda de tweets
def generate_alerts_for_influencer(username):
    c = twint.Config()
    c.Username = username
    c.Search = " OR ".join(search_terms)  # Búsqueda por términos promocionales
    c.Store_object = True
    c.Limit = 10  # Límite de tweets por usuario para evitar sobrecarga
    c.Hide_output = True  # Oculta la salida en consola

    # Ejecutar búsqueda y obtener tweets
    twint.run.Search(c)
    tweets = twint.output.tweets_list

    # Crear alerta para cada tweet encontrado
    for tweet in tweets:
        alert = {
            "time_published": datetime.strptime(tweet.datestamp, '%Y-%m-%d').strftime('%Y-%m-%d'),
            "alertType": "Social Media",
            "alertSubType": "Influencer Promo",
            "description": tweet.tweet,
            "url": f"https://twitter.com/{username}/status/{tweet.id}"
        }
        alerts.append(alert)

# Generar alertas para cada influencer
for influencer in influencers:
    generate_alerts_for_influencer(influencer)

# Imprimir todas las alertas generadas
for alert in alerts:
    print(alert)