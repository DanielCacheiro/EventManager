import time

import instaloader
from datetime import datetime, timedelta

# Inicializar Instaloader
loader = instaloader.Instaloader()

def get_instagram_alerts():
    alerts = []
    one_week_ago = datetime.now() - timedelta(days=7)
    for influencer in influencers:
        try:
            profile = instaloader.Profile.from_username(loader.context, influencer)
            time.sleep(180)
            for post in profile.get_posts():
                if post.date < one_week_ago:
                    continue
                alert = {
                    "time_published": post.date.strftime('%Y-%m-%d'),
                    "alertType": "Social Media",
                    "alertSubType": influencer,
                    "description": post.caption if post.caption else "No caption",
                    "likes": post.likes,
                    "url": f"https://www.instagram.com/p/{post.shortcode}/"
                }
                alerts.append(alert)
        except Exception as e:
            time.sleep(180)
            print(f"Error al obtener publicaciones de {influencer}: {e}")

    alerts = sorted(alerts, key=lambda x: x['likes'], reverse=True)

    return alerts


# Ejemplo de uso
influencers = ["chiaraferragni", "leoniehanne","lornaluxe", "xeniaadonts", "martaa_diiaz", "mariapombo", "sarabace"]
all_alerts = []

