from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import time
from webdriver_manager.chrome import ChromeDriverManager
import requests


# Configura Selenium WebDriver (ejemplo con Chrome)

# Busca la sección que contiene las apuestas especiales
# Esto puede cambiar dependiendo de la estructura de la página
def obtener_apuestas_especiales():
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    # Navega a la página de apuestas especiales
    driver.get('https://www.betfair.es/sport/special-bets')

    # Espera unos segundos para que se cargue el contenido dinámico
    time.sleep(2)  # Ajusta el tiempo según sea necesario

    # Obtén el contenido HTML de la página
    html = driver.page_source

    # Analiza el HTML con BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    competition_soup = soup.find_all('li', class_='competition-item')
    allEvents = []

    # Iterar sobre cada competición
    for competition in competition_soup:
        # Extraer el enlace y nombre de cada competición
        competition_link = competition.find('a')['href']
        competition_name = competition.find('a').get('title')

        # Ir a la página de la competición
        competition_url = f"https://www.betfair.es{competition_link}"
        comp_response = requests.get(competition_url)
        comp_soup = BeautifulSoup(comp_response.content, 'html.parser')

        # Encontrar todos los mercados dentro de la competición
        market_soup = comp_soup.find_all('li', class_='multipick-market-selector-item')

        # Iterar sobre cada mercado en la competición
        for market in market_soup:
            # Extraer el nombre del evento y el enlace al mercado
            event_name = market.find('span', class_='label').get_text().strip()
            market_link = market.find('a')['href']

            # Ir a la página del mercado específico
            market_url = f"https://www.betfair.es{market_link}"
            market_response = requests.get(market_url)
            mark_soup = BeautifulSoup(market_response.content, 'html.parser')

            # Encontrar el contenedor de apuestas dentro del mercado específico
            bets_container = mark_soup.find('div', class_='events-body')
            if bets_container:
                runners = bets_container.find_all('li', class_='ui-runner')

                # Iterar sobre las opciones de apuesta dentro del contenedor de apuestas
                for runner in runners:
                    name = runner.find('span', class_='runner-name').text.strip() if runner.find('span',
                                                                                                 class_='runner-name') else None
                    price = runner.find('span', class_='ui-runner-price').text.strip() if runner.find('span',
                                                                                                      class_='ui-runner-price') else None

                    if price and name:
                        try:
                            # Convertir el precio a flotante y calcular la probabilidad
                            probability = f"{90 / float(price.replace(',', '.')):.2f}"
                            alert = {
                                "alertType": "Prediction",
                                "alertSubType": competition_name,
                                "description": event_name,
                                "selection": name.replace('"', "'"),
                                "probability": probability
                            }
                            allEvents.append(alert)
                        except ValueError:
                            print(f"No se puede calcular la probabilidad para el precio {price}")

    return allEvents


    # Cierra el navegador
    driver.quit()