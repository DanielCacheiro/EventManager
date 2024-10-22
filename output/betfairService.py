from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import time
from webdriver_manager.chrome import ChromeDriverManager
import requests


# Configura Selenium WebDriver (ejemplo con Chrome)
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# Busca la sección que contiene las apuestas especiales
# Esto puede cambiar dependiendo de la estructura de la página
def obtener_apuestas_especiales():
    # Navega a la página de apuestas especiales
    driver.get('https://www.betfair.es/sport/special-bets')

    # Espera unos segundos para que se cargue el contenido dinámico
    time.sleep(2)  # Ajusta el tiempo según sea necesario

    # Obtén el contenido HTML de la página
    html = driver.page_source

    # Analiza el HTML con BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    competition_soup = soup.find_all('li', class_='competition-item')

    # Iterar sobre cada competición
    for competition in competition_soup:
        # Extraer el enlace de cada competición
        competition_link = competition.find('a')['href']

        competition_name = competition.find('a').get('title')

        # Ir a la página de la competición
        competition_url = f"https://www.betfair.es{competition_link}"
        comp_response = requests.get(competition_url)
        comp_soup = BeautifulSoup(comp_response.content, 'html.parser')

        # Ahora extraer las filas de las apuestas
        runners = comp_soup.find_all('li', class_='ui-runner')
        for runner in runners:
            # Extraer el nombre y la cuota
            name = runner.find('span', class_='runner-name').text
            price = runner.find('span', class_='ui-runner-price').text
            print(f"Competición: {competition_name}, Nombre: {name}, Cuota: {price}")

    # Cierra el navegador
    driver.quit()