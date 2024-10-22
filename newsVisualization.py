import pandas as pd

# Cargar los datos
news_classifications = pd.read_json('news_classifications.json')

# Visualizar los resultados
print(news_classifications.head())