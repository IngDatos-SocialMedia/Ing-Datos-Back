import requests
import json
import os
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Tu clave de API de CoinMarketCap
api_key = "2ae2db9c-f876-4822-9566-f961a7c84f79"

# URL de la API de CoinMarketCap para obtener los datos
url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

# Lista de criptomonedas a consultar
symbols = ['BTC', 'ETH', 'LTC', 'BNB', 'ADA', 'SOL']  # Agrega más símbolos según lo necesites

# Parámetros de la consulta
params = {
    'start': '1',  # Iniciar desde el primer resultado
    'limit': '10',  # Obtener los 10 primeros
    'convert': 'EUR'  # Convertir el precio a EUR
}

# Encabezados para la autenticación de la API
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': api_key,
}

# Realizar la solicitud GET a la API
response = requests.get(url, headers=headers, params=params)

# Verificar que la solicitud fue exitosa
if response.status_code == 200:
    data = response.json()

    # Filtrar solo los datos de las criptomonedas que queremos
    filtered_data = []
    for coin in data['data']:
        if coin['symbol'] in symbols:
            price = coin['quote']['EUR']['price']
            # Extraer más datos
            market_cap = coin['quote']['EUR']['market_cap']
            volume_24h = coin['quote']['EUR']['volume_24h']
            percent_change_1h = coin['quote']['EUR']['percent_change_1h']
            percent_change_24h = coin['quote']['EUR']['percent_change_24h']
            percent_change_7d = coin['quote']['EUR']['percent_change_7d']
            circulating_supply = coin['circulating_supply']
            max_supply = coin.get('max_supply', 'N/A')  # Si no existe, devolvemos 'N/A'
            cmc_rank = coin['cmc_rank']

            # Almacenar la información adicional
            filtered_data.append({
                'name': coin['name'],
                'symbol': coin['symbol'],
                'price': price,
                'market_cap': market_cap,
                'volume_24h': volume_24h,
                'percent_change_1h': percent_change_1h,
                'percent_change_24h': percent_change_24h,
                'percent_change_7d': percent_change_7d,
                'circulating_supply': circulating_supply,
                'max_supply': max_supply,
                'cmc_rank': cmc_rank,
                'timestamp': coin['last_updated']
            })

    # Crear la carpeta si no existe
    os.makedirs('data/coinmarketcap', exist_ok=True)

    # Guardar los resultados en un archivo JSON
    with open('crypto_etl_project/data/coinmarketcap/coin_data.json', 'w', encoding='utf-8') as f:
        json.dump(filtered_data, f, ensure_ascii=False, indent=4)

    print("Datos guardados en data/coinmarketcap/coin_data.json")

else:
    print(f"Error en la solicitud: {response.status_code}")
    print(f"Mensaje de error: {response.text}")
