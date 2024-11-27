import requests
import json
import os
from datetime import datetime

# Lista de criptomonedas que quieres consultar
symbols = ["PEPE", "TRX", "XRP", "THE", "USDT", "DOGE"]

# Moneda de destino (USD por defecto)
target_currency = "USD"

# URL de la API de CryptoCompare
base_url = "https://min-api.cryptocompare.com/data/pricemultifull"

# Tu API Key de CryptoCompare (se recomienda cargar desde variable de entorno o archivo de configuración)
api_key = os.getenv("CRYPTOCOMPARE_API_KEY", "tu_api_key_aqui")  # Usa variable de entorno

# Parámetros de la consulta
fsyms = ",".join(symbols)  # Criptomonedas a consultar
tsyms = target_currency  # Moneda de destino

params = {
    'fsyms': fsyms,
    'tsyms': tsyms,
    'api_key': api_key
}

# Realizar la solicitud GET a la API
try:
    response = requests.get(base_url, params=params)
    response.raise_for_status()  # Lanza una excepción si el código de estado es 4xx o 5xx
except requests.exceptions.RequestException as e:
    print(f"Error al obtener los datos: {e}")
    exit()

# Verificar que la solicitud fue exitosa
if response.status_code == 200:
    data = response.json()

    # Filtrar los datos relevantes: solo el symbol_pair, price y timestamp
    filtered_data = []
    if "RAW" in data:
        for symbol, info in data['RAW'].items():
            for currency, details in info.items():
                # Extraer la información relevante
                price = details.get('PRICE', 'N/A')
                timestamp = details.get('LASTUPDATE', 'N/A')

                # Convertir timestamp si es válido
                if timestamp != 'N/A':
                    try:
                        timestamp = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S')
                    except (ValueError, TypeError):
                        timestamp = 'Invalid timestamp'
                
                filtered_data.append({
                    'symbol_pair': f"{symbol}{currency}",  # Ejemplo: PEPEUSD
                    'price': price,
                    'timestamp': timestamp,
                })

    # Crear el directorio para guardar el archivo si no existe
    output_dir = 'crypto_etl_project/data/cryptocompare'
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'crypto_prices.json')

    # Guardar los datos en un archivo JSON
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=4)
        print(f"Datos guardados correctamente en {output_file}")
    except Exception as e:
        print(f"Error al guardar los datos en el archivo: {e}")

else:
    print(f"Error al obtener los datos: {response.status_code}")
