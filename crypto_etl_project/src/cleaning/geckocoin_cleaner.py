import requests
import json
import os

# URL de la API de CoinGecko
url = "https://api.coingecko.com/api/v3/coins/markets"

# Criptomonedas a consultar (Arweave, Tezos, Starknet, EOS)
symbols = ['arweave', 'tezos', 'starknet', 'eos']  # Lista de monedas a consultar

# Parámetros de la consulta
params = {
    'vs_currency': 'usd',  # Convertir el precio a USD
    'ids': ','.join(symbols),  # Convertir la lista de símbolos a una cadena separada por comas
    'order': 'market_cap_desc',  # Ordenar por capitalización de mercado
    'per_page': '10',  # Limitar a 10 resultados
    'page': '1',  # Página de resultados
}

# Realizar la solicitud GET a la API
try:
    response = requests.get(url, params=params)
    response.raise_for_status()  # Lanza una excepción si el código de estado es 4xx o 5xx
except requests.exceptions.RequestException as e:
    print(f"Error al obtener los datos: {e}")
    exit()

# Verificar que la solicitud fue exitosa
if response.status_code == 200:
    data = response.json()

    # Filtrar los datos que necesitamos y agregar el par de trading
    filtered_data = []
    for coin in data:
        # Agregar el par de trading (mayúsculas)
        trading_pair = f"{coin['symbol'].upper()}USDT"  # Crear el par de trading (ej. ARUSDT)
        
        # Extraer los datos requeridos y conservar solo los campos necesarios
        filtered_data.append({
            'name': coin['symbol'].upper(),  # Convertir el nombre a mayúsculas
            'symbol': trading_pair,  # Símbolo con USDT en mayúsculas
            'price': coin['current_price'],  # Precio en USD
            'timestamp': coin['last_updated']  # Fecha y hora de la última actualización
        })

    # Guardar los datos en un archivo JSON con el nuevo nombre
    output_dir = 'crypto_etl_project/data/coingecko'  # Directorio de salida
    os.makedirs(output_dir, exist_ok=True)  # Crear el directorio si no existe
    output_file = os.path.join(output_dir, 'gecko_new.json')  # Ruta del archivo de salida (cambiado a gecko_new.json)

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=4)
        print(f"Datos guardados correctamente en {output_file}")
    except Exception as e:
        print(f"Error al guardar los datos en el archivo: {e}")

else:
    print(f"Error al obtener los datos: {response.status_code}")