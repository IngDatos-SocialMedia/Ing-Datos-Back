import requests
import json
import os

# Criptomonedas que quieres consultar
symbols = ["STEEM", "ZEN", "NEO", "DASH"]

# URL de la API de CoinLayer
base_url = "http://api.coinlayer.com/live"

# Tu API Key de CoinLayer
api_key = "99f98258c61114adda01e562a7f3d61f"

# Parámetros de la consulta
symbols_param = ",".join(symbols)  # Criptomonedas a consultar
params = {
    'access_key': api_key,
    'symbols': symbols_param,  # Criptomonedas
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

    # Guardar los datos crudos en un archivo JSON
    output_dir = 'crypto_etl_project/data/coinlayer'
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'coinlayer_data.json')

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Datos crudos guardados correctamente en {output_file}")
    except Exception as e:
        print(f"Error al guardar los datos crudos en el archivo: {e}")

else:
    print(f"Error al obtener los datos: {response.status_code}")
