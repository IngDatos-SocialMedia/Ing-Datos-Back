import requests
import json
import os

def fetch_and_save_data_coingecko():
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
        return

    # Verificar que la solicitud fue exitosa
    if response.status_code == 200:
        data = response.json()

        # Filtrar los datos que necesitamos
        filtered_data = []
        for coin in data:
            # Extraer los datos requeridos
            filtered_data.append({
                'name': coin['name'],  # Nombre de la criptomoneda
                'symbol': coin['symbol'],  # Símbolo
                'price': coin['current_price'],  # Precio en USD
                'timestamp': coin['last_updated'],  # Fecha y hora de la última actualización
            })

        # Guardar los datos en un archivo JSON
        output_dir = 'crypto_etl_project/data/coingecko'  # Directorio de salida
        os.makedirs(output_dir, exist_ok=True)  # Crear el directorio si no existe
        output_file = os.path.join(output_dir, 'gecko_data.json')  # Ruta del archivo de salida

        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(filtered_data, f, ensure_ascii=False, indent=4)
            print(f"Datos guardados correctamente en {output_file}")
        except Exception as e:
            print(f"Error al guardar los datos en el archivo: {e}")

    else:
        print(f"Error al obtener los datos: {response.status_code}")

# Llamar a la función
if __name__ == "__main__":
    fetch_and_save_data_coingecko()
