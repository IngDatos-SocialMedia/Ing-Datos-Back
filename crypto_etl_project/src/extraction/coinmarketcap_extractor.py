import requests
import json
import os

# Función para obtener y guardar datos de CoinMarketCap
def fetch_and_save_data_coinmarketcap():
    # Definir la API Key directamente en la función
    api_key = "2ae2db9c-f876-4822-9566-f961a7c84f79"  # Tu clave de API de CoinMarketCap

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

    # Verificar si ya está en ejecución mediante un archivo de bloqueo
    lock_file = "coinmarketcap_lockfile.lock"

    if os.path.exists(lock_file):
        print("El script ya está en ejecución.")
        return
    else:
        # Crear un archivo de bloqueo
        with open(lock_file, 'w') as f:
            f.write("Lock file: Script en ejecución.")

    # Realizar la solicitud GET a la API
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Lanza una excepción si el código de estado es 4xx o 5xx
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener los datos: {e}")
        # Eliminar el archivo de bloqueo en caso de error
        os.remove(lock_file)
        return

    # Verificar que la solicitud fue exitosa
    if response.status_code == 200:
        data = response.json()

        # Filtrar solo los datos de las criptomonedas que queremos
        filtered_data = []
        for coin in data['data']:
            if coin['symbol'] in symbols:
                # Extraer los datos requeridos
                price = coin['quote']['EUR']['price']
                market_cap = coin['quote']['EUR']['market_cap']
                volume_24h = coin['quote']['EUR']['volume_24h']
                percent_change_1h = coin['quote']['EUR']['percent_change_1h']
                percent_change_24h = coin['quote']['EUR']['percent_change_24h']
                percent_change_7d = coin['quote']['EUR']['percent_change_7d']
                circulating_supply = coin['circulating_supply']
                max_supply = coin.get('max_supply', 'N/A')  # Si no existe, devolvemos 'N/A'
                cmc_rank = coin['cmc_rank']

                # Transformar la información al formato requerido para la base de datos
                filtered_data.append({
                    'name': coin['name'],  # Nombre de la criptomoneda
                    'symbol': coin['symbol'],  # Símbolo
                    'price': price,  # Precio en EUR
                    'timestamp': coin['last_updated'],  # Fecha y hora de la última actualización
                    # Puedes agregar más campos aquí si es necesario para la base de datos
                })

        # Guardar los datos en un archivo JSON
        output_dir = 'crypto_etl_project/data/coinmarketcap'
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'coin_data.json')
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(filtered_data, f, ensure_ascii=False, indent=4)
            print(f"Datos guardados correctamente en {output_file}")
        except Exception as e:
            print(f"Error al guardar los datos en el archivo: {e}")
        finally:
            # Eliminar el archivo de bloqueo después de guardar los datos
            os.remove(lock_file)

    else:
        print(f"Error al obtener los datos: {response.status_code}")
        # Eliminar el archivo de bloqueo si ocurre un error
        os.remove(lock_file)

# Llamar a la función
if __name__ == "__main__":
    fetch_and_save_data_coinmarketcap()
