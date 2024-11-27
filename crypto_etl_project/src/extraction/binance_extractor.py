import os
import json
from dotenv import load_dotenv
from binance.client import Client
from datetime import datetime
import time

print("Iniciando extractor de datos de Binance...")

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener las claves de la API de las variables de entorno
API_KEY = os.getenv("BINANCE_API_KEY")
SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

# Conexión con la API de Binance
client = Client(API_KEY, SECRET_KEY)

# Función para obtener el precio actual de una criptomoneda
def get_crypto_price(symbol="BTCUSDT"):
    try:
        # Obtiene el precio de la criptomoneda (por ejemplo, BTC/USDT)
        ticker = client.get_symbol_ticker(symbol=symbol)
        return {
            "symbol": str(symbol),  # Convertimos el símbolo a string
            "price": str(ticker['price']),  # Convertimos el precio a string
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Timestamp como string
        }
    except Exception as e:
        print(f"Error al obtener los datos para {symbol}: {e}")
        return None

# Función para guardar los datos en un archivo JSON
def save_to_json(data, file_path):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Cargar los datos existentes
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        else:
            existing_data = []

        # Agregar los nuevos datos
        existing_data.append(data)

        # Guardar los datos en el archivo JSON
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=4)
        print(f"Datos guardados en {file_path}")
    except Exception as e:
        print(f"Error al guardar los datos en {file_path}: {e}")

# Función para realizar la extracción cada 1 minuto para múltiples criptomonedas
def fetch_and_save_data():
    symbols = ["ARUSDT", "XTZUSDT", "STRKUSDT", "EOSUSDT"]

    output_file = "crypto_etl_project/data/binance/binance_data.json"  # El archivo donde se guardarán los datos

    while True:
        for symbol in symbols:
            # Obtener los datos de la criptomoneda
            data = get_crypto_price(symbol)
            if data:
                print(f"Datos obtenidos: {data}")

                # Guardar los datos en el archivo JSON
                save_to_json(data, output_file)

        # Esperar hasta el próximo ciclo de consulta (1 minuto)
        print(f"Esperando 1 minuto para la siguiente consulta...")
        time.sleep(30)  # Esperar 1 minuto (60 segundos)

if __name__ == "__main__":
    fetch_and_save_data()
