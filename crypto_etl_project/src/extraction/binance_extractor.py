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

# Función para guardar los datos en un archivo JSON (sobrescribiendo el archivo)
def save_to_json(data, file_path):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Sobrescribir el archivo con todos los datos acumulados
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)  # Sobrescribir con la lista de datos acumulados
        print(f"Datos guardados en {file_path}")
    except Exception as e:
        print(f"Error al guardar los datos en {file_path}: {e}")

# Función para realizar la extracción cada 1 minuto para múltiples criptomonedas
def fetch_and_save_data():
    symbols = ["ARUSDT", "XTZUSDT", "STRKUSDT", "EOSUSDT", "DASHUSDT", "NEOUSDT", "STEEMUSDT", "ZENUSDT", "PEPEUSDT", "TRXUSDT", "XRPUSDT", "THEUSDT", "USDTUSDT", "DOGEUSDT"]

    output_file = "crypto_etl_project/data/binance/binance_data.json"  # El archivo donde se guardarán los datos

    # Crear una lista para almacenar los datos
    data_list = []

    while True:
        for symbol in symbols:
            # Obtener los datos de la criptomoneda
            data = get_crypto_price(symbol)
            if data:
                print(f"Datos obtenidos: {data}")
                
                # Agregar los datos a la lista
                data_list.append(data)

        # Guardar los datos de todas las criptomonedas en el archivo JSON (sobrescribir todo el archivo)
        if data_list:
            save_to_json(data_list, output_file)
        
        # Limpiar la lista de datos después de guardarlos
        data_list.clear()

        # Esperar hasta el próximo ciclo de consulta (1 minuto)
        print(f"Esperando 1 minuto para la siguiente consulta...")
        time.sleep(5)  # Esperar 5seg 

if __name__ == "__main__":
    fetch_and_save_data()
    
