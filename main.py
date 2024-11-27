import time
import threading
from crypto_etl_project.src.extraction.binance_extractor import fetch_and_save_data
from crypto_etl_project.src.cleaning.binance_cleaner import monitor_and_clean
from crypto_etl_project.src.loader.binance_loader import load_data_to_db
from crypto_etl_project.src.extraction.coinmarketcap_extractor import fetch_and_save_data_coinmarketcap
from crypto_etl_project.src.cleaning.coinmarketcap_cleaner import monitor_and_clean_coinmarketcap
from crypto_etl_project.src.loader.coinmarketcap_loader import load_data_to_db_coinmarketcap
from crypto_etl_project.src.loader.coin_load import load_data_to_db_coinmarketcap2

# Flag global para controlar la ejecución
running = True
file_path2 = "crypto_etl_project/data/coinmarketcap/coin_data_transformed.json"  # Archivo JSON con los datos transformados

# Función para la extracción de datos de Binance
def extract_data_binance():
    global running
    print("Iniciando el proceso de extracción de datos de Binance...")
    while running:
        fetch_and_save_data()
        time.sleep(60)  # Espera 1 minuto antes de la siguiente extracción

# Función para la transformación de los datos de Binance
def transform_data_binance():
    global running
    print("Iniciando el proceso de limpieza de datos de Binance...")
    file_path = "crypto_etl_project/data/binance/binance_data.json"  # El archivo donde se guardan los datos extraídos
    new_file_path = "crypto_etl_project/data/binance/binance_data_new.json"  # El archivo donde se guardan los datos limpiados
    while running:
        monitor_and_clean(file_path, new_file_path, interval=30)
        time.sleep(30)  # Espera 30 segundos antes de la siguiente transformación

# Función para la carga de datos en la base de datos de Binance
def load_data_binance():
    global running
    print("Iniciando la carga de datos a la base de datos de Binance...")
    new_file_path = "crypto_etl_project/data/binance/binance_data_new.json"  # El archivo con los datos limpiados
    while running:
        load_data_to_db(new_file_path)
        time.sleep(60)  # Espera 1 minuto antes de la siguiente carga

# Función para la extracción de datos de CoinMarketCap (solo una vez al inicio)
def extract_data_coinmarketcap():
    print("Iniciando el proceso de extracción de datos de CoinMarketCap...")
    fetch_and_save_data_coinmarketcap()  # Ejecutar el extractor de CoinMarketCap una vez
    print("Extracción de CoinMarketCap completada.")

# Función para la transformación de los datos de CoinMarketCap (solo una vez al inicio)
def transform_data_coinmarketcap():
    print("Iniciando el proceso de limpieza de datos de CoinMarketCap...")
    file_path = "crypto_etl_project/data/coinmarketcap/coin_data.json"  # El archivo donde se guardan los datos extraídos
    new_file_path = "crypto_etl_project/data/coinmarketcap/coin_data_transformed.json"  # El archivo donde se guardan los datos transformados
    monitor_and_clean_coinmarketcap(file_path, new_file_path)  # Ejecutar la transformación de CoinMarketCap una vez
    print("Transformación de CoinMarketCap completada.")

# Función para la carga de datos en la base de datos de CoinMarketCap (solo una vez al inicio)
def load_data_coinmarketcap():
    print("Iniciando la carga de datos a la base de datos de CoinMarketCap...")
    new_file_path = "crypto_etl_project/data/coinmarketcap/coin_data_transformed.json"  # El archivo con los datos transformados
    load_data_to_db_coinmarketcap(new_file_path)  # Ejecutar la carga de CoinMarketCap una vez
    print("Carga de CoinMarketCap completada.")

# Función principal para orquestar el flujo ETL con hilos
def main():
    global running
    load_data_to_db_coinmarketcap2(file_path2)

    # Esperar 10 segundos al iniciar el programa
    print("Esperando 5 segundos antes de comenzar el proceso...")

    # Ejecutar las funciones de CoinMarketCap solo una vez
    extract_data_coinmarketcap()
    transform_data_coinmarketcap()
    time.sleep(5)
    print("Carga de 2 completada.")

    time.sleep(5)

    # Crear hilos para las tareas de Binance
    extract_thread_binance = threading.Thread(target=extract_data_binance)
    transform_thread_binance = threading.Thread(target=transform_data_binance)
    load_thread_binance = threading.Thread(target=load_data_binance)
    

    # Iniciar hilos
    extract_thread_binance.start()
    transform_thread_binance.start()
    load_thread_binance.start()
    

    # Esperar a que todos los hilos de Binance terminen antes de continuar
    extract_thread_binance.join()
    transform_thread_binance.join()
    load_thread_binance.join()

    print("Todos los hilos de Binance han terminado.")

# Captura la señal de interrupción y detén los hilos
if __name__ == "__main__":
    try:
        while running:
            main()
    except KeyboardInterrupt:
        running = False
        print("\nProceso interrumpido por el usuario (Ctrl+C). Deteniendo el flujo ETL.")
        time.sleep(1)  # Dar un tiempo para que los hilos terminen
        print("Flujo ETL detenido.")
