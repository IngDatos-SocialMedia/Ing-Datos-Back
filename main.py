import time
import threading
from crypto_etl_project.src.extraction.binance_extractor import fetch_and_save_data  # Importa la función del extractor Binance
from crypto_etl_project.src.cleaning.binance_cleaner import monitor_and_clean       # Importa la función de limpieza Binance
from crypto_etl_project.src.loader.binance_loader import load_data_to_db            # Importa la función de carga a DB Binance

from crypto_etl_project.src.extraction.coinmarketcap_extractor import fetch_and_save_data_coinmarketcap  # Importa el extractor CoinMarketCap
from crypto_etl_project.src.cleaning.coinmarketcap_cleaner import monitor_and_clean_coinmarketcap        # Importa la función de limpieza CoinMarketCap
from crypto_etl_project.src.loader.coinmarketcap_loader import load_data_to_db_coinmarketcap               # Importa la función de carga a DB CoinMarketCap

# Flag global para controlar la ejecución
running = True

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

# Función para la extracción de datos de CoinMarketCap (solo una vez cada 2 horas)
def extract_data_coinmarketcap():
    print("Iniciando el proceso de extracción de datos de CoinMarketCap...")
    fetch_and_save_data_coinmarketcap()  # Ejecutar el extractor de CoinMarketCap una vez
    print("Extracción de CoinMarketCap completada.")

# Función para la transformación de los datos de CoinMarketCap (solo una vez cada 2 horas)
def transform_data_coinmarketcap():
    print("Iniciando el proceso de limpieza de datos de CoinMarketCap...")
    file_path = "crypto_etl_project/data/coinmarketcap/coin_data.json"  # El archivo donde se guardan los datos extraídos
    new_file_path = "crypto_etl_project/data/coinmarketcap/coin_data_transformed.json"  # El archivo donde se guardan los datos transformados
    monitor_and_clean_coinmarketcap(file_path, new_file_path)  # Ejecutar la transformación de CoinMarketCap una vez
    print("Transformación de CoinMarketCap completada.")

# Función para la carga de datos en la base de datos de CoinMarketCap (solo una vez cada 2 horas)
def load_data_coinmarketcap():
    print("Iniciando la carga de datos a la base de datos de CoinMarketCap...")
    new_file_path = "crypto_etl_project/data/coinmarketcap/coin_data_transformed.json"  # El archivo con los datos transformados
    load_data_to_db_coinmarketcap(new_file_path)  # Ejecutar la carga de CoinMarketCap una vez
    print("Carga de CoinMarketCap completada.")

# Función principal para orquestar el flujo ETL con hilos
def main():
    global running

    # Crear hilos para cada tarea
    extract_thread_binance = threading.Thread(target=extract_data_binance)
    transform_thread_binance = threading.Thread(target=transform_data_binance)
    load_thread_binance = threading.Thread(target=load_data_binance)

    extract_thread_binance.start()
    transform_thread_binance.start()
    load_thread_binance.start()

    # Ejecutar la tarea de CoinMarketCap cada 2 horas
    while running:
        # Ejecutar una vez cada 2 horas
        extract_data_coinmarketcap()
        transform_data_coinmarketcap()
        load_data_coinmarketcap()
        time.sleep(7200)  # Espera 2 horas (7200 segundos)

    # Esperar a que todos los hilos de Binance terminen antes de continuar
    extract_thread_binance.join()
    transform_thread_binance.join()
    load_thread_binance.join()

# Captura la señal de interrupción y detén los hilos
if __name__ == "__main__":
    try:
        while running:
            main()
    except KeyboardInterrupt:
        running = False
        print("\nProceso interrumpido por el usuario (Ctrl+C). Deteniendo el flujo ETL.")
        time.sleep(1)  # Dar un tiempo para que los hilos terminen adecuadamente
