import time
import threading
from crypto_etl_project.src.extraction.binance_extractor import fetch_and_save_data
from crypto_etl_project.src.cleaning.binance_cleaner import monitor_and_clean
from crypto_etl_project.src.loader.binance_loader import load_data_to_db


#Extractores
from crypto_etl_project.src.extraction.coinmarketcap_extractor import fetch_and_save_data_coinmarketcap
##CRYTOCOMPARE
from crypto_etl_project.src.extraction.coinlayer_extractor import main
from crypto_etl_project.src.extraction.geckocoin_extractor import fetch_and_save_data_coingecko

#Transformadores
from crypto_etl_project.src.cleaning.coinmarketcap_cleaner import transform_and_save_data
from crypto_etl_project.src.cleaning.coinlayer_cleaner import process_and_save_coinlayer_data
from crypto_etl_project.src.cleaning.crypto_cleaner import process_and_save_cryptocompare_data
from crypto_etl_project.src.cleaning.geckocoin_cleaner import process_and_save_geckocoin_data

#Loaders
from crypto_etl_project.src.loader.coinmarketcap_loader import load_data_to_db_coinmarketcap

#Dataset
from crypto_etl_project.src.loader.joinLoads_loader import load_and_combine_data

#Carga de BD
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
    transform_and_save_data(file_path, new_file_path)  # Ejecutar la transformación de CoinMarketCap una vez
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
    
    #GenerarDataSet
    print("Generación de data set")
    load_and_combine_data()
    print("Carga de datos")
    load_data_to_db_coinmarketcap2()
    

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
