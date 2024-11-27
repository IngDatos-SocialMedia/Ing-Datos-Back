import time
import threading
from crypto_etl_project.src.extraction.binance_extractor import fetch_and_save_data  # Importa la función del extractor
from crypto_etl_project.src.cleaning.binance_cleaner import monitor_and_clean       # Importa la función de limpieza
from crypto_etl_project.src.loader.binance_loader import load_data_to_db            # Importa la función de carga a DB

# Flag global para controlar la ejecución
running = True

# Función para la extracción de datos
def extract_data():
    global running
    print("Iniciando el proceso de extracción de datos de Binance...")
    while running:
        fetch_and_save_data()
        time.sleep(60)  # Espera 1 minuto antes de la siguiente extracción

# Función para la transformación de los datos
def transform_data():
    global running
    print("Iniciando el proceso de limpieza de datos...")
    file_path = "crypto_etl_project/data/binance/binance_data.json"  # El archivo donde se guardan los datos extraídos
    new_file_path = "crypto_etl_project/data/binance/binance_data_new.json"  # El archivo donde se guardan los datos limpiados
    while running:
        monitor_and_clean(file_path, new_file_path, interval=30)
        time.sleep(30)  # Espera 30 segundos antes de la siguiente transformación

# Función para la carga de datos en la base de datos
def load_data():
    global running
    print("Iniciando la carga de datos a la base de datos...")
    new_file_path = "crypto_etl_project/data/binance/binance_data_new.json"  # El archivo con los datos limpiados
    while running:
        load_data_to_db(new_file_path)
        time.sleep(60)  # Espera 1 minuto antes de la siguiente carga

# Función principal para orquestar el flujo ETL con hilos
def main():
    global running

    # Crear hilos para cada tarea
    extract_thread = threading.Thread(target=extract_data)
    transform_thread = threading.Thread(target=transform_data)
    load_thread = threading.Thread(target=load_data)

    # Iniciar los hilos
    extract_thread.start()
    transform_thread.start()
    load_thread.start()

    # Esperar a que todos los hilos terminen antes de continuar
    extract_thread.join()
    transform_thread.join()
    load_thread.join()

# Captura la señal de interrupción y detén los hilos
if __name__ == "__main__":
    try:
        while running:
            main()
    except KeyboardInterrupt:
        running = False
        print("\nProceso interrumpido por el usuario (Ctrl+C). Deteniendo el flujo ETL.")
        time.sleep(1)  # Dar un tiempo para que los hilos terminen adecuadamente
