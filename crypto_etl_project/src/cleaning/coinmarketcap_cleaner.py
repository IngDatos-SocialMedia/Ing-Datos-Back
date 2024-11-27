import os
import json
import pandas as pd
import time

# Función para cargar los datos desde el archivo JSON
def load_data(file_path):
    print(f"Cargando datos desde {file_path}...")
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return pd.DataFrame(json.load(f))
        except Exception as e:
            print(f"Error al cargar el archivo: {e}")
            return pd.DataFrame()  # En caso de error, devolver un DataFrame vacío
    else:
        print("Archivo vacío o no encontrado, devolviendo un DataFrame vacío.")
        return pd.DataFrame()

# Función para realizar el mapeo de los símbolos y eliminar columnas no deseadas
def transform_data(data):
    print("Iniciando la transformación de datos...")
    
    # Verificar si las columnas necesarias están presentes
    if 'symbol' not in data.columns or 'price' not in data.columns:
        print("Las columnas 'symbol' o 'price' no se encuentran en los datos.")
        return pd.DataFrame()

    # Añadir 'USDT' a cada symbol para que se pueda usar como clave foránea
    data['symbol'] = data['symbol'].astype(str) + 'USDT'

    # Seleccionar solo las columnas necesarias
    columns_to_keep = ['name', 'symbol', 'price', 'timestamp']
    data = data[columns_to_keep] if all(col in data.columns for col in columns_to_keep) else pd.DataFrame()

    # Convertir la columna 'price' a valores numéricos, forzando errores a NaN
    data['price'] = pd.to_numeric(data['price'], errors='coerce')

    # Eliminar filas con NaN en la columna 'price'
    data = data.dropna(subset=['price'])

    # Devolver los datos transformados
    print(f"Datos transformados: {data.head()}")
    return data

# Función para guardar los datos procesados en un archivo JSON
def save_to_json(data, file_path):
    print(f"Guardando datos en {file_path}...")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    try:
        # Sobrescribir los datos en el archivo JSON
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Datos sobrescritos correctamente en {file_path}")
    except Exception as e:
        print(f"Error al guardar los datos: {e}")

# Función para monitorear el archivo y procesar los datos periódicamente
def monitor_and_clean_coinmarketcap(file_path, new_file_path, interval=30):
    """
    Monitorea el archivo de datos y lo transforma periódicamente.
    El intervalo está en segundos (por defecto, cada 30 segundos).
    """
    print(f"Monitoreando el archivo {file_path} cada {interval} segundos...")

    # Verificar si el script ya está en ejecución
    lock_file = "coinmarketcap_lockfile.lock"
    if os.path.exists(lock_file):
        print("El script ya está en ejecución.")
        return
    else:
        # Crear un archivo de bloqueo
        with open(lock_file, 'w') as f:
            f.write("Lock file: Script en ejecución.")

    try:
        while True:
            # Cargar los datos
            data = load_data(file_path)
            
            # Si hay datos, realizar la transformación
            if not data.empty:
                transformed_data = transform_data(data)
                if not transformed_data.empty:
                    save_to_json(transformed_data.to_dict(orient='records'), new_file_path)
            else:
                print("No se encontraron datos para transformar.")

            # Esperar el intervalo antes de la siguiente transformación
            time.sleep(interval)

    finally:
        # Eliminar el archivo de bloqueo después de la ejecución
        os.remove(lock_file)

# Ejecución principal del proceso
def transform_and_save_data():
    file_path = "crypto_etl_project/data/coinmarketcap/coin_data.json"  # Archivo original donde se guardan los datos extraídos
    new_file_path = "crypto_etl_project/data/coinmarketcap/coin_data_transformed.json"  # Archivo donde se guardan los datos transformados

    # Iniciar la monitorización y transformación periódica
    monitor_and_clean_coinmarketcap(file_path, new_file_path, interval=30)

# Ejecutar el proceso de transformación
if __name__ == "__main__":
    transform_and_save_data()
