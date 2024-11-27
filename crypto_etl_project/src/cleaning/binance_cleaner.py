import os
import json
import time
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from sklearn.impute import KNNImputer

print("Iniciando transformación de datos de Binance...")

# Función para cargar los datos existentes desde el archivo JSON
def load_data(file_path):
    print(f"Cargando datos desde {file_path}...")
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        with open(file_path, "r", encoding="utf-8") as f:
            return pd.DataFrame(json.load(f))
    else:
        print("Archivo vacío o no encontrado, devolviendo un DataFrame vacío.")
        return pd.DataFrame()

# Función para limpiar los datos (eliminar NaN, normalización, etc.)
def clean_data(data):
    print("Iniciando la limpieza de datos...")

    # Convertir la columna 'price' a valores numéricos, forzando errores a NaN
    data['price'] = pd.to_numeric(data['price'], errors='coerce')

    # Usar KNN Imputer para rellenar valores nulos en 'price'
    imputer = KNNImputer(n_neighbors=3)  # Usar los 3 vecinos más cercanos para la imputación
    data['price'] = imputer.fit_transform(data[['price']])

    # Eliminar filas con NaN en la columna 'price' si no se puede imputar
    data = data.dropna(subset=['price'])

    # Normalización de los precios (si es necesario)
    scaler = StandardScaler()
    data['price'] = scaler.fit_transform(data[['price']])

    # Conversión de timestamps a fechas legibles en formato string
    # Si el 'timestamp' está vacío o es nulo, reemplazarlo por la fecha y hora actual
    data['timestamp'] = data['timestamp'].apply(
        lambda x: datetime.now().strftime('%Y-%m-%d %H:%M:%S') if pd.isnull(x) else x
    )

    print("Datos limpiados y transformados correctamente.")
    return data

# Función para guardar los datos procesados en un archivo JSON (sobrescribiendo el archivo)
def save_to_json(data, file_path):
    print(f"Guardando datos en {file_path}...")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Sobrescribir el archivo con los nuevos datos
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    print(f"Datos guardados correctamente en {file_path}")

# Función para monitorear el archivo JSON y limpiar los nuevos datos
def monitor_and_clean(file_path, new_file_path, interval=30):
    print(f"Comenzando a monitorear el archivo de datos cada {interval} segundos...")
    count = 0
    while count < interval:
        print(f"Esperando {interval} segundos antes de la próxima verificación... {count + 1}/{interval}")

        # Cargar los datos existentes
        data = load_data(file_path)

        # Si hay datos, limpiarlos y procesarlos
        if not data.empty:
            print("Datos encontrados, limpiando y procesando...")
            cleaned_data = clean_data(data)

            # Guardar los datos limpiados en un archivo nuevo sin sobrescribir todo
            save_to_json(cleaned_data.to_dict(orient='records'), new_file_path)
            print(f"Datos limpiados y guardados en {new_file_path}")
        
        # Si no se encuentran nuevos datos, espera el próximo ciclo
        else:
            print("No se encontraron nuevos datos. Esperando 30 segundos...")

        # Incrementar el contador
        count += 1
        time.sleep(interval)  # Espera 'interval' segundos antes de la siguiente verificación

# Ejecución principal del proceso
if __name__ == "__main__":
    file_path = "crypto_etl_project/data/binance/binance_data.json"  # Archivo original donde se guardan los datos extraídos
    new_file_path = "crypto_etl_project/data/binance/binance_data_new.json"  # Archivo donde se guardan los datos limpiados y procesados

    # Monitorear el archivo y limpiar los datos
    monitor_and_clean(file_path, new_file_path, interval=30)
