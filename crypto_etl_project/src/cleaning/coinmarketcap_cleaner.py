import os
import json
import pandas as pd

# Función para cargar los datos desde el archivo JSON
def load_data(file_path):
    print(f"Cargando datos desde {file_path}...")
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        with open(file_path, "r", encoding="utf-8") as f:
            return pd.DataFrame(json.load(f))
    else:
        print("Archivo vacío o no encontrado, devolviendo un DataFrame vacío.")
        return pd.DataFrame()

# Función para realizar el mapeo de los símbolos y eliminar columnas no deseadas
def transform_data(data):
    print("Iniciando la transformación de datos...")

    # Añadir 'USDT' a cada symbol para que se pueda usar como clave foránea
    data['symbol'] = data['symbol'] + 'USDT'

    # Seleccionar solo las columnas necesarias
    columns_to_keep = ['name', 'symbol', 'price', 'percent_change_1h', 'percent_change_24h', 'percent_change_7d', 'timestamp']
    data = data[columns_to_keep]

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

    # Sobrescribir los datos en el archivo JSON
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    print(f"Datos sobrescritos correctamente en {file_path}")

# Ejecución principal del proceso
def transform_and_save_data():
    file_path = "crypto_etl_project/data/coinmarketcap/coin_data.json"  # Archivo original donde se guardan los datos extraídos
    new_file_path = "crypto_etl_project/data/coinmarketcap/coin_data_transformed.json"  # Archivo donde se guardan los datos transformados

    # Cargar los datos y hacer la transformación
    data = load_data(file_path)
    if not data.empty:
        transformed_data = transform_data(data)
        save_to_json(transformed_data.to_dict(orient='records'), new_file_path)
    else:
        print("No se encontraron datos para transformar.")

# Ejecutar el proceso de transformación
if __name__ == "__main__":
    transform_and_save_data()
