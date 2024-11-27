import os
import json
import pandas as pd

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
    if not all(col in data.columns for col in ['name', 'symbol', 'price', 'timestamp']):
        print("Las columnas necesarias ('name', 'symbol', 'price', 'timestamp') no se encuentran en los datos.")
        return pd.DataFrame()

    # Convertir 'name' y 'symbol' a mayúsculas
    data['name'] = data['name'].astype(str).str.upper()
    data['symbol'] = data['symbol'].astype(str).str.upper() + 'USDT'

    # Convertir la columna 'price' a valores numéricos, forzando errores a NaN
    data['price'] = pd.to_numeric(data['price'], errors='coerce')

    # Eliminar filas con NaN en la columna 'price'
    data = data.dropna(subset=['price'])

    # Convertir 'timestamp' de string a timestamp Unix (si ya es un timestamp Unix, lo dejamos como está)
    data['timestamp'] = pd.to_datetime(data['timestamp'], errors='coerce')

    # Convertir el 'timestamp' a formato Unix (segundos desde el 1 de enero de 1970)
    data['timestamp'] = data['timestamp'].astype('int64') // 10**9

    # Eliminar filas con NaT en la columna 'timestamp' (en caso de error en la conversión)
    data = data.dropna(subset=['timestamp'])

    # Crear el formato final que necesitas (un diccionario con nombre, symbol, price y timestamp)
    transformed_data = data[['name', 'symbol', 'price', 'timestamp']].to_dict(orient='records')

    # Devolver los datos transformados
    print(f"Datos transformados: {transformed_data[:5]}")  # Mostrar solo los primeros 5 para verificar
    return transformed_data

# Función para guardar los datos procesados en un archivo JSON
def save_to_json(data, file_path):
    print(f"Guardando datos en {file_path}...")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Verificar si 'data' es una lista no vacía
    if data:
        try:
            # Sobrescribir los datos en el archivo JSON
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            print(f"Datos sobrescritos correctamente en {file_path}")
        except Exception as e:
            print(f"Error al guardar los datos: {e}")
    else:
        print("No hay datos para guardar.")

# Ejecución principal del proceso
def transform_and_save_data():
    file_path = "crypto_etl_project/data/coinmarketcap/coin_data.json"  # Archivo original donde se guardan los datos extraídos
    new_file_path = "crypto_etl_project/data/coinmarketcap/coin_data_transformed.json"  # Archivo donde se guardan los datos transformados

    # Cargar los datos
    data = load_data(file_path)
    
    # Si hay datos, realizar la transformación
    if not data.empty:
        transformed_data = transform_data(data)
        if transformed_data:
            save_to_json(transformed_data, new_file_path)
    else:
        print("No se encontraron datos para transformar.")

# Ejecutar el proceso de transformación
if __name__ == "__main__":
    transform_and_save_data()
