import os
import json
import time
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from sklearn.impute import KNNImputer
from sklearn.linear_model import SGDRegressor
import joblib

print("Iniciando transformación de datos de Binance...")

# Inicializar el modelo de regresión (modelo incremental)
model = SGDRegressor()

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

# Función para acumular los datos de entrenamiento en el archivo JSON
def accumulate_data_for_ml(new_data, ml_file_path):
    # Verificar si el archivo de datos ML existe y si está vacío
    if os.path.exists(ml_file_path):
        try:
            # Si el archivo no está vacío, cargar los datos históricos
            with open(ml_file_path, "r", encoding="utf-8") as f:
                historical_data = json.load(f)
        except json.JSONDecodeError:
            # Si hay un error en el archivo (vacío o corrupto), inicializar los datos históricos como una lista vacía
            print(f"El archivo {ml_file_path} está vacío o tiene un formato incorrecto. Inicializando datos.")
            historical_data = []
    else:
        # Si el archivo no existe, inicializarlo con una lista vacía
        print(f"El archivo {ml_file_path} no existe. Creando un nuevo archivo.")
        historical_data = []

    # Acumular los nuevos datos al conjunto histórico
    historical_data.extend(new_data)

    # Guardar los datos acumulados nuevamente en el archivo JSON
    with open(ml_file_path, "w", encoding="utf-8") as f:
        json.dump(historical_data, f, ensure_ascii=False, indent=4)

    print(f"Datos acumulados y guardados en {ml_file_path}")


# Función para actualizar el modelo de manera incremental
def update_model(data):
    global model
    # Características (en este caso, solo el precio)
    X_new = data[['price']]
    # Valor objetivo (puede ser cualquier otra variable, en este caso tomamos el precio como objetivo)
    y_new = data['price']

    # Entrenamiento incremental del modelo
    model.partial_fit(X_new, y_new)
    print("Modelo actualizado con nuevos datos.")

    # Guardar el modelo actualizado en disco
    joblib.dump(model, 'incremental_model.pkl')
    print("Modelo guardado en disco.")

# Función para monitorear el archivo JSON y limpiar los nuevos datos
def monitor_and_clean(file_path, new_file_path, ml_file_path, interval=1):
    print(f"Comenzando a monitorear el archivo de datos...")

    # Obtener el tiempo de la última modificación del archivo
    last_modified_time = os.path.getmtime(file_path)

    while True:
        # Verificar si el archivo ha sido modificado
        current_modified_time = os.path.getmtime(file_path)

        if current_modified_time != last_modified_time:
            print("¡Archivo modificado! Comenzando el proceso de limpieza...")

            # Cargar los datos existentes
            data = load_data(file_path)

            # Si hay datos, limpiarlos y procesarlos
            if not data.empty:
                print("Datos encontrados, limpiando y procesando...")

                # Limpiar los datos
                cleaned_data = clean_data(data)

                # Actualizar el modelo con los datos procesados
                update_model(cleaned_data)

                # Guardar los datos limpiados en un archivo nuevo para visualización
                save_to_json(cleaned_data.to_dict(orient='records'), new_file_path)
                print(f"Datos limpiados y guardados en {new_file_path}")

                # Acumular los datos para el modelo ML (acumulando en el archivo de datos históricos)
                accumulate_data_for_ml(cleaned_data.to_dict(orient='records'), ml_file_path)

            # Actualizar el tiempo de la última modificación
            last_modified_time = current_modified_time
        
        # Esperar un poco antes de verificar nuevamente
        time.sleep(interval)  # Espera 1 segundo antes de la siguiente verificación

# Ejecución principal del proceso
if __name__ == "__main__":
    file_path = "crypto_etl_project/data/binance/binance_data.json"  # Archivo original donde se guardan los datos extraídos
    new_file_path = "crypto_etl_project/data/binance/binance_data_new.json"  # Archivo donde se guardan los datos limpiados y procesados
    ml_file_path = "crypto_etl_project/data/binance/binance_data_new_ML.json"  # Archivo donde se acumulan los datos para entrenamiento

    # Monitorear el archivo y limpiar los datos
    monitor_and_clean(file_path, new_file_path, ml_file_path)
