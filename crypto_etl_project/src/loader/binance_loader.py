import os
import json
import psycopg2
import time

# Función para cargar datos desde un archivo JSON
def load_data(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data
    return []

# Función para conectar con PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname="crypto_db",       # Nombre de la base de datos
            user="postgres",          # Nombre de usuario de PostgreSQL
            password="185",           # Contraseña de PostgreSQL
            host="localhost",         # En el caso de que esté en tu máquina local
            port="5432"               # Puerto por defecto de PostgreSQL
        )
        return conn
    except Exception as e:
        print(f"Error de conexión: {e}")
        return None

# Función para crear la tabla si no existe
def create_table_if_not_exists(cursor):
    cursor.execute(''' 
        CREATE TABLE IF NOT EXISTS crypto_data (
            symbol TEXT,
            price REAL,
            timestamp TEXT,
            PRIMARY KEY (symbol, timestamp)
        )
    ''')

# Función para eliminar los datos antiguos de la tabla
def delete_old_data(cursor):
    cursor.execute('DELETE FROM public.crypto_data;')

# Función para insertar o actualizar los datos en PostgreSQL
def update_data_to_db(data):
    conn = connect_to_db()
    if conn:
        try:
            cursor = conn.cursor()

            # Crear la tabla si no existe
            create_table_if_not_exists(cursor)

            # Eliminar los datos antiguos
            delete_old_data(cursor)

            # Insertar los nuevos datos
            for entry in data:
                cursor.execute(''' 
                    INSERT INTO crypto_data (symbol, price, timestamp)
                    VALUES (%s, %s, %s)
                ''', (entry['symbol'], entry['price'], entry['timestamp']))

            # Hacer commit de las transacciones
            conn.commit()

        except Exception as e:
            print(f"Error al actualizar los datos: {e}")
        finally:
            cursor.close()
            conn.close()

# Función para monitorear el archivo y cargar los datos actualizados
def monitor_and_update_load(file_path):
    last_mod_time = os.path.getmtime(file_path)  # Guardar el tiempo de la última modificación
    while True:
        current_mod_time = os.path.getmtime(file_path)
        if current_mod_time != last_mod_time:
            data = load_data(file_path)
            if data:
                update_data_to_db(data)
                print("Se ha detectado un cambio, actualización en la base de datos.")
                last_mod_time = current_mod_time  # Actualizar el tiempo de la última modificación
        time.sleep(1)  # Esperar 1 segundo antes de comprobar nuevamente el archivo

# Ejecutar la función de monitoreo
if __name__ == "__main__":
    file_path = "crypto_etl_project/data/binance/binance_data_new.json"  # Archivo JSON con los datos actualizados
    monitor_and_update_load(file_path)
