import os
import json
import psycopg2
import time
import pandas as pd

# Función para cargar datos desde un archivo JSON
def load_data(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data
    return []

# Función para conectarse a la base de datos PostgreSQL
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

# Función para crear la tabla `coste` si no existe
def create_coste_table(cursor):
    cursor.execute(''' 
        CREATE TABLE IF NOT EXISTS coste (
            symbol TEXT,
            variacion REAL,
            base REAL,
            name TEXT,
            precio REAL,
            hora_variacion TEXT,
            hora_base TEXT,
            PRIMARY KEY (symbol, hora_variacion)
        )
    ''')

# Función para insertar los datos en la tabla `coste` solo si los `symbol` coinciden
def insert_into_coste(cursor):
    # Consultar los datos de `crypto_data`
    cursor.execute('SELECT symbol, price AS variacion, timestamp AS hora_variacion FROM crypto_data;')
    crypto_data = cursor.fetchall()

    # Consultar los datos de `crypto_prices`
    cursor.execute('SELECT symbol, price AS base, name, timestamp AS hora_base FROM crypto_prices;')
    crypto_prices = cursor.fetchall()

    # Iterar sobre cada `symbol` en `crypto_data`
    for c_data in crypto_data:
        for p_data in crypto_prices:
            # Verificar si los `symbol` coinciden
            if c_data[0] == p_data[0]:
                # Calcular el precio (diferencia entre base y variación)
                precio = p_data[1] - c_data[1]

                # Insertar los datos en la tabla `coste`
                cursor.execute('''
                    INSERT INTO coste (symbol, variacion, base, name, precio, hora_variacion, hora_base)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, hora_variacion)
                    DO UPDATE SET 
                        variacion = EXCLUDED.variacion,
                        base = EXCLUDED.base,
                        name = EXCLUDED.name,
                        precio = EXCLUDED.precio,
                        hora_base = EXCLUDED.hora_base;
                ''', (c_data[0], c_data[1], p_data[1], p_data[2], precio, c_data[2], p_data[3]))

    # Exportar los datos de la tabla coste a un archivo JSON y CSV
    export_data_to_file()

# Función para exportar los datos de la tabla `coste` a un archivo JSON y CSV
def export_data_to_file():
    conn = connect_to_db()
    if conn:
        try:
            # Consultar los datos de la tabla `coste`
            query = "SELECT * FROM coste;"
            df = pd.read_sql(query, conn)  # Usamos pandas para leer directamente desde SQL
            
            # Guardar los datos en un archivo JSON
            json_file_path = r"C:\Users\dylan\Desktop\Bit\Proyecto\dataset\dataset.json"
            df.to_json(json_file_path, orient="records", lines=True)
            print(f"Datos exportados correctamente a {json_file_path}")
            
            # Alternativamente, guardar como CSV
            csv_file_path = r"C:\Users\dylan\Desktop\Bit\Proyecto\dataset\dataset.csv"
            df.to_csv(csv_file_path, index=False)
            print(f"Datos exportados correctamente a {csv_file_path}")
        except Exception as e:
            print(f"Error al exportar los datos: {e}")
        finally:
            conn.close()

# Función para actualizar los datos en la tabla `crypto_data`
def update_data_to_db(data):
    conn = connect_to_db()
    if conn:
        try:
            cursor = conn.cursor()
            # Crear la tabla `crypto_data` si no existe
            cursor.execute(''' 
                CREATE TABLE IF NOT EXISTS crypto_data (
                    symbol TEXT,
                    price REAL,
                    timestamp TEXT,
                    PRIMARY KEY (symbol, timestamp)
                );
            ''')

            # Eliminar los datos antiguos de la tabla `crypto_data`
            cursor.execute('DELETE FROM public.crypto_data;')

            # Insertar los nuevos datos en `crypto_data`
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
            # Cargar los datos del archivo y actualizar la tabla `crypto_data`
            data = load_data(file_path)
            if data:
                update_data_to_db(data)
                print("Se ha detectado un cambio, actualización en la base de datos.")
            
            # Conectarse nuevamente a la base de datos y crear la tabla `coste` y actualizarla
            conn = connect_to_db()
            if conn:
                try:
                    cursor = conn.cursor()
                    # Crear la tabla `coste` si no existe
                    create_coste_table(cursor)
                    # Insertar los datos en la tabla `coste`
                    insert_into_coste(cursor)
                    # Confirmar cambios
                    conn.commit()
                except Exception as e:
                    print(f"Error al actualizar la tabla coste: {e}")
                finally:
                    cursor.close()
                    conn.close()

            # Actualizar el tiempo de la última modificación
            last_mod_time = current_mod_time

        time.sleep(1)  # Esperar 1 segundo antes de comprobar nuevamente el archivo

# Ejecutar la función de monitoreo
if __name__ == "__main__":
    file_path = "crypto_etl_project/data/binance/binance_data_new.json"  # Archivo JSON con los datos actualizados
    monitor_and_update_load(file_path)
