import os
import json
import psycopg2  # Para conectar con PostgreSQL

print("Iniciando subida de datos de Binance...")

# Función para cargar datos desde un archivo JSON
def load_data(file_path):
    print(f"Intentando cargar los datos desde {file_path}...")
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"Datos cargados: {len(data)} registros encontrados.")
            return data
    else:
        print(f"El archivo {file_path} no existe.")
        return []

# Función para conectar con PostgreSQL
def connect_to_db():
    print("Intentando conectar a la base de datos PostgreSQL...")
    try:
        conn = psycopg2.connect(
            dbname="crypto_db",       # Nombre de la base de datos
            user="postgres",          # Nombre de usuario de PostgreSQL
            password="185",           # Contraseña de PostgreSQL
            host="localhost",         # En el caso de que esté en tu máquina local
            port="5432"               # Puerto por defecto de PostgreSQL
        )
        print("Conexión exitosa a la base de datos.")
        return conn
    except Exception as e:
        print(f"Error al conectar con la base de datos: {e}")
        return None

# Función para crear la tabla si no existe
def create_table_if_not_exists(cursor):
    print("Verificando si la tabla crypto_data existe...")
    cursor.execute(''' 
        CREATE TABLE IF NOT EXISTS crypto_data (
            symbol TEXT,
            price REAL,
            timestamp TEXT
        )
    ''')
    print("Tabla verificada o creada si no existía.")

# Función para insertar los datos en PostgreSQL
def insert_data_to_db(data):
    print("Iniciando la inserción de datos en la base de datos...")
    conn = connect_to_db()
    if conn:
        try:
            cursor = conn.cursor()

            # Crear la tabla si no existe
            create_table_if_not_exists(cursor)

            # Insertar los datos
            for entry in data:
                print(f"Procesando {entry['symbol']} con timestamp {entry['timestamp']}...")
                
                # Verificar si el registro ya existe en la base de datos
                cursor.execute('''SELECT 1 FROM crypto_data WHERE symbol = %s AND timestamp = %s LIMIT 1;''', (entry['symbol'], entry['timestamp']))
                if cursor.fetchone() is None:  # Si no existe, insertamos los nuevos datos
                    cursor.execute('''
                        INSERT INTO crypto_data (symbol, price, timestamp)
                        VALUES (%s, %s, %s)
                    ''', (entry['symbol'], entry['price'], entry['timestamp']))
                    print(f"Datos insertados: {entry['symbol']} a {entry['timestamp']}")
                else:
                    print(f"Datos duplicados encontrados para {entry['symbol']} a {entry['timestamp']}")

            # Hacer commit de las transacciones
            conn.commit()
            print("Datos cargados a la base de datos PostgreSQL.")

        except Exception as e:
            print(f"Error al insertar los datos: {e}")
        finally:
            cursor.close()
            conn.close()  # Asegura el cierre de la conexión

# Función principal para cargar los datos
def load_data_to_db(file_path):
    print("Iniciando el proceso de carga de datos...")
    data = load_data(file_path)
    if data:
        insert_data_to_db(data)
    else:
        print("No hay datos para cargar.")

# Ejecutar función principal
if __name__ == "__main__":
    file_path = "crypto_etl_project/data/binance/binance_data.json"  # Archivo JSON con los datos
    load_data_to_db(file_path)
