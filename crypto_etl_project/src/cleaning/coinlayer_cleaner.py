import json
import os

# Leer los datos crudos desde el archivo
input_file = 'crypto_etl_project/data/coinlayer/coinlayer_data.json'

try:
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
except Exception as e:
    print(f"Error al leer los datos desde el archivo: {e}")
    exit()

# Filtrar los datos relevantes: name, symbol, price y timestamp
filtered_data = []

# Asegurarnos de que la clave "rates" existe y procesamos los datos
if "rates" in data:
    # Obtener el timestamp
    timestamp = data.get('timestamp', 'N/A')

    # Recorrer las criptomonedas que están en "rates" y mapearlas
    for symbol, price in data["rates"].items():
        # Crear el symbol con el sufijo USDT
        symbol_with_usdt = f"{symbol}USDT"
        
        # Crear el name sin el sufijo USDT
        name_without_usdt = symbol
        
        # Agregar la información mapeada
        filtered_data.append({
            'name': name_without_usdt,       # Nombre sin el sufijo 'USDT'
            'symbol': symbol_with_usdt,      # Símbolo con el sufijo 'USDT'
            'price': price,                  # Precio de la moneda
            'timestamp': timestamp           # Timestamp de la consulta
        })

# Crear el directorio para guardar el archivo si no existe
output_dir = 'crypto_etl_project/data/coinlayer'
os.makedirs(output_dir, exist_ok=True)

# Definir el archivo de salida
output_file = os.path.join(output_dir, 'coinlayer_data_new.json')

# Guardar los datos mapeados en un archivo JSON
try:
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_data, f, ensure_ascii=False, indent=4)
    print(f"Datos mapeados guardados correctamente en {output_file}")
except Exception as e:
    print(f"Error al guardar los datos mapeados en el archivo: {e}")
