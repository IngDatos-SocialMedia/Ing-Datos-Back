import json
import os

# Ruta del archivo JSON original
input_file = 'crypto_etl_project/data/cryptocompare/crypto_prices.json'
# Ruta del archivo de salida
output_file = 'crypto_etl_project/data/cryptocompare/crypto_prices_new.json'

# Leer el archivo JSON existente
try:
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"Datos cargados correctamente desde {input_file}")
except Exception as e:
    print(f"Error al leer el archivo {input_file}: {e}")
    exit()

# Mapeo y renombrado de los atributos
mapped_data = []
for entry in data:
    # Extraer el symbol_pair y hacer los cambios necesarios
    symbol_pair = entry.get('symbol_pair', '')
    
    # Obtener el nombre de la criptomoneda (sin "USD")
    name = symbol_pair.replace('USD', '')  # Eliminar "USD" para el campo name
    symbol = symbol_pair + 'T'  # Agregar "USDT" al símbolo original

    # Crear un nuevo diccionario con los atributos renombrados
    mapped_data.append({
        'name': name,  # Renombrado a 'name' con solo la criptomoneda
        'symbol': symbol,  # Renombrado a 'symbol' con "USDT" agregado al final
        'price': entry.get('price', 'N/A'),
        'timestamp': entry.get('timestamp', 'N/A'),
    })

# Crear el directorio de salida si no existe
output_dir = os.path.dirname(output_file)
os.makedirs(output_dir, exist_ok=True)

# Guardar los datos transformados en el nuevo archivo JSON
try:
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(mapped_data, f, ensure_ascii=False, indent=4)
    print(f"Datos renombrados guardados correctamente en {output_file}")
except Exception as e:
    print(f"Error al guardar los datos en el archivo {output_file}: {e}")
