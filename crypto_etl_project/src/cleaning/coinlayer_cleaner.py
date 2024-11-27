import json
import os
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np

# Función principal para procesar y guardar los datos
def process_and_save_coinlayer_data():
    # Leer los datos crudos desde el archivo
    input_file = 'crypto_etl_project/data/coinlayer/coinlayer_data.json'

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error al leer los datos desde el archivo: {e}")
        return

    # Filtrar los datos relevantes: name, symbol, price y timestamp
    filtered_data = []
    price_data = []  # Para almacenar los precios y usar regresión lineal si es necesario

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
            
            # Si el precio está vacío o es nulo, lo añadimos para imputar después
            if price is None or price == "":
                price = np.nan  # Marcamos como NaN para imputación

            # Agregar la información mapeada
            filtered_data.append({
                'name': name_without_usdt,       # Nombre sin el sufijo 'USDT'
                'symbol': symbol_with_usdt,      # Símbolo con el sufijo 'USDT'
                'price': price,                  # Precio de la moneda
                'timestamp': timestamp           # Timestamp de la consulta
            })

            # Agregar los datos para usar en la regresión lineal (solo aquellos con valores de precio)
            price_data.append({
                'symbol': symbol,
                'price': price if price is not np.nan else None
            })

    # Realizar regresión lineal para llenar valores nulos en 'price'
    fill_missing_prices(filtered_data, price_data)

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

# Función para imputar los precios faltantes usando regresión lineal
def fill_missing_prices(filtered_data, price_data):
    # Convertir los datos de precios en un DataFrame
    df = pd.DataFrame(price_data)

    # Filtrar los valores no nulos para entrenamiento (solo aquellos que tienen un precio)
    train_data = df.dropna(subset=['price'])

    if len(train_data) == 0:
        print("No hay datos suficientes para realizar la regresión lineal.")
        return

    # Preparar los datos para la regresión lineal
    # Usamos el índice como variable predictora
    train_data['index'] = range(len(train_data))

    # Modelo de regresión lineal
    model = LinearRegression()

    # Entrenamos el modelo solo con los datos no nulos
    X_train = train_data[['index']]  # Variable independiente (índice)
    y_train = train_data['price']    # Variable dependiente (precio)

    model.fit(X_train, y_train)

    # Ahora, predecimos los precios para los datos faltantes
    missing_data = df[df['price'].isna()]
    if not missing_data.empty:
        # Para las filas con precios faltantes, usamos el índice para predecir el valor
        missing_data['index'] = range(len(train_data), len(train_data) + len(missing_data))
        X_missing = missing_data[['index']]
        predicted_prices = model.predict(X_missing)

        # Reemplazar los valores faltantes con las predicciones
        for idx, pred_price in zip(missing_data.index, predicted_prices):
            df.at[idx, 'price'] = pred_price

    # Actualizar los datos filtrados con los precios imputados
    for i, item in enumerate(filtered_data):
        symbol = item['symbol'].replace('USDT', '')  # Obtener el símbolo sin 'USDT'
        df_symbol = df[df['symbol'] == symbol]
        if not df_symbol.empty:
            # Actualizar el precio en filtered_data
            item['price'] = df_symbol.iloc[0]['price']

# Ejecutar la función principal
if __name__ == "__main__":
    process_and_save_coinlayer_data()
