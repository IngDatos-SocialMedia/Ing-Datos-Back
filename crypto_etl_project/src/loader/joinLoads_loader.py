import os
import json
import pandas as pd

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

# Función para cargar y combinar los datos de todos los archivos
def load_and_combine_data():
    print("Iniciando el proceso de carga y combinación de datos...")
    
    # Cargar los datos de los archivos JSON
    gecko_data = load_data("crypto_etl_project/data/coingecko/gecko_new.json")
    coinlayer_data = load_data("crypto_etl_project/data/coinlayer/coinlayer_data_new.json")
    coinmarketcap_data = load_data("crypto_etl_project/data/coinmarketcap/coin_data_transformed.json")
    cryptocompare_data = load_data("crypto_etl_project/data/cryptocompare/crypto_prices_new.json")
    
    # Convertir los datos a DataFrame
    gecko_df = pd.DataFrame(gecko_data)
    coinlayer_df = pd.DataFrame(coinlayer_data)
    coinmarketcap_df = pd.DataFrame(coinmarketcap_data)
    cryptocompare_df = pd.DataFrame(cryptocompare_data)
    
    # Mostrar las primeras filas de cada DataFrame (opcional)
    print(gecko_df.head())
    print(coinlayer_df.head())
    print(coinmarketcap_df.head())
    print(cryptocompare_df.head())

    # Concatenar todos los DataFrames en uno solo
    combined_df = pd.concat([gecko_df, coinlayer_df, coinmarketcap_df, cryptocompare_df], ignore_index=True)
    
    
    
    # Mostrar las primeras filas del DataFrame combinado con el cambio de nombre
    print(combined_df.head())

    return combined_df

# Llamar a la función para cargar y combinar los datos
combined_df = load_and_combine_data()

# Guardar el DataFrame combinado como un archivo CSV (opcional)
combined_df.to_csv("combined_data.csv", index=False)
