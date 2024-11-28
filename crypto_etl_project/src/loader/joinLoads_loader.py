import os
import json
import pandas as pd

# Función para cargar datos desde un archivo JSON
def load_data(file_path):
    """
    Carga los datos de un archivo JSON y devuelve el contenido en formato de lista de diccionarios.
    
    :param file_path: Ruta del archivo JSON a cargar
    :return: Lista de diccionarios con los datos cargados
    """
    print(f"Intentando cargar los datos desde {file_path}...")
    
    if os.path.exists(file_path):  # Verificar si el archivo existe
        try:
            # Abrir el archivo y cargar los datos en formato JSON
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                print(f"Datos cargados: {len(data)} registros encontrados.")
                return data
        except json.JSONDecodeError as e:
            print(f"Error al decodificar el archivo JSON: {e}")
            return []
    else:
        print(f"El archivo {file_path} no existe.")
        return []

# Función para cargar y combinar los datos de todos los archivos JSON
def load_and_combine_data():
    """
    Carga datos desde varios archivos JSON y los combina en un único DataFrame de Pandas.
    
    :return: DataFrame combinado con los datos de todos los archivos JSON
    """
    print("Iniciando el proceso de carga y combinación de datos...")

    # Rutas de los archivos JSON
    gecko_file = "crypto_etl_project/data/coingecko/gecko_new.json"
    coinlayer_file = "crypto_etl_project/data/coinlayer/coinlayer_data_new.json"
    coinmarketcap_file = "crypto_etl_project/data/coinmarketcap/coin_data_transformed.json"
    cryptocompare_file = "crypto_etl_project/data/cryptocompare/crypto_prices_new.json"

    # Cargar los datos desde los archivos JSON
    gecko_data = load_data(gecko_file)
    coinlayer_data = load_data(coinlayer_file)
    coinmarketcap_data = load_data(coinmarketcap_file)
    cryptocompare_data = load_data(cryptocompare_file)

    # Convertir los datos a DataFrames de Pandas
    gecko_df = pd.DataFrame(gecko_data)
    coinlayer_df = pd.DataFrame(coinlayer_data)
    coinmarketcap_df = pd.DataFrame(coinmarketcap_data)
    cryptocompare_df = pd.DataFrame(cryptocompare_data)

    # Concatenar todos los DataFrames en uno solo
    combined_df = pd.concat([gecko_df, coinlayer_df, coinmarketcap_df, cryptocompare_df], ignore_index=True)

    # Mostrar las primeras filas del DataFrame combinado
    print(combined_df.head())  # Visualización de las primeras filas

    return combined_df

# Función principal para ejecutar el proceso completo y guardar los datos combinados
def main():
    """
    Ejecuta el proceso de carga, combinación de datos y guardado en archivo CSV.
    """
    # Ejecutar el proceso de carga y combinación de datos
    combined_df = load_and_combine_data()
    
    if combined_df.empty:
        print("El DataFrame combinado está vacío. No se puede guardar el archivo CSV.")
    else:
        print(f"Datos combinados listos para guardar. Total de registros: {len(combined_df)}")
        
        # Guardar el DataFrame combinado como un archivo CSV
        combined_df.to_csv("combined_data.csv", index=False)
        print("Datos combinados guardados correctamente en 'combined_data.csv'.")

# Ejecutar la función principal si este archivo es ejecutado directamente
if __name__ == "__main__":
    main()
