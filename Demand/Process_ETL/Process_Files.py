'''Contiene las siguientes funciones:
- read_files: Lee archivos Excel de un directorio y los consolida en un DataFrame   
- asign_country_code: Asigna el código de país a cada fila del DataFrame df usando el DataFrame country como referencia.
- process_columns: Procesa las columnas relevantes del DataFrame df y las convierte a mayúsculas.
- group_parquet: Guarda un DataFrame consolidado en archivos Parquet segmentados por año-mes.
'''

#--------------------------------------------------
#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
# Liberia
import pandas as pd

# Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico.
import glob
import os

from Fill_Rate.Process_ETL.Process_Files import read_files, group_parquet


# Asgina pais segun el demand group
def asign_country_code(df_consolidated, df_country):
        """
        Asigna el código de país a cada fila del DataFrame df
        usando el DataFrame country como referencia.
        """
        # Crear una columna 'code concat country' que concatena 'Country Code' y 'Destination Country'
       
        # Crear un mapa de códigos de país a nombres de país
        country_map = df_country.set_index('Demand Group')['Country']

        # Usar .map() para crear la nueva columna 'new country'
        df_consolidated['fk_Country'] = df_consolidated['Demand Group'].map(country_map)

        return df_consolidated

# Procesa las columnas relevantes del DataFrame df y las convierte a mayúsculas.    
def process_columns(df_consolidated,lst_columns):
    ""    
    """
            Escoje las columnas relevantes del DataFrame df y las procesa
            para asegurar que todas las columnas estén en mayúsculas y sin espacios.
    """
    try:
        

        # --- PROCESAMIENTO Y AGRUPACION ---
        # Crear una columna 'year_month' para usar en la agrupación (ej: '2023-01').
        # Se usa .str.zfill(2) para asegurar que los meses tengan dos dígitos (ej: '01', '02', etc.)
        # lo que mejora la consistencia y el orden de los nombres de archivo.       
        df_consolidated['fk_year_month'] = (df_consolidated['Fiscal Year'].astype(str) + '-' +
                                            df_consolidated['Fiscal Period'].astype(str).str.zfill(2))
        df_consolidated['clasification']=(df_consolidated['GPP Division'] + '-' +
                                         df_consolidated['GPP Category'] + '-' +
                                         df_consolidated['GPP Portfolio'])
        
        
        df_consolidated['fk_date_country_clasification'] = (df_consolidated['fk_year_month'] + '-' +
                                                            df_consolidated['fk_Country']+ '-' +
                                                            df_consolidated['clasification']).str.lower().str.strip()
        df_consolidated['fk_Date']=pd.to_datetime(df_consolidated['fk_year_month'],
                                                  format='%Y-%m',
                                                  errors='coerce')
        df_consolidated.rename(columns={
            'Global Material': 'fk_SKU'}, inplace=True)
        
        df_processed = df_consolidated[lst_columns]                                                                                                                                                                           
        # Convertir todas las columnas a mayúsculas y eliminar espacios
        for col in df_processed.columns:        
            df_processed.loc[:,col] = df_consolidated[col].astype(str).str.upper().str.strip()    
       
    except KeyError as e:
                print(f"Error: La columna {e} no se encontró en los archivos. ")
    return df_processed

# Función principal que ejecuta el script.
# Esta función no recibe parámetros y no devuelve ningún valor.
def main():
    # Importar las rutas de acceso rápido desde config_paths.py.,
    from config_paths import DemandPaths
    historic_raw_dir = DemandPaths.INPUT_RAW_HISTORIC_DIR
    country_code_file = DemandPaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
    processed_parquet_dir = DemandPaths.OUTPUT_PROCESSED_PARQUETS_DIR
    # Leer los archivos de datos históricos y consolidarlos en un DataFrame.
    df_consolidated = read_files(historic_raw_dir)
    # Leer el archivo de códigos de país.
    df_country = pd.read_excel(country_code_file,
                               sheet_name='Code Country Demand', dtype=str, engine='openpyxl')
    # Definir las columnas relevantes para el procesamiento.    
    lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_SKU',
                  'fk_date_country_clasification',
                  'Demand History & Forecast-QTY', 'Shipment History& Forecast-Qty',
                  'Demand History & Forecast-GSV', 'Shipment History&Forecast-GSV']
    df_consolidated = asign_country_code(df_consolidated, df_country)
    df_processed=process_columns(df_consolidated,lst_columns)
    group_parquet(df_processed, processed_parquet_dir,name='demand')


# --- EJECUCION DEL SCRIPT ---
# Es una buena práctica envolver la ejecución principal en un bloque if __name__ == "__main__":
if __name__ == "__main__":
    main()
    print("Script de procesamiento de archivos historicos de Demand ejecutado correctamente.")