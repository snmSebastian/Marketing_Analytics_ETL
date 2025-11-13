'''
Módulo de orquestación para la Carga Completa (Full Load) de los datos de Demanda (Demand).
Reutiliza funciones genéricas de E/L del módulo Fill_Rate y define lógica de transformación (T)
específica para Demanda, incluyendo un mapeo de país distinto y una clave de unicidad adaptada.

Contiene las siguientes funciones:
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

from Fill_Rate.Process_ETL.Process_Files import read_files, group_parquet,format_columns


# Asgina pais segun el demand group
def asign_country_code(df_consolidated, df_country):
        """
        Asigna el código de país (fk_Country) a cada registro utilizando la columna 'Demand Group'
        como clave de mapeo contra el DataFrame de referencia

        Args:
            df_consolidated (pd.DataFrame): DataFrame principal de Demand. Debe contener la columna 'Demand Group'.
            df_country (pd.DataFrame): DataFrame de referencia para el mapeo. Debe contener 'Demand Group' y 'Country'.
        Returns:
            pd.DataFrame: El DataFrame df_consolidated modificado con la nueva columna 'fk_Country'.
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
    Renombra la columna 'Global Material' a 'fk_SKU', calcula las claves compuestas ('fk_year_month',
    'clasification') y genera la clave única 'fk_date_country_clasification'
    (omitiendo el código de cliente). Finalmente, selecciona las columnas deseadas

    Args:
        df_consolidated (pd.DataFrame): DataFrame con todas las columnas sin procesar.
        lst_columns (list): Lista de strings con los nombres de las columnas finales deseadas, incluyendo las recién creadas.
    Returns:
        pd.DataFrame: DataFrame final, filtrado y estandarizado, listo para ser guardado.
    KeyError: Si alguna columna requerida para la creación de claves (ej: 'Fiscal Year')
             no existe en el DataFrame.
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
                                                  format='%Y-%b',
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

def assign_local_currency(df_consolidated,df_fx_rate):
     df_consolidated['fk_YearMonthCountry']=(df_consolidated['fk_year_month'] + '-' +
                                                            df_consolidated['fk_Country'])
     df_consolidated['fk_YearMonthCountry']=df_consolidated['fk_YearMonthCountry'].str.upper().str.strip()
     df_fx_rate['fk_YearMonthCountry']=(df_fx_rate['Year']+'-'+
                                        df_fx_rate['Month']+'-'+
                                        df_fx_rate['Country'])
     df_fx_rate['fk_YearMonthCountry']=df_fx_rate['fk_YearMonthCountry'].str.upper().str.strip()
     df_consolidated=pd.merge(
          df_consolidated,
          df_fx_rate[['fk_YearMonthCountry','OP Rate']],
          on='fk_YearMonthCountry',
          how='left')
     df_consolidated['OP Rate']=df_consolidated['OP Rate'].astype(float)
     df_consolidated['Demand History & Forecast-GSV']=df_consolidated['Demand History & Forecast-GSV'].astype(float)
     df_consolidated['OP Rate']=df_consolidated['OP Rate'].fillna(0)
     df_consolidated['Demand History & Forecast-GSV']=df_consolidated['Demand History & Forecast-GSV'].fillna(0)

     df_consolidated['Demand $ Local Currency']=df_consolidated['Demand History & Forecast-GSV']*df_consolidated['OP Rate']
     cols_to_drop = ['fk_YearMonthCountry','OP Rate']
     df_consolidated.drop(columns=[col for col in cols_to_drop if col in df_consolidated.columns], inplace=True)
     return df_consolidated    


# Función principal que ejecuta el script.
# Esta función no recibe parámetros y no devuelve ningún valor.
def main():
    print("=" * 55)
    print("---  INICIANDO PROCESO: DEMAND FULL LOAD ETL ---")
    print("=" * 55)
    """
    Función principal que orquesta el flujo ETL de Carga Completa para los datos de Demanda.	
    Inicializa las rutas, lee los archivos, realiza la adaptación específica de mapeo de país y	
    procesamiento de claves para Demanda, y guarda el resultado particionado.
    Returns: None: La función orquesta el proceso y no devuelve un valor.
    """
    # Importar las rutas de acceso rápido desde config_paths.py.,
    from config_paths import DemandPaths
    historic_raw_dir = DemandPaths.INPUT_RAW_HISTORIC_DIR
    country_code_file = DemandPaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
    processed_parquet_dir = DemandPaths.OUTPUT_PROCESSED_PARQUETS_DIR
    fx_rate=DemandPaths.INPUT_PROCESSED_FX_RATE_FILE
     #===============================
    # --- Lectura de archivos 
    #===============================
    # Leer los archivos de datos históricos y consolidarlos en un DataFrame.
    df_consolidated = read_files(historic_raw_dir)
    # Leer el archivo de códigos de país.
    df_country = pd.read_excel(country_code_file,
                               sheet_name='Code Country Demand', dtype=str, engine='openpyxl')
    df_fx_rate = pd.read_excel(fx_rate, dtype=str, engine='openpyxl')

    # Definir las columnas relevantes para el procesamiento.    
    lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_SKU',
                  'fk_date_country_clasification',
                  'Demand History & Forecast-QTY', 'Shipment History& Forecast-Qty',
                  'Demand History & Forecast-GSV', 'Shipment History&Forecast-GSV']
    df_consolidated = asign_country_code(df_consolidated, df_country)
    df_processed=process_columns(df_consolidated,lst_columns)

    #--- Formato de columnas ----
    lst_columns_str = ['fk_Date','fk_year_month', 'fk_Country', 'fk_SKU',
                  'fk_date_country_clasification']
    lst_columns_float=['Demand History & Forecast-QTY', 'Shipment History& Forecast-Qty',
                  'Demand History & Forecast-GSV', 'Shipment History&Forecast-GSV']
    df_processed=format_columns(df_processed,lst_columns_str,lst_columns_float)
    #=========================================================
    #--- ASIGNACIÓN COLUMNAS CALCULADAS
    #=========================================================
    df_consolidated=assign_local_currency(df_consolidated,df_fx_rate)
    
    # --- Agrupacion en archivos parquets
    group_parquet(df_processed, processed_parquet_dir,name='demand')


# --- EJECUCION DEL SCRIPT ---
# Es una buena práctica envolver la ejecución principal en un bloque if __name__ == "__main__":
if __name__ == "__main__":
    try:
        main()
        print("Script de procesamiento de archivos historicos de Demand ejecutado correctamente.")
    except Exception as e:
        print(f"Error en procesamiento de archivos historicos de Demand: {e}")