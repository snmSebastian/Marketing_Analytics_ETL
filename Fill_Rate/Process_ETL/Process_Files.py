'''
Módulo de procesamiento principal para datos de Fill Rate.
Este script implementa el flujo ETL (Extracción, Transformación y Carga)
para consolidar archivos históricos de Fill Rate, asignar códigos de país,
realizar transformaciones clave y guardar los datos limpios en formato Parquet
particionado por año-mes.

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
import numpy as np

# Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico.
import glob
import os


# Lectura de archivos
def read_files(input_path):
    """
    Lee archivos Excel de un directorio, los consolida en un dataframe
    
    Args:
        input_path (str): Ruta del directorio donde se encuentran los archivos Excel (.xlsx) a consolidar.
    
    Returns:
        pd.DataFrame or None: DataFrame consolidado con todos los datos de los archivos, o None si no se encuentran
        archivos o la lectura falla sin consolidar nada.
     """
    
    # --- LECTURA Y CONSOLIDACION DE ARCHIVOS ---
    # Buscar todos los archivos .xlsx en el directorio de entrada.
    all_files_xlsx = glob.glob(os.path.join(input_path, "*.xlsx"))

    if not all_files_xlsx:
        print(f"Advertencia: No se encontraron archivos .xlsx en '{input_path}'.")
        return

    # Leer cada archivo y agregarlo a una lista de DataFrames:
    lst_files_xlsx = []
    for filename in all_files_xlsx:
        try:
            print(f"Leyendo archivo: {os.path.basename(filename)}")
            # Usar pd.read_excel para archivos .xlsx, no pd.read_csv
            df = pd.read_excel(filename, dtype=str, engine='openpyxl')
            lst_files_xlsx.append(df)
        except PermissionError:
            print(f"  [ERROR] Permiso denegado para leer el archivo: {os.path.basename(filename)}."
                  "\n  Asegúrate de que no esté abierto en Excel y vuelve a intentarlo.")
        except Exception as e:
            print(f"  [ERROR] No se pudo procesar el archivo {os.path.basename(filename)}: {e}")

    # Concatenar todos los DataFrames en uno solo
    if len(lst_files_xlsx) == 0:
        df_consolidated = lst_files_xlsx[0]
    else:
         df_consolidated = pd.concat(lst_files_xlsx, axis=0, ignore_index=True)


    for col in df_consolidated.columns:
        df_consolidated[col] = df_consolidated[col].astype(str).str.upper().str.strip()

    return df_consolidated

# Asignacion pais
def asign_country_code(df_consolidated, df_country):
        """
        Asigna el código de país a cada fila del DataFrame df
        usando el DataFrame country como referencia.

        Args:
            df_consolidated (pd.DataFrame): DataFrame principal con los datos de Fill Rate.
                                            Debe contener las columnas 'Country Code' y 'Destination Country'.
            df_country (pd.DataFrame): DataFrame de referencia para el mapeo de países.
                                       Debe contener 'Country Code Concat' y 'Country'.

        Returns:
            pd.DataFrame: El DataFrame df_consolidated original, modificado con las nuevas columnas 'code concat country'
                          y 'fk_Country'.    
        """
        # Crear una columna 'code concat country' que concatena 'Country Code' y 'Destination Country'
        df_consolidated['code concat country'] = df_consolidated['Country Code'].astype(str) + df_consolidated['Destination Country'].astype(str)

        # Crear un mapa de códigos de país a nombres de país
        for col in df_country.columns:
            df_country[col] = df_country[col].astype(str).str.upper().str.strip()
        country_map = df_country.set_index('Country Code Concat')['Country']

        # Usar .map() para crear la nueva columna 'new country'
        df_consolidated['fk_Country'] = df_consolidated['code concat country'].map(country_map)

        return df_consolidated

def process_columns(df_consolidated,lst_columns):
    """    
        Renombra, calcula columnas clave ('fk_year_month', 'clasification', 'fk_date_country_customer_clasification',
        'fk_Date'), y selecciona el subconjunto final de columnas para el DataFrame procesado.
    Args:
        df_consolidated (pd.DataFrame): DataFrame consolidado que contiene todas las columnas sin procesar.
        lst_columns (list): Lista de strings con los nombres de las columnas finales deseadas, incluyendo las recién creadas (e.g., 'fk_Date', 'fk_Country').
    Returns:
        pd.DataFrame: DataFrame final, filtrado por lst_columns, listo para ser guardado.
    """
    try:
        

        # --- PROCESAMIENTO Y AGRUPACION ---
        # Crear una columna 'year_month' para usar en la agrupación (ej: '2023-01').
        # Se usa .str.zfill(2) para asegurar que los meses tengan dos dígitos (ej: '01', '02', etc.)
        # lo que mejora la consistencia y el orden de los nombres de archivo.   
        # 
        # Definir los mapeos que quieres aplicar
        mapeo_deseado = {
            'Sold-To-Customer Code': 'fk_Sold_To_Customer_Code',
            'Sold-To Customer Code': 'fk_Sold_To_Customer_Code', # La clave es diferente, el valor es el mismo
            'Country Material': 'fk_SKU',
            'Global Material': 'fk_SKU',
        }

        # Crear un nuevo diccionario de renombre que solo incluye las columnas existentes
        columnas_existentes = df_consolidated.columns
        mapeo_filtrado = {
            old_name: new_name
            for old_name, new_name in mapeo_deseado.items()
            if old_name in columnas_existentes
        }

        df_consolidated.rename(columns=mapeo_filtrado, inplace=True)    

        df_consolidated['fk_year_month'] = (df_consolidated['Fiscal Year'].astype(str) + '-' +
                                            df_consolidated['Fiscal Period'].astype(str).str.zfill(2))
        df_consolidated['clasification']=(df_consolidated['GPP Division'] + '-' +
                                         df_consolidated['GPP Category'] + '-' +
                                         df_consolidated['GPP Portfolio'])
        
        
        df_consolidated['fk_date_country_customer_clasification'] = (df_consolidated['fk_year_month'] + '-' +
                                                            df_consolidated['fk_Country']+ '-' +
                                                            df_consolidated['fk_Sold_To_Customer_Code']+ '-' +
                                                            df_consolidated['clasification']).str.upper().str.strip()
        df_consolidated['fk_Date']=pd.to_datetime(df_consolidated['fk_year_month'],
                                                  format='%Y-%b',
                                                  errors='coerce')
       
        df_processed = df_consolidated[lst_columns].copy()                                                                                                                                                                           
        # Convertir todas las columnas a mayúsculas y eliminar espacios
        for col in df_processed.columns:        
            df_processed.loc[:,col] = df_consolidated[col].astype(str).str.upper().str.strip()    
       
    except KeyError as e:
                print(f"Error: La columna {e} no se encontró en los archivos. ")
    return df_processed

def format_columns(df: pd.DataFrame, lst_columns_str: list, lst_columns_float: list) -> pd.DataFrame:
    """
    Convierte las columnas especificadas a str o float, manejando errores de conversión:
    - Los errores en float se convierten a NaN y luego a 0.
    - Los valores NaN/nulos/errores en str se convierten a la cadena vacía "".
    """
    df=df.copy()
    df=df[lst_columns_str+lst_columns_float]
    # 1. CONVERSIÓN Y LIMPIEZA DE CADENAS (STR)
    for col in lst_columns_str:
        # 1.1. Convertir a str
        df[col] = df[col].astype(str)
        
        # 1.2. Limpieza de strings: minúsculas, reemplazo de 'nan' a '', y eliminar espacios
        df[col] = (df[col].str.lower()
                           .str.replace('nan', '', regex=False)
                           .str.strip())
        
        # 1.3. Opcional: Manejo de nulos originales que no son 'nan' string (ej. np.nan)
        # Aunque astype(str) maneja la mayoría, este fillna es un seguro extra.
        df[col] = df[col].fillna('')

    # 2. CONVERSIÓN A FLOTANTE (FLOAT) con manejo de errores
    for col in lst_columns_float:
        # 2.1. Conversión con manejo de errores (errors='coerce' -> errores a NaN)
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col]=df[col].fillna(np.nan)  # Asegurar que los NaN se manejen correctamente
        # 2.2. Reemplazar NaN (errores de conversión) por 0, y asegurar dtype float
        df[col] = df[col].fillna(0).astype(np.float32)
        
    return df

def group_parquet(df_processed, output_path,name='fill_rate'):
    """ Guarda un dataframe consolidado en archivos Parquet segmentados por año-mes.
    Args:
        df_processed (pd.DataFrame): DataFrame ya limpio y procesado. Debe contener la columna 'fk_year_month'.
        output_path (str): Ruta del directorio donde se guardarán los archivos Parquet particionados.
        name (str, optional): Prefijo para los nombres de los archivos Parquet generados. Por defecto es 'fill_rate'. 

    Returns:
        None: La función no devuelve un valor, sino que guarda los archivos en el disco.
    """
    # --- ESCRITURA DE ARCHIVOS PARQUET SEGMENTADOS ---
    # Agrupar el DataFrame por 'year_month' y guardar cada grupo en un archivo Parquet.
    for period, group in df_processed.groupby('fk_year_month'):
        # Crear un nombre de archivo descriptivo, ej: sales_2023-01.parquet
        output_filename = f"{name}_{period}.parquet"
        output_full_path = os.path.join(output_path, output_filename)
        print(f"Guardando grupo {period} en: {output_full_path}\n")
        # Guardar el grupo en formato Parquet, excluyendo el índice.
        group.to_parquet(output_full_path, index=False)
        #print("\nProceso completado. Archivos Parquet generados exitosamente.")

def main():
    """
    Función principal que orquesta el flujo ETL completo para los datos de Fill Rate.
    Lee las rutas de configuración, ejecuta la lectura, el mapeo de países,
    el procesamiento de columnas y la segmentación en archivos Parquet.

    Returns: None: La función orquesta el proceso completo y no devuelve un valor.
    """
    # importamos las rutas de archivos
    from config_paths import FillRatePaths
    fil_rate_historic_raw_dir = FillRatePaths.INPUT_RAW_HISTORIC_DIR
    country_code_file = FillRatePaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
    processed_parquet_dir = FillRatePaths.OUTPUT_PROCESSED_PARQUETS_DIR

    # Leer los archivos de datos históricos y consolidarlos en un DataFrame.
    df_consolidated = read_files(fil_rate_historic_raw_dir)
    # Leer el archivo de códigos de país.
    df_country = pd.read_excel(country_code_file,
                               sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
    # Definir las columnas relevantes para el procesamiento.    
    lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                   'fk_date_country_customer_clasification',
                   'Fill Rate First Pass Order Qty', 'Fill Rate First Pass Invoice Qty',
                   'Fill Rate First Pass Order $', 'Fill Rate First Pass Invoice $']
    df_consolidated = asign_country_code(df_consolidated, df_country)
    df_processed=process_columns(df_consolidated,lst_columns)
    # Defino formato de las columnas
    lst_columns_str=['fk_Date', 'fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code',
       'fk_SKU', 'fk_date_country_customer_clasification']
    
    lst_columns_float=['Fill Rate First Pass Order Qty', 'Fill Rate First Pass Invoice Qty',
       'Fill Rate First Pass Order $', 'Fill Rate First Pass Invoice $']
    df_processed=format_columns(df_processed,lst_columns_str,lst_columns_float)
    
    group_parquet(df_processed, processed_parquet_dir, name='fill_rate')



# --- EJECUCION DEL SCRIPT ---
# Es una buena práctica envolver la ejecución principal en un bloque if __name__ == "__main__":
if __name__ == "__main__":
    try:
        main()
        print("Processing of historical Fill Rate data completed successfully.")
    except Exception as e:
        print(f"Error en procesamiento de datos de Fill Rate: {e}")