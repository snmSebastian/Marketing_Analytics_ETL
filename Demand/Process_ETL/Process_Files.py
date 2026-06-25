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
from pathlib import Path
import sys

from Fill_Rate.Process_ETL.Process_Files import read_files, group_parquet,format_columns
from Sales.Process_ETL.Process_Files import assign_nsv,assign_NPI_New_Carryover,assign_fk_YearRegionSku

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
        """
    Asigna el código de país normalizando 'Demand Group' para evitar errores de case-sensitivity.
    """
        # 1. Normalizamos la tabla maestra
        df_lookup = df_country.copy()
        df_lookup['Demand Group'] = df_lookup['Demand Group'].astype(str).str.upper().str.strip()
        
        # 2. Creamos el mapa (asegurando valores únicos)
        country_map = df_lookup.drop_duplicates('Demand Group').set_index('Demand Group')['Country']

        # 3. Normalizamos la columna de búsqueda en el principal
        search_col = df_consolidated['Demand Group'].astype(str).str.upper().str.strip()

        # 4. Mapeo y respaldo
        df_consolidated['fk_Country'] = search_col.map(country_map).fillna(df_consolidated['Demand Group'])

        return df_consolidated

def asign_gpp(df_consolidated, df_gpp):
    """
    Asigna GPP segun la clave compuesta 'fk_gpp' a cada registro.

    Genera una clave temporal 'fk_gpp' concatenando códigos de división, categoría y portafolio 
    para realizar un cruce (merge) y traer dimensiones descriptivas.

    Args:
        df_consolidated (pd.DataFrame): DataFrame principal que contiene las columnas de códigos: 
            'GPP Division Code', 'GPP Category Code' y 'GPP Portfolio Code'.
        df_gpp (pd.DataFrame): DataFrame de referencia de GPP que debe contener los códigos de 
            mapeo y las columnas: 'GPP SBU', 'GPP Division Description', 
            'GPP Category Description' y 'GPP Portfolio Description'.

    Returns:
        pd.DataFrame: El DataFrame consolidado enriquecido con las descripciones de GPP y 
            sin la columna temporal de unión.
    """
    df_gpp['fk_gpp']=df_gpp['GPP Division Code'] + '-' + df_gpp['GPP Category Code'] + '-' + df_gpp['GPP Portfolio Code']
    df_consolidated['fk_gpp']=df_consolidated['GPP Division Code'] + '-' + df_consolidated['GPP Category Code'] + '-' + df_consolidated['GPP Portfolio Code']
    
    # Eliminar duplicados en el maestro para evitar duplicidad de filas en el merge
    df_gpp = df_gpp.drop_duplicates(subset=['fk_gpp'])

    df_consolidated=pd.merge(
        df_consolidated,
        df_gpp[['fk_gpp','GPP SBU','GPP Division Description','GPP Category Description','GPP Portfolio Description']],
        on='fk_gpp',
        how='left')
    df_consolidated.drop(columns=['fk_gpp'], inplace=True)
    return df_consolidated

def asign_skuName(df_consolidated,df_skuName):
    """
    Asigna nombres de SKU y marcas al DataFrame consolidado mediante la clave 'fk_SKU'.

    Limpia los nombres de las columnas, estandariza las llaves de unión y elimina 
    duplicados en la referencia antes de realizar un merge por la izquierda.

    Args:
        df_consolidated (pd.DataFrame): Datos de demanda. Requiere columna 'Global Material'.
        df_skuName (pd.DataFrame): Maestro de materiales. Requiere 'SKU', 'SKU Description' y 'BRAND'.

    Returns:
        pd.DataFrame: DataFrame enriquecido con 'SKU Description' y 'BRAND'.
    """
    
    df_consolidated.rename(columns={
            'Global Material': 'fk_SKU'}, inplace=True)
    df_skuName.rename(columns={
            'SKU': 'fk_SKU'}, inplace=True)
    df_skuName.drop_duplicates(subset=['fk_SKU'], inplace=True)
    
    df_consolidated['fk_SKU'] = df_consolidated['fk_SKU'].astype(str).str.upper().str.strip()
    df_skuName['fk_SKU'] = df_skuName['fk_SKU'].astype(str).str.upper().str.strip()

    df_consolidated=pd.merge(
        df_consolidated,
        df_skuName[['fk_SKU','SKU Description','Brand']],
        on='fk_SKU',
        how='left')
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
                                           df_consolidated['Fiscal Period'].astype(str).str[-2:])
        df_consolidated['fk_Date']=pd.to_datetime(df_consolidated['fk_year_month'],
                                                  format='%Y-%m',
                                                  errors='coerce')
        df_consolidated.rename(columns={
            'Global Material': 'fk_SKU'}, inplace=True)
        
       
        df_processed = df_consolidated[lst_columns].copy()                                                                                                                                                                           
        # Convertir todas las columnas a mayúsculas y eliminar espacios
        for col in df_processed.columns:        
            df_processed[col] = df_processed[col].astype(str).str.upper().str.strip() 
       
    except KeyError as e:
                print(f"Error: La columna {e} no se encontró en los archivos. ")
    return df_processed

def assign_local_currency(df_consolidated,df_fx_rate):
     """
        Convierte el Forecast de dólares a moneda local aplicando la tasa de cambio (OP Rate).

        Crea una llave temporal de 'Año-Mes-País' para cruzar los datos consolidados con la 
        tabla de FX Rates. Al final, limpia las columnas auxiliares y nos entrega el 
        Forecast con el valor calculado en moneda local, asegurándose de que no queden 
        valores vacíos (NaN) que rompan los cálculos.
     """

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
     df_consolidated['OP Rate'] = df_consolidated['OP Rate'].fillna(0).astype('float32')
     df_consolidated['FORECAST_VALUE_GSV'] = df_consolidated['FORECAST_VALUE_GSV'].fillna(0).astype('float32')

     df_consolidated['Demand $ Local Currency']=df_consolidated['FORECAST_VALUE_GSV']*df_consolidated['OP Rate']
     cols_to_drop = ['fk_YearMonthCountry','OP Rate']
     df_consolidated.drop(columns=[col for col in cols_to_drop if col in df_consolidated.columns], inplace=True)
     return df_consolidated    

def delete_parquet_files(folder_path: str):

    """
    Elimina todos los archivos con extensión .parquet dentro de la carpeta especificada.
    
    Args:
        folder_path (str): Ruta del directorio donde se encuentran los parquets.
        
    Returns:
        None: La función ejecuta la acción e imprime el resultado.
    """
    # --- CONFIGURACIÓN DE RUTA ---
    directory = Path(folder_path)
    
    # Verificar si la ruta existe y es un directorio
    if not directory.is_dir():
        print(f"La ruta {folder_path} no es válida o no existe.")
        return

    try:
        #=========================================================
        # --- BÚSQUEDA Y ELIMINACIÓN DE ARCHIVOS ---
        #=========================================================
        # .glob('*.parquet') busca solo archivos con esa extensión
        files_to_delete = list(directory.glob('*.parquet'))
        
        if not files_to_delete:
            print(f"No se encontraron archivos .parquet en: {folder_path}")
            return

        for file in files_to_delete:
            file.unlink()  # Elimina el archivo permanentemente
            print(f"Archivo eliminado: {file.name}")

        print(f"--- Limpieza completada: {len(files_to_delete)} archivos eliminados. ✅ ---")

    except Exception as e:
        print(f"Error al intentar eliminar archivos: {e}")
        sys.exit(1)


       