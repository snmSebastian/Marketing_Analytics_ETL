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
import numpy as np

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
    Toma informacion dada por datalake y Asigna GPP segun la clave compuesta 'fk_gpp' a cada registro.

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
    Esta informacion de nombre y brand es tomada del datalake.
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

def assign_FX_NSV_LC(df_consolidated,df_fx_rate):
     """
       Genera la variable (nsv_lc_cy/fx_cy)-(nsv_lc_py/fx_py)
     """
     #LLAVE PARA OBTENER FX CY
     df_consolidated['fk_YearMonthCountryCrncy']=(df_consolidated['fk_year_month'] + '-' +
                                             df_consolidated['fk_Country']+'-'+
                                             df_consolidated['CRNCY_KEY']                   )
     df_consolidated['fk_YearMonthCountryCrncy']=df_consolidated['fk_YearMonthCountryCrncy'].str.upper().str.strip()
     
     #LLAVE PARA OBTNER FX PY
     df_consolidated['fk_YearMonthCountryCrncy_PY'] = (
        (df_consolidated['fk_year_month'].str[:4].astype(int) - 1).astype(str) 
        + df_consolidated['fk_year_month'].str[4:] 
        + '-' + df_consolidated['fk_Country'] 
        + '-' + df_consolidated['CRNCY_KEY']
     )
     df_consolidated['fk_YearMonthCountryCrncy_PY']=df_consolidated['fk_YearMonthCountryCrncy_PY'].str.upper().str.strip()
     
     
     #LLAVE FX
     df_fx_rate['fk_YearMonthCountryCrncy']=(df_fx_rate['Year']+'-'+
                                        df_fx_rate['Month']+'-'+
                                        df_fx_rate['Country']+'-'+
                                        df_fx_rate['Currency'])
     df_fx_rate['fk_YearMonthCountryCrncy']=df_fx_rate['fk_YearMonthCountryCrncy'].str.upper().str.strip()
     
     #CRUCE PARA OBTENER FX CY
     df_consolidated=pd.merge(
          df_consolidated,
          df_fx_rate[['fk_YearMonthCountryCrncy','Adjusted Rate']],
          on='fk_YearMonthCountryCrncy',
          how='left')
     df_consolidated.rename(columns={'Adjusted Rate':'Adjusted Rate_CY'},inplace=True)
     print('fx cy')
     #CRUCE PARA OBTENER FX PY
     df_consolidated = pd.merge(
            df_consolidated,
            df_fx_rate[['fk_YearMonthCountryCrncy', 'Adjusted Rate']],
            left_on='fk_YearMonthCountryCrncy_PY',  
            right_on='fk_YearMonthCountryCrncy',    
            how='left'
        )
     print('fx py')
     df_consolidated.rename(columns={'Adjusted Rate':'Adjusted Rate_PY'},inplace=True)

     df_consolidated['Adjusted Rate_PY'] = df_consolidated['Adjusted Rate_PY'].fillna(0).infer_objects(copy=False).astype('float32')
     df_consolidated['Adjusted Rate_CY'] = df_consolidated['Adjusted Rate_CY'].fillna(0).infer_objects(copy=False).astype('float32')

     print('ok')
     df_consolidated['FORECAST_VALUE_GSV_LC'] = df_consolidated['FORECAST_VALUE_GSV_LC'].fillna(0).astype('float32')


     df_consolidated['FX_NSV_LC']=(df_consolidated['FORECAST_VALUE_GSV_LC']/df_consolidated['Adjusted Rate_CY'])-(df_consolidated['FORECAST_VALUE_GSV_LC']/df_consolidated['Adjusted Rate_PY'])
     print('ok sum')
     cols_to_drop = ['fk_YearMonthCountryCrncy','fk_YearMonthCountryCrncy_PY']
     df_consolidated.drop(columns=[col for col in cols_to_drop if col in df_consolidated.columns], inplace=True)
     return df_consolidated    

import numpy as np

def sodimac(df_consolidated):
    """Clasifica los registros según el grupo de demanda de Sodimac."""
    
    # 
    lst_sodimac = {'ARSODIMAC', 'CHSODIMAC', 'COSODIMAC', 'PESODIMAC'}
    
    # Creamos la máscara booleana
    mask_sodimac = df_consolidated['Demand Group'].isin(lst_sodimac)
    
    # Asignamos los valores sin modificar el DataFrame original por accidente
    df_consolidated = df_consolidated.copy()
    df_consolidated['Sodimac'] = np.where(mask_sodimac, 'Sodimac', 'Others')
    
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


    """
    Calcula las Ventas Netas ajustadas por el factor 'Combo %' (asociado a NPI/promociones).
    
    El cruce se realiza a nivel de la clave compuesta: Año Fiscal, País y SKU.

    Args:
        df_processed (pd.DataFrame): DataFrame principal de ventas/demanda. Debe contener 
                                     'fk_year_month', 'fk_Country', 'fk_SKU' y 'NSV'.
        df_filter_npi (pd.DataFrame): Tabla de referencia NPI/Combo, conteniendo 
                                      'fk_YearCountrySku' y 'Combo %'.

    Returns:
        pd.DataFrame: El DataFrame modificado con la columna calculada 
                      'Net Sales NPI w/Combo'.
    """
    
    #---RATES
    df_fx_rate['Country']=df_fx_rate['Country'].str.lower().str.strip()
    df_fx_rate['Country'] = df_fx_rate['Country'].replace({'brasil': 'brazil'})
    
    df_fx_rate['fk_YearMontsCountry']=df_fx_rate['Date'].astype(str).str[:6] + '-' + df_fx_rate['Country']
    df_processed['fk_YearMontsCountry']=df_processed['fk_year_month']+'-'+df_processed['fk_Country']

    df_fx_rate['fk_YearMontsCountry']=df_fx_rate['fk_YearMontsCountry'].str.lower().str.strip()
    df_processed['fk_YearMontsCountry']=df_processed['fk_YearMontsCountry'].str.lower().str.strip()
    df_fx_rate=df_fx_rate.drop_duplicates('fk_YearMontsCountry')

    df_processed=pd.merge(
         df_processed,
         df_fx_rate[['fk_YearMontsCountry','OP Rate','Adjusted Rate']],
         on='fk_YearMontsCountry',
         how='left'
    )
    df_processed['OP Rate']=df_processed['OP Rate'].fillna('1')
    df_processed['Adjusted Rate']=df_processed['Adjusted Rate'].fillna('1')

    #--GToNET
    df_gross_to_net['fk_YearMontsCountry_g2net']=df_gross_to_net['Date'].astype(str).str[:6]+'-'+df_gross_to_net['Country']
    df_processed['fk_YearMontsCountry_g2net']=df_processed['fk_year_month']+'-'+df_processed['fk_Country'].str[:3]

    df_gross_to_net['fk_YearMontsCountry_g2net']=df_gross_to_net['fk_YearMontsCountry_g2net'].str.lower().str.strip()
    df_processed['fk_YearMontsCountry_g2net']=df_processed['fk_YearMontsCountry_g2net'].str.lower().str.strip()

    df_gross_to_net=df_gross_to_net['fk_YearMontsCountry_g2net'].drop_duplicate('fk_YearMontsCountry_g2net')

    df_processed=pd.merge(
         df_processed,
         df_gross_to_net[['fk_YearMontsCountry_g2net','G2N%']],
         on='fk_YearMontsCountry_g2net',
         how='left'
    )



    df_processed['fk_YearCountrySku']=df_processed['fk_YearCountrySku'].str.upper().str.strip()
    df_filter_npi['fk_YearCountrySku']=df_filter_npi['fk_YearCountrySku'].str.upper().str.strip()
    df_filter_npi = df_filter_npi.drop_duplicates(subset=['fk_YearCountrySku'])
    df_processed=pd.merge(
        df_processed,
        df_filter_npi[['fk_YearCountrySku','Combo %']],
        how='left',
        left_on='fk_YearCountrySku',
        right_on='fk_YearCountrySku'
    )
    df_processed['Combo %'] = pd.to_numeric(df_processed['Combo %'], errors='coerce')
    df_processed['Combo %']=df_processed['Combo %'].fillna(1)
    df_processed['Combo %']=df_processed['Combo %'].astype(float)
    df_processed['NSV']=df_processed['NSV'].fillna(0)
    df_processed['NSV']=df_processed['NSV'].astype(float)
    df_processed['Net Sales NPI w/Combo']=df_processed['NSV']*df_processed['Combo %']
    df_processed['Net Sales NPI w/Combo']=df_processed['Net Sales NPI w/Combo'].fillna(0)
    cols_to_drop = ['fk_YearCountrySku','Combo %']
    df_processed.drop(columns=[col for col in cols_to_drop if col in df_processed.columns], inplace=True)
    return df_processed

