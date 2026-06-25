"""
LA ADUANA DE PRODUCTOS: EL FILTRO PRE-MAESTRO
-------------------------------------------
Este script es el corazón de la integridad de datos. Su misión es detectar "intrusos": 
SKUs que aparecen en las ventas o la demanda pero que nadie conoce en el Maestro. 
En lugar de dejar que rompan los reportes de Power BI, los atrapa, les crea un perfil 
técnico sugerido y se los pasa al analista en un Excel para que les dé el visto bueno.

Sin este proceso, las jerarquías de marca y categoría serían un caos total.

FLUJO DE TRABAJO:
1. Gran Recolección: Consolida archivos Parquet y Excel de Sales, Demand y Fill Rate 
   para ver qué se está moviendo en la región.
2. Cacería de SKUs: Compara contra el Maestro actual para identificar qué códigos 
   son nuevos "extranjeros".
3. Perfilamiento Genético: Usa el motor de 'column_processing' para heredar datos de 
   SKU Base, consultar Snowflake y extraer voltaje/baterías de las descripciones.
4. Auditoría de Consistencia: Detecta si un SKU Base viejo se está queriendo pasar 
   de listo con categorías o SBUs distintas a las originales.
5. Entrega del Workfile: Escupe el archivo de revisión que el equipo de Master Data 
   usa para validar antes de la carga final.

💡 NOTA DE SENIOR:
Ojo con la función `consolidar_parquets`; si la carpeta tiene muchísimos archivos, 
el consumo de RAM puede subir rápido. Este script es un orquestador, así que si 
quieres cambiar *cómo* se calcula el voltaje o la marca, el lugar correcto es 
meterle mano a `column_processing.py`, no aquí.

Asigna informacion primero tomando de datalake luego por sku base
"""

#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
# Liberia
import pandas as pd
import numpy as np
# Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico.
import glob
import os
import sys

import pandas as pd
from pathlib import Path

from typing import List, Union

# Importación de Funciones de Transformación y Reutilización
# Se importan las funciones de procesamiento de datos compartidas (read_files)
# y la lógica de clasificación de productos (Master_Products/column_processing).
from Fill_Rate.Process_ETL.Process_Files import  read_files,clean_sku
from Master_Products.column_processing import (obtain_new_products, assign_sku_base, assign_info_by_key,
                              assign_gpp_by_portafolio, verify_psd, verify_gpp,
                              corded_or_cordless_or_gas, assing_qty_batteries, assing_voltaje,
                              assign_bare, assign_sub_brand, review_sku_base_with_diferent_category)
def consolidar_parquets(carpeta_path):
    """
    Escanea una carpeta, busca todos los archivos .parquet y los une en un solo gran DataFrame.
    
    Es el paso final para "pegar" todas las piezas del rompecabezas que se procesaron 
    por separado. Si no encuentra archivos, te avisa por consola en lugar de 
    lanzar un error que detenga todo el script. 
    
    Ojo: Asegúrate de tener suficiente RAM si la carpeta está muy pesada, ya que 
    carga todo a la vez antes de concatenar."""
    # Definir la ruta de la carpeta
    ruta = Path(carpeta_path)
    
    # Obtener lista de todos los archivos .parquet
    archivos = list(ruta.glob("*.parquet"))
    
    if not archivos:
        print(f"No se encontraron archivos en {carpeta_path}")
        return None

    print(f"Consolidando {len(archivos)} archivos...")

    # Leer cada archivo y guardarlo en una lista
    # Nota: Usamos copy() para asegurar que cada DF sea independiente
    lista_df = [pd.read_parquet(f) for f in archivos]
    
    # Concatenar todos en un solo DataFrame
    df_final = pd.concat(lista_df, ignore_index=True)
    print('Se consolidaron los archivos parquets')
    return df_final

def main():
    """	
    Orquesta el pipeline completo para identificar, clasificar y generar la lista de SKUs a revisar.	
        1. Carga los datos de actualización y los maestros de referencia.	
        2. Llama a las funciones de procesamiento para asignar SKU Base, clasificaciones GPP y atributos.	
        3. Identifica SKUs Base inconsistentes del maestro histórico.	
        4. Consolida y exporta el resultado final (nuevos SKUs + SKUs inconsistentes) al archivo de trabajo Excel.	
    Returns: None: La función orquesta el proceso y guarda el resultado en un archivo Excel (Workfile).
    """
    print("=" * 55)
    print("---  INICIANDO PROCESO: PRODUCTS REVIEW UPDATE ETL ---")
    print("=" * 55)
    #-------------------------------
    #---- RUTAS DE LOS ARCHIVOS
    #-------------------------------
    try:
        from config_paths import MasterProductsPaths
        path_fill_rate_update=MasterProductsPaths.INPUT_RAW_UPDATE_FILL_RATE_DIR
        path_sales_update=MasterProductsPaths.INPUT_RAW_UPDATE_SALES_DIR
        path_demand_update=MasterProductsPaths.INPUT_RAW_UPDATE_DEMAND_DIR
                
        path_New_Products=MasterProductsPaths.WORKFILE_NEW_PRODUCTS_REVIEW_FILE    
        path_gpp=MasterProductsPaths.INPUT_PROCESSED_GPP_BRAND_FILE
        path_psd=MasterProductsPaths.INPUT_RAW_SHARED_PSD_FILE
        path_proyects=MasterProductsPaths.INPUT_PROCESSED_PROYECTS_FILE
        path_sku_snowflake=MasterProductsPaths.INPUT_RAW_SkuName_FILE


        #path_master_products=MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE_PRUEBA
        path_master_products=MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE
        
        #-----------------------------
        #----  Cargo dataframes
        #-----------------------------
        df_fill_rate=read_files(path_fill_rate_update)
        df_sales=consolidar_parquets(path_sales_update)
        df_demand=consolidar_parquets(path_demand_update)

        df_master_products=pd.read_excel(path_master_products, dtype=str, engine='openpyxl')
        df_sku_review=pd.read_excel(path_New_Products, dtype=str, engine='openpyxl')

        df_gpp=pd.read_excel(path_gpp, dtype=str, engine='openpyxl',sheet_name='GPP')
        df_brand=pd.read_excel(path_gpp, dtype=str, engine='openpyxl',sheet_name='Brand')
        df_psd=pd.read_excel(path_psd, dtype=str, engine='openpyxl')
        df_snowflake=pd.read_parquet(path_sku_snowflake, engine='pyarrow')


        #-------------------------
        # Limpieza SKU
        #-------------------------
        df_fill_rate=clean_sku(df_fill_rate,'Country Material')
        df_sales=clean_sku(df_sales,'fk_SKU')
        df_demand=clean_sku(df_demand,'Global Material')
        df_master_products=clean_sku(df_master_products,'SKU')
        df_sku_review=clean_sku(df_sku_review,'SKU')
        df_psd=clean_sku(df_psd,'SKU')
        df_snowflake=clean_sku(df_snowflake,'SKU')


      
        #---------------------------------------------------
        #--- Genero el archivo con los nuevos productos
        #----------------------------------------------------
        df_new_products= obtain_new_products(df_fill_rate, df_sales, df_demand, df_sku_review,df_master_products)
        # Filtro robusto: elimina valores nulos reales (NaN/None) y strings vacíos
        df_new_products = df_new_products[df_new_products['SKU'].notna()]
        df_new_products = df_new_products[df_new_products['SKU'].astype(str).str.upper() != 'NONE']

        lst_colums_gpp=['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP', 'GPP SBU',
        'GPP SBU Description', 'SBU Type', 'GPP Division Code',
        'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code',
        'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
        'Voltaje', 'Bare', 'origen_sku','check_sku']
        
        columnas_a_crear=[col for col in lst_colums_gpp if col not in df_new_products.columns]
        for col in columnas_a_crear:
            df_new_products[col] = pd.Series(np.nan, index=df_new_products.index, dtype='object')
        df_new_products = df_new_products[lst_colums_gpp]
        df_new_products['origen_sku'] = 'new sku'

        
        #----------------------------------------------------
        #---- Procesamiento de los nuevos productos
        #----------------------------------------------------

        # Asigno el sku base a los nuevos productos    
        sku_base_set = set(df_master_products['SKU Base'].dropna().unique())
        df_new_products['SKU Base'] = df_new_products['SKU'].apply(lambda x: assign_sku_base(x, sku_base_set))
        
        #Asigno el gpp del datalake para los nuevos productos
        key_column = ['SKU']
        columns_merge = ['SKU Description', 'Brand','GPP SBU',
            'GPP Division Code',
            'GPP Category Code',
            'GPP Portfolio Code'
        ]
        df_new_products.loc[:, columns_merge] = np.nan
        df_new_products = assign_info_by_key(
            df_new_products, 
            df_snowflake, 
            key_column, 
            columns_merge
        )
        df_new_products['GPP']=df_new_products['GPP SBU']+'-'+df_new_products['GPP Division Code']+'-'+df_new_products['GPP Category Code']+'-'+df_new_products['GPP Portfolio Code']

        condicion = (df_new_products['GPP'] == '---') | (df_new_products['GPP'].str.len() < 6) | (df_new_products['GPP'].isna())
        df_new_products.loc[condicion, 'GPP'] = np.nan  # Usa np.nan para que pandas lo trate como nulo real


        # Genero tres dataframes,
        #  1) sku que tienen sku base y estan en el datalake por tanto se les pudo asignar gpp 
        # 2 ) sku que tienen sku base y NO ESTAN en el datalake por tanto NO se les pudo asignar gpp
        # 3) NO tiene sku base y NO esta en el datalake 
        df_new_products_asigned=df_new_products[(~ df_new_products['GPP'].isna())].copy()
    
        df_new_products_con_base = df_new_products[(df_new_products['SKU Base'] != '-') & 
                                                (df_new_products['GPP'].isna())].copy()
        df_new_products_sin_base = df_new_products[(df_new_products['SKU Base'] == '-') & 
                                                (df_new_products['GPP'].isna())].copy()

        #...................................................................
        #----- Procesamiento para los nuevos productos con sku base
        #...................................................................

        #Asigno el gpp para los nuevos productos con sku base
        key_column = ['SKU Base']
        columns_merge = [
            'Brand', 'GPP'
        ]
        df_new_products_con_base.loc[:, columns_merge] = np.nan
        df_new_products_con_base = assign_info_by_key(
            df_new_products_con_base, 
            df_master_products, 
            key_column, 
            columns_merge
        )
    
        # Concateno los productos con los gpp asignados bien sea por sku base o lo que dice snowflake
        df_new_products_asigned['¿como se asigno gpp?']='snowflake'
        df_new_products_con_base['¿como se asigno gpp?'] = 'sku base'
        df_new_products_sin_base['¿como se asigno gpp?'] = '-'
        lst_colums_gpp=['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP', 'GPP SBU',
        'GPP SBU Description', 'SBU Type', 'GPP Division Code',
        'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code',
        'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
        'Voltaje', 'Bare', 'origen_sku','¿como se asigno gpp?','check_sku']

        df_new_products_asigned=df_new_products_asigned[lst_colums_gpp].copy()
        df_new_products_con_base=df_new_products_con_base[lst_colums_gpp].copy()
        df_new_products_sin_base=df_new_products_sin_base[lst_colums_gpp].copy()
        

        df_products_with_gpp=pd.concat([df_new_products_asigned,df_new_products_con_base, df_new_products_sin_base], ignore_index=True)
        df_products_with_gpp = df_products_with_gpp.fillna(value='-')
        lst_psd=list(df_psd['SKU'])

        # Verifico si los sku que siguen sin gpp son psd
        df_products_with_gpp['GPP'] = np.where(
            ((df_products_with_gpp['GPP'] == 'nan') |
            (df_products_with_gpp['GPP'] == '-') |
            (df_products_with_gpp['GPP'].str.startswith('PSD',na=False))) & 
            (df_products_with_gpp['SKU'].isin(lst_psd)),
            "PSD-70-70X-70999",
            df_products_with_gpp['GPP']
        )

        df_products_with_gpp['GPP'] = np.where(
            (df_products_with_gpp['GPP'].str.startswith('PSD',na=False)), 
            "PSD-70-70X-70999",
            df_products_with_gpp['GPP']
        )
        
        # Asigno la notacion existe en la tabla principal de clasificaciones, segun el gpp asignado
        columns_merge_sku_sin_info = [ 'GPP SBU', 'GPP SBU Description', 'SBU Type', 
            'GPP Division Code', 'GPP Division Description', 'GPP Category Code',
            'GPP Category Description', 'GPP Portfolio Code', 
            'GPP Portfolio Description']
        df_products_with_gpp.loc[:,columns_merge_sku_sin_info]= np.nan  # Inicializo las columnas a NaN
        key_column = ['GPP']
        df_products_with_gpp= assign_info_by_key(
            df_products_with_gpp, 
            df_gpp, 
            key_column, 
            columns_merge_sku_sin_info
        )

        df_new_products_gpp=df_products_with_gpp.copy()
        df_new_products_gpp = df_new_products_gpp.fillna("").astype(str)
        df_new_products_gpp = df_new_products_gpp.fillna(value='-')
        
        
        # ---------------------------------------------------------------------------------------
        # Tratamiento de columnas corded / cordless, qyt batteries, voltaje,bare,sub-brand
        # ---------------------------------------------------------------------------------------
        #Asigno el corded o cordless a los nuevos productos
        df_new_products_gpp['Corded / Cordless'] = df_new_products_gpp.apply(
            lambda row: corded_or_cordless_or_gas(row['SKU'], row['SKU Description'], row['GPP Category Description'], row['GPP Portfolio Description'],
                                            row['Corded / Cordless']), axis=1)
        
        # Asigno la cantidad de baterías a los nuevos productos
        df_new_products_gpp['Batteries Qty'] = df_new_products_gpp.apply(
            lambda row: assing_qty_batteries(row['SKU']), axis=1)
        # Asigno el voltaje a los nuevos productos
        df_new_products_gpp['Voltaje'] = df_new_products_gpp.apply(
            lambda row: assing_voltaje(row['SKU Description']), axis=1)
        
        # Asigno el valor de Bare a los nuevos productos
        df_new_products_gpp['Bare'] = df_new_products_gpp.apply(
            lambda row: assign_bare(row['SKU'], row['Batteries Qty'], row['Corded / Cordless']), axis=1)
        


        lst_colums_gpp_final=['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP', 'GPP SBU',
        'GPP SBU Description', 'SBU Type', 'GPP Division Code',
        'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code',
        'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
        'Voltaje', 'Bare','origen_sku','¿como se asigno gpp?','check_sku']
    
        df_new_products_gpp = df_new_products_gpp[lst_colums_gpp_final]
        #------------------------------------------------------------------
        # ---- SKU que deben ser revisados
        #------------------------------------------------------------------
        
        # Extraigo los sku base que tienen diferente sbu-category para su revision
        df_sku_base_review=review_sku_base_with_diferent_category(df_master_products,lst_colums_gpp)
        # Creo el dataframe que contiene tanto los nuevos sku como los sku a revisar
        df_review_products=pd.concat([df_new_products_gpp,df_sku_base_review], ignore_index=True)
        
        # Exporto a excel el dataframe de nuevos productos
        df_review_products.to_excel(path_New_Products, index=False)
        print("Proceso de generacion archivo de sku por revisar completado exitosamente.")     
        pass
    except Exception as e:
        print(f"Error en la generacion del archivo con nuevos sku review: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
    
