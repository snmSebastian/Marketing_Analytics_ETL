"""
Módulo de Orquestación y Ejecución para la generación del Archivo de Revisión de Productos.
Este script centraliza el flujo ETL (llamando a las funciones de column_processing.py)
para identificar nuevos SKUs, asignar clasificaciones GPP, atributos (Corded/Cordless, Voltaje),
y generar un archivo de trabajo (WORKFILE_NEW_PRODUCTS_REVIEW_FILE) que requiere
la validación manual del analista.
"""

#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
# Liberia
import pandas as pd
import numpy as np
# Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico.
import glob
import os

import pandas as pd
from typing import List, Union

# Importación de Funciones de Transformación y Reutilización
# Se importan las funciones de procesamiento de datos compartidas (read_files)
# y la lógica de clasificación de productos (Master_Products/column_processing).
from Fill_Rate.Process_ETL.Process_Files import asign_country_code, read_files
from Master_Products.column_processing import (obtain_new_products, assign_sku_base, assign_info_by_key,
                              assign_gpp_by_portafolio, verify_psd, verify_gpp,
                              corded_or_cordless_or_gas, assing_qty_batteries, assing_voltaje,
                              assign_bare, assign_sub_brand, review_sku_base_with_diferent_category)


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
    print("--- 🔄 INICIANDO PROCESO: PRODUCTS REVIEW UPDATE ETL ---")
    print("=" * 55)
    #-------------------------------
    #---- RUTAS DE LOS ARCHIVOS
    #-------------------------------
    from config_paths import MasterProductsPaths
    path_fill_rate_update=MasterProductsPaths.INPUT_RAW_UPDATE_FILL_RATE_DIR
    path_sales_update=MasterProductsPaths.INPUT_RAW_UPDATE_SALES_DIR
    path_demand_update=MasterProductsPaths.INPUT_RAW_UPDATE_DEMAND_DIR
    
    path_producst_hts=MasterProductsPaths.WORKFILE_HTS_FILE
    path_producst_pwt=MasterProductsPaths.WORKFILE_PWT_FILE
    
    path_New_Products=MasterProductsPaths.WORKFILE_NEW_PRODUCTS_REVIEW_FILE    
    path_gpp=MasterProductsPaths.INPUT_PROCESSED_GPP_BRAND_FILE
    path_psd=MasterProductsPaths.INPUT_RAW_SHARED_PSD_FILE
   
    #path_master_products=MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE_PRUEBA
    path_master_products=MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE
    
    #-----------------------------
    #----  Cargo dataframes
    #-----------------------------
    df_fill_rate=read_files(path_fill_rate_update)
    df_sales=read_files(path_sales_update)
    df_demand=read_files(path_demand_update)

    df_master_products=pd.read_excel(path_master_products, dtype=str, engine='openpyxl')
    df_new_products=pd.read_excel(path_New_Products, dtype=str, engine='openpyxl')

    df_gpp=pd.read_excel(path_gpp, dtype=str, engine='openpyxl',sheet_name='GPP')
    df_brand=pd.read_excel(path_gpp, dtype=str, engine='openpyxl',sheet_name='Brand')
    df_psd=pd.read_excel(path_psd, dtype=str, engine='openpyxl')
    
    #---------------------------------------------------
    #--- Genero el archivo con los nuevos productos
    #----------------------------------------------------
    df_new_products= obtain_new_products(df_fill_rate, df_sales, df_demand, df_new_products,df_master_products)
    
    #----------------------------------------------------
    #---- Procesamiento de los nuevos productos
    #----------------------------------------------------

    # Asigno el sku base a los nuevos productos    
    sku_base_set = set(df_master_products['SKU Base'].dropna().unique())
    df_new_products['SKU Base'] = df_new_products['SKU'].apply(lambda x: assign_sku_base(x, sku_base_set))
    
    # Genero dos dataframes, uno con sku base y otro sin sku base
    df_new_products_con_base = df_new_products[df_new_products['SKU Base'] != '-'].copy()
    df_new_products_sin_base = df_new_products[df_new_products['SKU Base'] == '-'].copy() 

    #...................................................................
    #----- Procesamiento para los nuevos productos con sku base
    #...................................................................

    #Asigno el gpp para los nuevos productos con sku base
    key_column = ['SKU Base']
    columns_merge = [
        'Brand', 'GPP', 'GPP SBU', 'GPP SBU Description', 'SBU Type', 
        'GPP Division Code', 'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code', 
        'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
        'Voltaje', 'Bare'
    ]
    df_new_products_con_base.loc[:, columns_merge] = np.nan
    df_new_products_con_base = assign_info_by_key(
        df_new_products_con_base, 
        df_master_products, 
        key_column, 
        columns_merge
    )

    #...................................................................
    #----- Procesamiento para los nuevos productos SIN sku base
    #...................................................................

    # Asigno el gpp por medio del portafolio para los nuevos productos sin sku base
    df_gpp['fk_GPP_Portfolio'] = df_gpp['GPP Portfolio Description'].str.strip().str.upper().str.replace(' ', '') 
    df_new_products_sin_base['GPP'] = df_new_products_sin_base['GPP Portfolio Description'].apply(
        lambda x: assign_gpp_by_portafolio(x, df_gpp['fk_GPP_Portfolio'].unique(), df_gpp))
    
    # Asigno "-" a los gpp que no existen
    lst_gpp=(
    pd.Series(df_gpp['GPP'].dropna().unique())  # 1. Convertir el arreglo de NumPy de vuelta a Serie
    .str.strip()
    .str.upper()
    .str.replace(' ', '')
    .tolist()
    )
    df_new_products_sin_base['GPP'] = df_new_products_sin_base['GPP'].apply(
        lambda x: verify_gpp(x, lst_gpp))
    
    #-------------- Verifico si los sku que aun no tienen gpp, estan en la base compartida por PSD    
    df_posibble_psd = df_new_products_sin_base[df_new_products_sin_base['GPP'] == '-'].copy()
    # elimino los sku que no tienen gpp para no genrar duplicados al concatenar
    df_new_products_sin_base = df_new_products_sin_base[df_new_products_sin_base['GPP'] != '-']
    
    columns_merge_sku_sin_base = [ 'GPP SBU', 'GPP SBU Description', 'SBU Type', 
        'GPP Division Code', 'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code', 
        'GPP Portfolio Description']
    df_new_products_sin_base.loc[:,columns_merge_sku_sin_base]= np.nan  # Inicializo las columnas a NaN
    key_column = ['GPP']
    df_new_products_sin_base = assign_info_by_key(
        df_new_products_sin_base, 
        df_gpp, 
        key_column, 
        columns_merge_sku_sin_base
    )


    lst_psd = (
    pd.Series(df_psd['SKU'].dropna().unique())  # 1. Convertir el arreglo de NumPy de vuelta a Serie
    .str.strip()
    .str.upper()
    .str.replace(' ', '')
    .tolist()
    )
    
    df_posibble_psd['GPP']=df_posibble_psd['SKU'].apply(
        lambda x: verify_psd(x, lst_psd))
    # Asigno toda la informacion de clasificacion tomadno en cuenta el gpp de PSD
    key_column = ['GPP']
    columns_merge = [
        'GPP SBU', 'GPP SBU Description', 'SBU Type', 
        'GPP Division Code', 'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code', 
        'GPP Portfolio Description']
    df_posibble_psd.loc[:,columns_merge] = np.nan
    df_posibble_psd = assign_info_by_key(
        df_posibble_psd, 
        df_gpp, 
        key_column, 
        columns_merge
    )

    # -------------------------------------------------------------------------
    # --- Genero dataframe con los nuevos sku 
    # -------------------------------------------------------------------------
    # Asigno ¿como se asigno gpp? a los dataframes
    df_new_products_con_base['¿como se asigno gpp?'] = 'sku base'
    df_new_products_sin_base['¿como se asigno gpp?'] = 'por portafolio dado por SAP'
    df_posibble_psd['¿como se asigno gpp?'] = 'sku esta en la base compartida por PSD'
    # Ordeno las columnas de los dataframes
    lst_colums_gpp=['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP', 'GPP SBU',
    'GPP SBU Description', 'SBU Type', 'GPP Division Code',
    'GPP Division Description', 'GPP Category Code',
    'GPP Category Description', 'GPP Portfolio Code',
    'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
    'Voltaje', 'Bare', 'Sub-Brand','origen_sku','¿como se asigno gpp?','check_sku']
    df_new_products_con_base = df_new_products_con_base[lst_colums_gpp]
    df_new_products_sin_base = df_new_products_sin_base[lst_colums_gpp]
    df_posibble_psd = df_posibble_psd[lst_colums_gpp]
    # Genero el nuevo dataframe con los nuevos productos a partir de los dataframes con y sin sku base
    df_new_products_gpp=pd.concat([df_new_products_con_base, df_new_products_sin_base,df_posibble_psd], ignore_index=True)    
    
    # Tratamiento de los espacios en blanco y NaN
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
        lambda row: assing_qty_batteries(row['SKU'], row['SKU Description'], row['Batteries Qty']), axis=1)
    
    # Asigno el voltaje a los nuevos productos
    df_new_products_gpp['Voltaje'] = df_new_products_gpp.apply(
        lambda row: assing_voltaje(row['SKU Description'], row['Voltaje']), axis=1)
    
    # Asigno el valor de Bare a los nuevos productos
    df_new_products_gpp['Bare'] = df_new_products_gpp.apply(
        lambda row: assign_bare(row['SKU'], row['Batteries Qty'], row['Corded / Cordless']), axis=1)
    #Asigno la sub-marca a los nuevos productos
    df_new_products_gpp['Sub-Brand'] = df_new_products_gpp.apply(
        lambda row: assign_sub_brand(row['SKU'], row['SKU Description'], row['Brand']), axis=1)

    #------------------------------------------------------------------
    # ---- SKU que deben ser revisados
    #------------------------------------------------------------------
    
    # Extraigo los sku base que tienen diferente sbu-category para su revision
    df_sku_base_review=review_sku_base_with_diferent_category(df_master_products,lst_colums_gpp)
    
    # Creo el dataframe que contiene tanto los nuevos sku como los sku a revisar
    df_review_products=pd.concat([df_new_products_gpp,df_sku_base_review], ignore_index=True)
    
    # Exporto a excel el dataframe de nuevos productos
    df_review_products.to_excel(path_New_Products, index=False)

if __name__ == "__main__":
    main()
    print("Proceso de actualización de productos completado exitosamente.")