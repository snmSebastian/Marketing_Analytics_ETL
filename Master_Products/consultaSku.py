"""
EL DETECTOR DE IDENTIDADES: CONSULTA ON-DEMAND DE SKUS
-----------------------------------------------------
Este script es la "navaja suiza" para cuando marketing o ventas nos tiran una lista de 
códigos extraños y nos preguntan: "¿Y esto qué es?". En lugar de buscar uno por uno en 
SAP, Snowflake o Exceles viejos, este proceso les pone nombre, apellido y familia de 
forma automática. 

Es básicamente un simulador de clasificación que no ensucia el Maestro principal, 
pero te da todas las respuestas en un solo reporte.

FLUJO DE TRABAJO:
1. Carga de Pedidos: Recibe el archivo de consulta con los SKUs que están en el "limbo".
2. Cruce con la Nube: Escanea Snowflake para traer la jerarquía oficial (GPP) que 
   vive en el sistema global.
3. Rastreo Genético: Busca si el SKU tiene un "hermano mayor" (SKU Base) ya conocido 
   para heredarle sus propiedades y mantener la coherencia.
4. Análisis de ADN Técnico: Desmenuza las descripciones para detectar si el equipo 
   usa batería, qué voltaje tiene y si viene con accesorios o es una herramienta "nuda" (Bare).
5. Reporte de Acción: Escupe un Excel listo para que el analista valide y tome 
   decisiones sin romperse la cabeza.

💡 NOTA DE SENIOR:
Este script consume casi toda su lógica de `column_processing.py`. Si notas que una 
regla de voltaje o marca está fallando aquí, **no la arregles en este archivo**; 
vete directo al módulo core. Si lo arreglas allá, mejoras este buscador y de paso 
todo el pipeline regional. ¡Doble win!

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


import pandas as pd
from typing import List, Union

from Fill_Rate.Process_ETL.Process_Files import asign_country_code, read_files,clean_sku
from Master_Products.column_processing import *

def main():
    """	
    Función principal que orquesta el pipeline de identificación y clasificación de nuevos productos.	
    El flujo incluye:
        1) Consolidación de nuevos SKUs.
        2) Asignación de SKU Base (si aplica).	
        3) Look-up de GPP (por SKU Base o por Portafolio).
        4) Asignación de atributos (Corded/Cordless, Voltaje, Bare).	
        5) Generación de un archivo de revisión Excel (WORKFILE_NEW_PRODUCTS_REVIEW_FILE) que incluye nuevos SKUs y SKUs Base con clasificaciones inconsistentes.	
    Returns: None: La función orquesta el proceso y no devuelve un valor,
                   guardando el resultado en un archivo Excel        
    """
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: CONSULTA  SKU ---")
    print("=" * 55)
    #-------------------------------
    #---- RUTAS DE LOS ARCHIVOS
    #-------------------------------
    from config_paths import MasterProductsPaths
    #path_fill_rate_update=MasterProductsPaths.INPUT_RAW_UPDATE_FILL_RATE_DIR
    #path_sales_update=MasterProductsPaths.INPUT_RAW_UPDATE_SALES_DIR
    #path_demand_update=MasterProductsPaths.INPUT_RAW_UPDATE_DEMAND_DIR
    path_ConsultaSku=MasterProductsPaths.INPUT_RAW_ConsultaSKU_FILE

    path_producst_hts=MasterProductsPaths.WORKFILE_HTS_FILE
    path_producst_pwt=MasterProductsPaths.WORKFILE_PWT_FILE
    
    path_New_Products=MasterProductsPaths.WORKFILE_NEW_PRODUCTS_REVIEW_FILE    
    path_gpp=MasterProductsPaths.INPUT_PROCESSED_GPP_BRAND_FILE
    path_psd=MasterProductsPaths.INPUT_RAW_SHARED_PSD_FILE
    path_sku_snowflake=MasterProductsPaths.INPUT_RAW_SkuName_FILE
    path_proyects=MasterProductsPaths.INPUT_PROCESSED_PROYECTS_FILE
    path_result_ConsultSku=MasterProductsPaths.INPUT_RAW_Result_ConsultaSKU_FILE

    #path_master_products=MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE_PRUEBA
    path_master_products=MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE
    
    #-----------------------------
    #----  Cargo dataframes
    #-----------------------------
    #df_fill_rate=read_files(path_fill_rate_update)
    #df_sales=read_files(path_sales_update)
    #df_demand=read_files(path_demand_update)
    df_consultaSku=pd.read_excel(path_ConsultaSku, dtype=str, engine='openpyxl')
    

    df_master_products=pd.read_excel(path_master_products, dtype=str, engine='openpyxl')
    df_new_products=pd.read_excel(path_New_Products, dtype=str, engine='openpyxl')

    df_gpp=pd.read_excel(path_gpp, dtype=str, engine='openpyxl',sheet_name='GPP')
    df_brand=pd.read_excel(path_gpp, dtype=str, engine='openpyxl',sheet_name='Brand')
    df_psd=pd.read_excel(path_psd, dtype=str, engine='openpyxl')
    df_snowflake=pd.read_parquet(path_sku_snowflake, engine='pyarrow')
    df_proyects=pd.read_excel(path_proyects, dtype=str, engine='openpyxl',sheet_name='Proyects')
    df_dewaltXR=pd.read_excel(path_proyects, dtype=str, engine='openpyxl',sheet_name='DW_XR')

    #================
    # limpieza sku
    #=================
    df_consultaSku=clean_sku(df_consultaSku,'SKU')
    df_master_products=clean_sku(df_master_products,'SKU')
    df_new_products=clean_sku(df_new_products,'SKU')
    df_psd=clean_sku(df_psd,'SKU')
    df_snowflake=clean_sku(df_snowflake,'SKU')
    df_proyects=clean_sku(df_proyects,'SKU')
    df_dewaltXR=clean_sku(df_dewaltXR,'SKU')
    
    #---------------------------------------------------
    #--- Genero el archivo con los nuevos productos
    #----------------------------------------------------
    df_new_products= df_consultaSku.copy()
    
    lst_colums_gpp=['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP', 'GPP SBU',
       'GPP SBU Description', 'SBU Type', 'GPP Division Code',
       'GPP Division Description', 'GPP Category Code',
       'GPP Category Description', 'GPP Portfolio Code',
       'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
       'Voltaje', 'Bare', 'Sub-Brand','Project Name','Dewalt XR','origen_sku','check_sku']
    
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


    # Genero dos dataframes, uno con sku base y otro sin sku base
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
    'Voltaje', 'Bare', 'Sub-Brand','origen_sku','¿como se asigno gpp?','check_sku']
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


    # ---------------------------------------------------------------------------------------
    # Tratamiento de columnas corded / cordless, qyt batteries, voltaje,bare,sun brand
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
    #Asigno la sub-marca a los nuevos productos
    df_new_products_gpp['Sub-Brand'] = df_new_products_gpp.apply(
        lambda row: assign_sub_brand(row['SKU'], row['SKU Description'], row['Brand']), axis=1)
    # Asigno Proyects y Dewalt XR
    #df_new_products_gpp=assign_proyects_xr(df_new_products_gpp, df_proyects, df_dewaltXR)
    

    # Extraigo los sku base que tienen diferente sbu-category para su revision
    #df_sku_base_review=review_sku_base_with_diferent_category(df_master_products,lst_colums_gpp)
    
    # Creo el dataframe que contiene tanto los nuevos sku como los sku a revisar
    df_review_products=pd.concat([df_new_products_gpp], ignore_index=True)
    

    # Exporto a excel el dataframe de nuevos productos
    # path_result_ConsultSku=r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Products\other\ResultConsultSku.xlsx'
    df_review_products.to_excel(path_result_ConsultSku, index=False)

if __name__ == "__main__":
    main()
    print("Proceso de consulta de productos completado exitosamente.")