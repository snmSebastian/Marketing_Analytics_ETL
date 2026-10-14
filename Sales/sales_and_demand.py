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
        from config_paths import MasterProductsPaths,FillRatePaths,SalesPaths,DemandPaths,SodimacPaths,Datamind_PATHS
        path_fill_rate_historic=FillRatePaths.OUTPUT_PROCESSED_PARQUETS_DIR
        path_sales_historic=SalesPaths.OUTPUT_PROCESSED_PARQUETS_DIR
        path_demand_update=DemandPaths.OUTPUT_SNAPSHOP_PROCESSED_PARQUETS_DIR
        path_sodimac=SodimacPaths.OUTPUT_PROCESSED_PARQUETS_DIR_EDAS
        path_datamind=Datamind_PATHS.OUTPUT_PROCESSED_PARQUETS_DIR
        path_md_products=MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE
        country_code_file = SalesPaths.INPUT_PROCESSED_COUNTRY_CODES_FILE

                
        path_New_Products=MasterProductsPaths.INPUT_RAW_ConsultaSKU_FILE    
        #-----------------------------
        #----  Cargo dataframes
        #-----------------------------
        df_country=pd.read_excel(country_code_file,sheet_name='Region')
        df_sales=consolidar_parquets(path_sales_historic)
        df_sales=df_sales[df_sales['fk_year_month']!='2026-07']
        df_demand=consolidar_parquets(path_demand_update)
        df_md_product=pd.read_excel(path_md_products,dtype=str)

        #-------------------------
        # Limpieza SKU
        #-------------------------
        df_sales=clean_sku(df_sales,'fk_SKU')
        print('2')
        df_demand=clean_sku(df_demand,'fk_SKU')

        lst_cols=['fk_year_month','fk_Country','fk_SKU','NSV']
        # Corrección: Agrupamos las tablas dentro de una lista [ ] en el primer argumento
        df_all_sku = pd.concat([df_sales[lst_cols], df_demand[lst_cols]], ignore_index=True)

        df_all_sku['fk_SKU']=df_all_sku['fk_SKU'].str.lower().str.strip()
        df_md_product['SKU']=df_md_product['SKU'].str.lower().str.strip()
        print('concat')
        df_sales_demand=pd.merge(
            df_all_sku,
            df_md_product[['SKU','Brand','GPP SBU']],
            left_on='fk_SKU',
            right_on='SKU',
            how='left'
        )
        print('merge sbu')
        print(df_sales_demand.head())

        df_sales_demand['fk_Country']=df_sales_demand['fk_Country'].str.lower().str.strip()
        df_country['Country']=df_country['Country'].str.lower().str.strip()
        df_country['Region']=df_country['Region'].str.lower().str.strip()

        df_sales_demand=pd.merge(
            df_sales_demand,
            df_country[['Country','Region']],
            left_on='fk_Country',
            right_on='Country',
            how='left'
        )
        print('merge country')
        print(df_sales_demand.head())
        df_sales_demand = df_sales_demand[['fk_year_month', 'Region','fk_SKU', 'Brand', 'GPP SBU', 'NSV']]
        print(df_sales_demand.head())
        df_sales_demand_grouped = df_sales_demand.groupby(['fk_year_month', 'Region', 'Brand', 'GPP SBU'])['NSV'].sum().reset_index()
        print(df_sales_demand.head())
        df_sales_demand_grouped.to_parquet(r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace\Data\Processed-Dataflow\Forecast MKT\sales_demand.parquet', index=False)
        
    except Exception as e:
        print(f'Error: {e}')
      
if __name__ == "__main__":
    main()
    
