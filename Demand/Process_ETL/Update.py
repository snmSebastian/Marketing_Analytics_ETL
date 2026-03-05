"""
Módulo de orquestación para el proceso de Actualización Incremental (Upsert) de los datos de Demanda (Demand).
Este script construye el pipeline de actualización combinando:
1. Funciones de Lectura/Carga genéricas del módulo Fill_Rate.
2. Lógica de Mapeo y Transformación específica de Demanda (asign_country_code, process_columns) definida localmente.
Esto garantiza una actualización eficiente y adaptada a la estructura de datos de Demand."""


# Librerias
import pandas as pd
import glob
import os
import sys
# La importación debe ser relativa al paquete actual.
from Fill_Rate.Process_ETL.Process_Files import read_files, group_parquet,format_columns
from Fill_Rate.Process_ETL.Update import read_parquets_to_update,update_parquets
from Demand.Process_ETL.Process_Files import *

def main():
    print("=" * 55)
    print("---  INICIANDO PROCESO: DEMAND FULL LOAD ETL ---")
    print("=" * 55)
    """
    Orquesta el flujo ETL de Carga Completa (Full Load) para los datos de Demanda.

    Este proceso consolida la extracción de Snowflake con maestros externos para generar 
    el dataset final procesado y particionado.

    Pasos del flujo:
        1. Carga de dependencias: Rutas, archivos de mapeo (Country, GPP, SKU) y maestros (NPI, NSV).
        2. Enriquecimiento: Asigna códigos de país, dimensiones GPP y nombres de SKU.
        3. Cálculo Financiero: Transforma GSV a NSV y calcula ventas incrementales de NPI.
        4. Estandarización: Aplica formatos de datos (string/float) y genera llaves de auditoría.
        5. Persistencia: Limpia el directorio de salida y guarda el resultado en formato Parquet.

    Returns:
        None
    """
    # Importar las rutas de acceso rápido desde config_paths.py.,
    from config_paths import DemandPaths,MasterProductsPaths
    demand_update_raw_dir = DemandPaths.INPUT_RAW_UPDATE_DIR
    country_code_file = DemandPaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
    processed_parquet_dir = DemandPaths.OUTPUT_PROCESSED_PARQUETS_DIR
    fx_rate=DemandPaths.INPUT_PROCESSED_FX_RATE_FILE
    path_gpp=MasterProductsPaths.INPUT_PROCESSED_GPP_BRAND_FILE
    path_sku_name=MasterProductsPaths.INPUT_RAW_SkuName_FILE
    processed_gross_to_net=DemandPaths.INPUT_PROCESSED_GROSS_TO_NET_FILE
    md_product_processed_file=DemandPaths.INPUT_PROCESSED_MASTER_PRODUCTS_FILE
    npi=DemandPaths.INPUT_PROCESSED_NPI_FILE

     #===============================
    # --- Lectura de archivos 
    #===============================
    # Leer los archivos de datos históricos y consolidarlos en un DataFrame.
    df_consolidated = pd.read_parquet(demand_update_raw_dir/'QueryDemand.parquet', engine='pyarrow')
    len_initial=len(df_consolidated)

    # Leer el archivo de códigos de país.
    df_country = pd.read_excel(country_code_file,
                               sheet_name='Code Country Demand', dtype=str, engine='openpyxl')
    df_country_nsv = pd.read_excel(country_code_file,
                               sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
    
    #df_fx_rate = pd.read_excel(fx_rate, dtype=str, engine='openpyxl')
    df_gpp=pd.read_excel(path_gpp, dtype=str, engine='openpyxl',sheet_name='GPP')
    df_skuName=pd.read_parquet(path_sku_name, engine='pyarrow')

    df_md_product=pd.read_excel(md_product_processed_file,dtype=str, engine='openpyxl')
    df_gross_to_net=pd.read_excel(processed_gross_to_net,dtype=str, engine='openpyxl')
    df_npi=pd.read_excel(npi,sheet_name='Database',dtype=str, engine='openpyxl')

    # Definir las columnas relevantes para el procesamiento.    
    lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_SKU','SKU Description','Brand',
                   'GPP SBU','GPP Division Description','GPP Category Description','GPP Portfolio Description',
                  'FCST_QTY', 'FORECAST_VALUE_GSV','CURRENT_STANDARD_COST']
    df_consolidated = asign_country_code(df_consolidated, df_country)
    #print('assing country')
    df_consolidated=asign_gpp(df_consolidated,df_gpp)
    #print('gpp')
    df_consolidated=asign_skuName(df_consolidated,df_skuName)
    #print('skuname')
    df_processed=process_columns(df_consolidated,lst_columns)
    #print('columns')
    #---ASING NSV
    df_processed.rename(columns={'FORECAST_VALUE_GSV':'Total Sales'},inplace=True)
    df_processed=assign_nsv(df_processed, df_md_product, df_gross_to_net,df_country_nsv)
    df_processed.rename(columns={'Total Sales':'FORECAST_VALUE_GSV'},inplace=True)
    
    df_processed=assign_NPI_New_Carryover(df_processed,df_npi,df_country_nsv)
    #print(f'longitud posterior a asignación NPI: {len(df_processed)}')

    df_processed=assign_fk_YearRegionSku(df_processed,df_country_nsv)
    #print(f'longitud posterior a asignación fk_YearRegionSku: {len(df_processed)}')



    #--- Formato de columnas ----
    lst_columns_str = ['fk_Date','fk_year_month', 'fk_Country', 'fk_SKU','SKU Description','Brand',
                       'GPP SBU','GPP Division Description','GPP Category Description','GPP Portfolio Description',
                       'New New/Carryover',
                       'fk_YearRegionSku']
    lst_columns_float=['FCST_QTY', 'FORECAST_VALUE_GSV','NSV',
                  'CURRENT_STANDARD_COST',
                  'NPI Incremental Sales $']
    df_processed=format_columns(df_processed,lst_columns_str,lst_columns_float)
    #=========================================================
    #--- ASIGNACIÓN COLUMNAS CALCULADAS
    #=========================================================
    #df_consolidated=assign_local_currency(df_consolidated,df_fx_rate)
    len_end=len(df_processed)
    if len_initial==len_end:
        print(f'{"="}*50')
        print(f'El DataFrame procesado tiene la misma longitud que el DataFrame original')
        print(f'{"="}*50')
    else:
        print(f'{"="}*50')
        print(f'El DataFrame procesado tiene una longitud diferente que el DataFrame original')
        print(f'El dataframe original tiene {len_initial} registros y el dataframe procesado tiene {len_end} registros')
        print(f'la diferencia es de {len_initial-len_end} registros')
        print(f'{"="}*50')
    #=========================================================
    #--- ACTUALIZACION CARPETA
    #=========================================================
    #---- Elimina todos los archivos existentes
    delete_parquet_files(processed_parquet_dir)
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
