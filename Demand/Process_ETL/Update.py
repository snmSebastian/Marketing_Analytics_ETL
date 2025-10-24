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

# La importación debe ser relativa al paquete actual.
from Fill_Rate.Process_ETL.Process_Files import read_files, group_parquet
from Fill_Rate.Process_ETL.Update import read_parquets_to_update,update_parquets
from Demand.Process_ETL.Process_Files import asign_country_code, process_columns

def main():
    """
    Orquesta el flujo de actualización incremental para los datos de Demanda.
        1. Procesa los archivos brutos de la actualización utilizando la lógica de transformación de Demand.
        2. Determina los periodos afectados.
        3. Aplica el Upsert utilizando la clave única 'fk_date_country_clasification'.
        4. Guarda los archivos Parquet actualizados, sobrescribiendo los periodos históricos.
    Returns: None: La función orquesta el proceso y no devuelve un valor.
    """
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: DEMAND UPDATE ETL ---")
    print("=" * 55)
    # --- CONFIGURACIÓN DE RUTAS ---
    from config_paths import DemandPaths
    #demand_historic_processed_dir = DemandPaths.OUTPUT_PROCESSED_PARQUETS_DIR
    demand_historic_processed_dir = DemandPaths.OUTPUT_PROCESSED_PARQUETS_DIR_PRUEBA
    
    demand_update_raw_dir = DemandPaths.INPUT_RAW_UPDATE_DIR
    country_code_file = DemandPaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
    
    # --- PROCESAMIENTO DE ARCHIVOS DE ACTUALIZACIÓN ---
    df_country = pd.read_excel(country_code_file,
                               sheet_name='Code Country Demand', dtype=str, engine='openpyxl')
    lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_SKU',
                  'fk_date_country_clasification',
                  'Demand History & Forecast-QTY', 'Shipment History& Forecast-Qty',
                  'Demand History & Forecast-GSV', 'Shipment History&Forecast-GSV']
    df_update = read_files(demand_update_raw_dir)
    if df_update is None or df_update.empty:
        print("No hay archivos para actualizar. Finalizando proceso.")
        return

    df_update = asign_country_code(df_update, df_country)
    df_update = process_columns(df_update, lst_columns)

    # --- LECTURA Y ACTUALIZACIÓN DE DATOS HISTÓRICOS ---
    lst_year_month_files_update = df_update['fk_year_month'].unique().tolist()
    df_parquets_historic = read_parquets_to_update(demand_historic_processed_dir, lst_year_month_files_update,lst_columns)
    
    df_final = update_parquets(df_parquets_historic, df_update,fk_column='fk_date_country_clasification')
    
    # --- ESCRITURA DE LOS DATOS ACTUALIZADOS ---
    group_parquet(df_final, demand_historic_processed_dir,name='demand')

if __name__ == "__main__":
    main()
    print("Demand ETL Update completed successfully. ✅.")