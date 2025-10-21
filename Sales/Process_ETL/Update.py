# Librerias
import pandas as pd
import glob
import os

# La importación debe ser relativa al paquete actual.
from Fill_Rate.Process_ETL.Process_Files import read_files, asign_country_code, process_columns, group_parquet
from Fill_Rate.Process_ETL.Update import read_parquets_to_update,update_parquets

def main():
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: SALES UPDATE ETL ---")
    print("=" * 55)
    # --- CONFIGURACIÓN DE RUTAS ---
    from config_paths import SalesPaths
    sales_historic_processed_dir =SalesPaths.OUTPUT_PROCESSED_PARQUETS_DIR_PRUEBA
    #sales_historic_processed_dir =SalesPaths.OUTPUT_PROCESSED_PARQUETS_DIR
    
    sales_update_raw_dir = SalesPaths.INPUT_RAW_UPDATE_DIR
    country_code_file = SalesPaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
    

    #path_parquets_historics = r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Sales\prueba'
    #path_files_updates = r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Sales\Mothly_Update'
    #path_country_codes = r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Shared_Information_for_Projects\Country\Region_Country_codes.xlsx'
    
    # --- PROCESAMIENTO DE ARCHIVOS DE ACTUALIZACIÓN ---
    df_country = pd.read_excel(country_code_file, sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
    lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                   'fk_date_country_customer_clasification',
                   'Total Sales', 'Total Cost', 'Units Sold']

    df_update = read_files(sales_update_raw_dir)
    if df_update is None or df_update.empty:
        print("No hay archivos para actualizar. Finalizando proceso.")
        return

    df_update = asign_country_code(df_update, df_country)
    df_update = process_columns(df_update, lst_columns)

    # --- LECTURA Y ACTUALIZACIÓN DE DATOS HISTÓRICOS ---
    lst_year_month_files_update = df_update['fk_year_month'].unique().tolist()
    df_parquets_historic = read_parquets_to_update(sales_historic_processed_dir, lst_year_month_files_update,lst_columns)
    
    df_final = update_parquets(df_parquets_historic, df_update,fk_column='fk_date_country_customer_clasification')
    
    # --- ESCRITURA DE LOS DATOS ACTUALIZADOS ---
    group_parquet(df_final, sales_historic_processed_dir,name='sales')

if __name__ == "__main__":
    main()
    print("Sales ETL Update completed successfully. ✅.")