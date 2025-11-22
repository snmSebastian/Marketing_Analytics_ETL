"""
Módulo de orquestación para el proceso de Actualización Incremental (Upsert) de los datos de Ventas.
Reutiliza los componentes de procesamiento (E y T) y las funciones de gestión de Parquet (L)
del módulo Fill_Rate para asegurar una metodología de actualización de datos estandarizada
y unificada en todo el proyecto ETL.
"""

# Librerias
import pandas as pd
import glob
import os
import sys

# La importación debe ser relativa al paquete actual.
from Fill_Rate.Process_ETL.Process_Files import read_files, asign_country_code, process_columns, group_parquet,format_columns
from Fill_Rate.Process_ETL.Update import read_parquets_to_update,update_parquets
from Sales.Process_ETL.Process_Files import (assign_nsv,assign_selling_unit_price,assign_NPI_New_Carryover,
                                             LaunchYear_VR,assign_num_batteries, assign_NSV_NPI_w_Combo)
def main():
    """
    Orquesta el flujo de actualización incremental para los datos de Ventas.
    El proceso incluye: 1) Carga y procesamiento de los nuevos archivos de actualización.
    2) Determinación de los periodos 'fk_year_month' a actualizar. 3) Aplicación de la
    lógica de 'Upsert' (reemplazo de registros). 4) Escritura final de los archivos Parquet.
    Returns: None: La función orquesta el proceso y no devuelve un valor.
    """
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: SALES UPDATE ETL ---")
    print("=" * 55)
    try:
        # --- CONFIGURACIÓN DE RUTAS ---
        from config_paths import SalesPaths       
        sales_update_raw_dir = SalesPaths.INPUT_RAW_UPDATE_DIR
        country_code_file = SalesPaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
        processed_gross_to_net=SalesPaths.INPUT_PROCESSED_GROSS_TO_NET_FILE
        npi=SalesPaths.INPUT_PROCESSED_NPI_FILE
        filter_npi=SalesPaths.INPUT_PROCESSED_FILTER_NPI_FILE
        md_product_processed_file=SalesPaths.INPUT_PROCESSED_MASTER_PRODUCTS_FILE

        #sales_historic_processed_dir =SalesPaths.OUTPUT_PROCESSED_PARQUETS_DIR_PRUEBA
        sales_historic_processed_dir =SalesPaths.OUTPUT_PROCESSED_PARQUETS_DIR
        #===============================
        # --- Lectura de archivos 
        #===============================
        df_update = read_files(sales_update_raw_dir)
        df_country = pd.read_excel(country_code_file, sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
        
        df_md_product=pd.read_excel(md_product_processed_file,dtype=str, engine='openpyxl')
        df_gross_to_net=pd.read_excel(processed_gross_to_net,dtype=str, engine='openpyxl')
        df_npi=pd.read_excel(npi,dtype=str, engine='openpyxl')
        df_filter_npi=pd.read_excel(filter_npi,dtype=str, engine='openpyxl')
       
        #=========================================================
        # --- PROCESAMIENTO DE ARCHIVOS DE ACTUALIZACIÓN ---
        #=========================================================
        # Definir las columnas relevantes para el procesamiento. 

        lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                    'fk_date_country_customer_clasification',
                    'Total Sales', 'Total Cost', 'Units Sold']

        if df_update is None or df_update.empty:
            print("No hay archivos para actualizar. Finalizando proceso.")
            return

        df_update = asign_country_code(df_update, df_country)
        df_update = process_columns(df_update, lst_columns)

        #=========================================================
        #--- ASIGNACIÓN COLUMNAS CALCULADAS
        #=========================================================
        df_update=assign_nsv(df_update,df_md_product,df_gross_to_net)
        df_update=assign_selling_unit_price(df_update)
        df_update=assign_NPI_New_Carryover(df_update,df_npi)
        df_update=LaunchYear_VR(df_update,df_npi,df_country)
        df_update=assign_num_batteries(df_update,df_md_product)
        df_update=assign_NSV_NPI_w_Combo(df_update,df_filter_npi)
        
        #=========================================================
        # --- LECTURA Y ACTUALIZACIÓN DE DATOS HISTÓRICOS ---
        #=========================================================
        lst_year_month_files_update = df_update['fk_year_month'].unique().tolist()
        df_parquets_historic = read_parquets_to_update(sales_historic_processed_dir, lst_year_month_files_update,lst_columns)
        df_final = update_parquets(df_parquets_historic, df_update,fk_column='fk_date_country_customer_clasification')
        
        #====================================
        # --- Formato de columnas ---
        #====================================
        lst_columns_srt = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                    'fk_date_country_customer_clasification',
                    'New New/Carryover',
                    'VR %','Launch Year']
        lst_columns_float = ['Total Sales', 'Total Cost', 'Units Sold',
                            'NSV','Selling Unit Price',
                            'NPI Incremental Sales $',
                            'Num Batteries Sales',
                            'Net Sales NPI w/Combo']
        df_final=format_columns(df_final,lst_columns_srt,lst_columns_float)
        
        # --- ESCRITURA DE LOS DATOS ACTUALIZADOS ---
        group_parquet(df_final, sales_historic_processed_dir,name='sales')
        print("Sales ETL Update completed successfully. ✅.")
        pass
    except Exception as e:
        print(f"Error en procesamiento de datos de Ventas: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
