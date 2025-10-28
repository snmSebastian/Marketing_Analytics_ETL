'''
Módulo de orquestación para la Carga Completa (Full Load) de los datos de Ventas.
Este script reutiliza las funciones de ETL definidas en el módulo Fill_Rate.Process_ETL.Process_Files
para leer, transformar y cargar los datos históricos de Ventas, estandarizando
el flujo de trabajo entre los distintos dominios de negocio.

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

# Importo funciones creadas que seran usadas nuevamente
from Fill_Rate.Process_ETL.Process_Files import read_files, asign_country_code, process_columns, group_parquet,format_columns

def main():
    """
    Función principal que orquesta el flujo ETL completo para los datos de Ventas (Sales).
    Define las rutas de entrada/salida y las columnas de métricas específicas
    ('Total Sales', 'Total Cost', 'Units Sold') antes de ejecutar el pipeline reutilizado.
    Returns: None: La función orquesta el proceso y no devuelve un valor.
    """
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: SALES FULL LOAD ETL ---")
    print("=" * 55)
    # --- CONFIGURACIÓN DE RUTAS ---
    # Importamos las rutas
    from config_paths import SalesPaths
    sales_historic_raw_dir = SalesPaths.INPUT_RAW_HISTORIC_DIR
    country_code_file = SalesPaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
    processed_parquet_dir = SalesPaths.OUTPUT_PROCESSED_PARQUETS_DIR

    #input_path = r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Sales\Historic'
    #output_path=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Sales'
    #path_country=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Shared_Information_for_Projects\Country\Region_Country_codes.xlsx'
    
    # Leer los archivos de datos históricos y consolidarlos en un DataFrame.
    df_consolidated = read_files(sales_historic_raw_dir)
    # Leer el archivo de códigos de país.
    df_country = pd.read_excel(country_code_file,
                               sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
    # Definir las columnas relevantes para el procesamiento.    
    lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                   'fk_date_country_customer_clasification',
                   'Total Sales', 'Total Cost', 'Units Sold']
    df_consolidated = asign_country_code(df_consolidated, df_country)
    df_processed=process_columns(df_consolidated,lst_columns)
    
    # --- Formato de columnas ---
    lst_columns_srt = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                   'fk_date_country_customer_clasification']
    lst_columns_float = ['Total Sales', 'Total Cost', 'Units Sold']
    df_processed=format_columns(df_processed,lst_columns_srt,lst_columns_float)
    
    #  --- ESCRITURA DE ARCHIVOS PARQUET SEGMENTADOS --
    group_parquet(df_processed, processed_parquet_dir,name='sales')

# --- EJECUCION DEL SCRIPT ---
# Es una buena práctica envolver la ejecución principal en un bloque if __name__ == "__main__":
if __name__ == "__main__":
    main()
    print("Processing of historical Sales data completed successfully. ✅.")
