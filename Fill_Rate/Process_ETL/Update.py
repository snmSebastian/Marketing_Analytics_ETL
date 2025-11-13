"""
Módulo de actualización incremental para los datos históricos procesados de Fill Rate.
Utiliza el principio de actualización por 'reemplazo de partición' (sobrescribiendo
los registros existentes en un periodo con los nuevos datos) para mantener
la integridad y actualidad de los archivos Parquet históricos.
"""

# Librerias
import pandas as pd
import glob
import os
import sys

# Importamos las funciones ya creadas que usaremos nuevamente
from .Process_Files import read_files, asign_country_code, process_columns, group_parquet, format_columns


# Leer los archivos Parquet históricos que se van a actualizar segun:fk_year_month y concatenarlos en un DataFrame
def read_parquets_to_update(path_parquets_historics, lst_year_month_files_update,lst_columns):
    """Lee los archivos parquet históricos que corresponden a los periodos a actualizar.
    Args:
        path_parquets_historics (str): Ruta del directorio que contiene los archivos Parquet históricos.
        lst_year_month_files_update (list): Lista de strings con los periodos 'YYYY-MM' (fk_year_month) a buscar y cargar.
        lst_columns (list): Lista de columnas esperadas. Usada para inicializar un DataFrame vacío si no se encuentran archivos.
    Returns:
        pd.DataFrame: DataFrame consolidado con los datos históricos de los periodos a actualizar, o un DataFrame
                      vacío con las lst_columns si no hay archivos.
    """
    lst_files = []
    for year_month in lst_year_month_files_update:
        # Busca archivos que coincidan con el patrón, ej: 'fill_rate_2023-01.parquet'
        pattern = os.path.join(path_parquets_historics, f"*{year_month}*.parquet")
        found_files = glob.glob(pattern)
        if not found_files:
            print(f"Advertencia: No se encontró archivo parquet para el periodo {year_month} en '{path_parquets_historics}'\n")
        lst_files.extend(found_files)
    
    if not lst_files:
        print("No se encontraron archivos parquet para actualizar.")
        return pd.DataFrame(columns=lst_columns)
        
    print(f"Archivos parquet a actualizar encontrados: {len(lst_files)}\n")

    return pd.concat([pd.read_parquet(file) for file in lst_files], ignore_index=True)

def update_parquets(df_parquets_historic, df_update,fk_column='fk_date_country_customer_clasification'):
    """
    Implementa la lógica de 'delete and insert' (o upsert). Identifica los registros a actualizar en el histórico
    usando la clave compuesta y los reemplaza concatenando los nuevos registros de df_update.
    Args:
        df_parquets_historic (pd.DataFrame): DataFrame que contiene los datos históricos leídos de los archivos Parquet de los periodos a actualizar.
        df_update (pd.DataFrame): DataFrame con los nuevos registros que deben reemplazar a los existentes.
        fk_column (str, optional): Nombre de la columna que actúa como clave única/compuesta para la deduplicación/reemplazo. Por defecto es 'fk_date_country_customer_clasification'.
    Returns:
        pd.DataFrame: El DataFrame final que contiene los registros históricos que no se actualizaron, más los
                      nuevos registros de df_update.
    
    """
    # Obtener la lista de claves únicas a actualizar/reemplazar.
    #print(f'columnas df_update: {df_update.columns}')
    #print(f'columnas df_parquets_historic: {df_parquets_historic.columns}')
    keys_to_update = df_update[fk_column].unique()
    
    # Filtrar el dataframe histórico para excluir los registros que NO serán actualizados.
    df_parquets_filtered = df_parquets_historic[~df_parquets_historic[fk_column].isin(keys_to_update)]
        
    # Combinar los datos históricos filtrados con los nuevos datos.
    if df_parquets_filtered.empty:
        df_final = df_update
        print("Advertencia: No se encontraron registros históricos que mantener. df_final = df_update.")
    else:
        df_final = pd.concat([df_parquets_filtered, df_update], ignore_index=True)
    return df_final

def main():
    """ 
    Orquesta el proceso de actualización incremental de Fill Rate.
        1. Procesa los archivos brutos de la actualización (usando funciones de Process_Files).
        2. Determina los periodos ('fk_year_month') afectados.
        3. Carga los archivos Parquet históricos correspondientes.
        4. Ejecuta la lógica de update_parquets para reemplazar los registros.
        5. Guarda el resultado final, sobrescribiendo los archivos Parquet originales (particionamiento).
    Returns: None: La función orquesta el proceso y no devuelve un valor.
    """

    print("=" * 55)
    print("--- INICIANDO PROCESO: FILL RATE UPDATE ETL ---")
    print("=" * 55)
    
    try:
        # --- IMPORTAMOS ARCHIVOS ---
        from config_paths import FillRatePaths
        #fill_rate_historic_processed_dir = FillRatePaths.OUTPUT_PROCESSED_PARQUETS_DIR_PRUEBA #  PRUEBA
        fill_rate_historic_processed_dir = FillRatePaths.OUTPUT_PROCESSED_PARQUETS_DIR
        
        fill_rate_update_raw_dir = FillRatePaths.INPUT_RAW_UPDATE_DIR
        country_code_file = FillRatePaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
        
        # --- PROCESAMIENTO DE ARCHIVOS DE ACTUALIZACIÓN ---
        df_country = pd.read_excel(country_code_file, sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
        for col in df_country.columns:
            df_country[col] = df_country[col].astype(str).str.upper().str.strip()

        lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                    'fk_date_country_customer_clasification',
                    'Fill Rate First Pass Order Qty', 'Fill Rate First Pass Invoice Qty',
                    'Fill Rate First Pass Order $', 'Fill Rate First Pass Invoice $']

        df_update = read_files(fill_rate_update_raw_dir)
        if df_update is None or df_update.empty:
            print("No hay archivos para actualizar. Finalizando proceso.")
            return

        df_update = asign_country_code(df_update, df_country)
        df_update = process_columns(df_update, lst_columns)
        
        # --- LECTURA Y ACTUALIZACIÓN DE DATOS HISTÓRICOS ---
        lst_year_month_files_update = df_update['fk_year_month'].unique().tolist()
        
        df_parquets_historic = read_parquets_to_update(fill_rate_historic_processed_dir, lst_year_month_files_update,lst_columns)
        
        df_final = update_parquets(df_parquets_historic, df_update,fk_column='fk_date_country_customer_clasification')
        # Defino formato de las columnas
        lst_columns_str=['fk_Date', 'fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code',
        'fk_SKU', 'fk_date_country_customer_clasification']
        
        lst_columns_float=['Fill Rate First Pass Order Qty', 'Fill Rate First Pass Invoice Qty',
        'Fill Rate First Pass Order $', 'Fill Rate First Pass Invoice $']
        
        
        df_final=format_columns(df_final,lst_columns_str,lst_columns_float)
        
        # --- ESCRITURA DE LOS DATOS ACTUALIZADOS ---
        group_parquet(df_final, fill_rate_historic_processed_dir,name='fill_rate')
        print("Fill Rate ETL Update completed successfully.")
        pass
    except Exception as e:
        print(f"Error en procesamiento de datos de Fill Rate: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
    