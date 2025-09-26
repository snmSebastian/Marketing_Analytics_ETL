# Librerias
import pandas as pd
import glob
import os

# La importación debe ser relativa al paquete actual.
from .Process_Files import read_files, asign_country_code, process_columns, group_parquet

# Leer los archivos Parquet históricos que se van a actualizar segun:fk_year_month y concatenarlos en un DataFrame
def read_parquets_to_update(path_parquets_historics, lst_year_month_files_update):
    """Lee los archivos parquet históricos que corresponden a los periodos a actualizar."""
    lst_files = []
    for year_month in lst_year_month_files_update:
        # Busca archivos que coincidan con el patrón, ej: 'fill_rate_2023-01.parquet'
        pattern = os.path.join(path_parquets_historics, f"*{year_month}*.parquet")
        found_files = glob.glob(pattern)
        if not found_files:
            print(f"Advertencia: No se encontró archivo parquet para el periodo {year_month} en '{path_parquets_historics}'")
        lst_files.extend(found_files)
    
    if not lst_files:
        print("No se encontraron archivos parquet para actualizar.")
        return pd.DataFrame()
        
    print(f"Archivos parquet a actualizar encontrados: {len(lst_files)}")
    return pd.concat([pd.read_parquet(file) for file in lst_files], ignore_index=True)

def update_parquets(df_parquets_historic, df_update):
    """
    Actualiza el dataframe histórico eliminando los registros viejos y
    concatenando los nuevos registros del dataframe de actualización.
    """
    # Obtener la lista de claves únicas a actualizar/reemplazar.
    keys_to_update = df_update['fk_date_country_customer_clasification'].unique()
    
    # Filtrar el dataframe histórico para excluir los registros que NO serán actualizados.
    df_parquets_filtered = df_parquets_historic[~df_parquets_historic['fk_date_country_customer_clasification'].isin(keys_to_update)]
    
    # Combinar los datos históricos filtrados con los nuevos datos.
    df_final = pd.concat([df_parquets_filtered, df_update], ignore_index=True)
    return df_final

def main():
    # --- CONFIGURACIÓN DE RUTAS ---
    path_parquets_historics = r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Fill_Rate\prueba'
    path_files_updates = r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Fill Rate\prueba'
    path_country_codes = r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Shared_Information_for_Projects\Country\Region_Country_codes.xlsx'
    output_path =r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Fill_Rate\prueba'

    # --- PROCESAMIENTO DE ARCHIVOS DE ACTUALIZACIÓN ---
    df_country = pd.read_excel(path_country_codes, sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
    lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                   'fk_date_country_customer_clasification',
                   'Fill Rate First Pass Order Qty', 'Fill Rate First Pass Invoice Qty',
                   'Fill Rate First Pass Order $', 'Fill Rate First Pass Invoice $']

    df_update = read_files(path_files_updates)
    if df_update is None or df_update.empty:
        print("No hay archivos para actualizar. Finalizando proceso.")
        return

    df_update = asign_country_code(df_update, df_country)
    df_update = process_columns(df_update, lst_columns)

    # --- LECTURA Y ACTUALIZACIÓN DE DATOS HISTÓRICOS ---
    lst_year_month_files_update = df_update['fk_year_month'].unique().tolist()
    df_parquets_historic = read_parquets_to_update(path_parquets_historics, lst_year_month_files_update)
    
    df_final = update_parquets(df_parquets_historic, df_update)
    
    # --- ESCRITURA DE LOS DATOS ACTUALIZADOS ---
    group_parquet(df_final, output_path)

if __name__ == "__main__":
    main()