'''Contiene las siguientes funciones:
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

from Fill_Rate.Process_ETL.Process_Files import read_files, asign_country_code, process_columns, group_parquet

def main():
    # La función realiza la escritura de archivos pero no devuelve ningún valor,
    # por lo que la llamamos directamente.
    input_path = r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Sales\Historic'
    output_path=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Sales'
    path_country=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Shared_Information_for_Projects\Country\Region_Country_codes.xlsx'
    # Leer los archivos de datos históricos y consolidarlos en un DataFrame.
    df_consolidated = read_files(input_path)
    # Leer el archivo de códigos de país.
    df_country = pd.read_excel(path_country,
                               sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
    # Definir las columnas relevantes para el procesamiento.    
    lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                   'fk_date_country_customer_clasification',
                   'Total Sales', 'Total Cost', 'Units Sold']
    df_consolidated = asign_country_code(df_consolidated, df_country)
    df_processed=process_columns(df_consolidated,lst_columns)
    group_parquet(df_processed, output_path,name='sales')


# --- EJECUCION DEL SCRIPT ---
# Es una buena práctica envolver la ejecución principal en un bloque if __name__ == "__main__":
if __name__ == "__main__":
    main()
