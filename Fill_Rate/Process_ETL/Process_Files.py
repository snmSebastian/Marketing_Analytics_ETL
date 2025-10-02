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


def read_files(input_path):
    """
    Lee archivos Excel de un directorio, los consolida en un dataframe
    """

    # --- LECTURA Y CONSOLIDACION DE ARCHIVOS ---
    # Buscar todos los archivos .xlsx en el directorio de entrada.
    all_files_xlsx = glob.glob(os.path.join(input_path, "*.xlsx"))

    if not all_files_xlsx:
        print(f"Advertencia: No se encontraron archivos .xlsx en '{input_path}'.")
        return

    # Leer cada archivo y agregarlo a una lista de DataFrames:
    lst_files_xlsx = []
    for filename in all_files_xlsx:
        try:
            print(f"Leyendo archivo: {os.path.basename(filename)}")
            # Usar pd.read_excel para archivos .xlsx, no pd.read_csv
            df = pd.read_excel(filename, dtype=str, engine='openpyxl')
            lst_files_xlsx.append(df)
        except PermissionError:
            print(f"  [ERROR] Permiso denegado para leer el archivo: {os.path.basename(filename)}."
                  "\n  Asegúrate de que no esté abierto en Excel y vuelve a intentarlo.")
        except Exception as e:
            print(f"  [ERROR] No se pudo procesar el archivo {os.path.basename(filename)}: {e}")

    # Concatenar todos los DataFrames en uno solo
    if len(lst_files_xlsx) == 0:
        df_consolidated = lst_files_xlsx[0]
    else:
         df_consolidated = pd.concat(lst_files_xlsx, axis=0, ignore_index=True)


    for col in df_consolidated.columns:
        df_consolidated[col] = df_consolidated[col].astype(str).str.upper().str.strip()

    return df_consolidated

def asign_country_code(df_consolidated, df_country):
        """
        Asigna el código de país a cada fila del DataFrame df
        usando el DataFrame country como referencia.
        """
        # Crear una columna 'code concat country' que concatena 'Country Code' y 'Destination Country'
        df_consolidated['code concat country'] = df_consolidated['Country Code'].astype(str) + df_consolidated['Destination Country'].astype(str)

        # Crear un mapa de códigos de país a nombres de país
        for col in df_country.columns:
            df_country[col] = df_country[col].astype(str).str.upper().str.strip()
        country_map = df_country.set_index('Country Code Concat')['Country']

        # Usar .map() para crear la nueva columna 'new country'
        df_consolidated['fk_Country'] = df_consolidated['code concat country'].map(country_map)

        return df_consolidated
    
def process_columns(df_consolidated,lst_columns):
    ""    
    """
            Escoje las columnas relevantes del DataFrame df y las procesa
            para asegurar que todas las columnas estén en mayúsculas y sin espacios.
    """
    try:
        

        # --- PROCESAMIENTO Y AGRUPACION ---
        # Crear una columna 'year_month' para usar en la agrupación (ej: '2023-01').
        # Se usa .str.zfill(2) para asegurar que los meses tengan dos dígitos (ej: '01', '02', etc.)
        # lo que mejora la consistencia y el orden de los nombres de archivo.   
        # 
        # Definir los mapeos que quieres aplicar
        mapeo_deseado = {
            'Sold-To-Customer Code': 'fk_Sold_To_Customer_Code',
            'Sold-To Customer Code': 'fk_Sold_To_Customer_Code', # La clave es diferente, el valor es el mismo
            'Country Material': 'fk_SKU',
            'Global Material': 'fk_SKU',
        }

        # Crear un nuevo diccionario de renombre que solo incluye las columnas existentes
        columnas_existentes = df_consolidated.columns
        mapeo_filtrado = {
            old_name: new_name
            for old_name, new_name in mapeo_deseado.items()
            if old_name in columnas_existentes
        }

        df_consolidated.rename(columns=mapeo_filtrado, inplace=True)    

        df_consolidated['fk_year_month'] = (df_consolidated['Fiscal Year'].astype(str) + '-' +
                                            df_consolidated['Fiscal Period'].astype(str).str.zfill(2))
        df_consolidated['clasification']=(df_consolidated['GPP Division'] + '-' +
                                         df_consolidated['GPP Category'] + '-' +
                                         df_consolidated['GPP Portfolio'])
        
        
        df_consolidated['fk_date_country_customer_clasification'] = (df_consolidated['fk_year_month'] + '-' +
                                                            df_consolidated['fk_Country']+ '-' +
                                                            df_consolidated['fk_Sold_To_Customer_Code']+ '-' +
                                                            df_consolidated['clasification']).str.upper().str.strip()
        df_consolidated['fk_Date']=pd.to_datetime(df_consolidated['fk_year_month'], errors='coerce')
       
        
        df_processed = df_consolidated[lst_columns]                                                                                                                                                                           
        # Convertir todas las columnas a mayúsculas y eliminar espacios
        for col in df_processed.columns:        
            df_processed[col] = df_consolidated[col].astype(str).str.upper().str.strip()    
       
    except KeyError as e:
                print(f"Error: La columna {e} no se encontró en los archivos. ")
    return df_processed

def group_parquet(df_processed, output_path,name='fill_rate'):
    """ Guarda un dataframe consolidado en archivos Parquet segmentados por año-mes. """
    # --- ESCRITURA DE ARCHIVOS PARQUET SEGMENTADOS ---
    # Agrupar el DataFrame por 'year_month' y guardar cada grupo en un archivo Parquet.
    for period, group in df_processed.groupby('fk_year_month'):
        # Crear un nombre de archivo descriptivo, ej: sales_2023-01.parquet
        output_filename = f"{name}_{period}.parquet"
        output_full_path = os.path.join(output_path, output_filename)
        print(f"Guardando grupo {period} en: {output_full_path}")
        # Guardar el grupo en formato Parquet, excluyendo el índice.
        group.to_parquet(output_full_path, index=False)
        print("\nProceso completado. Archivos Parquet generados exitosamente.")

def main():
    # Definir las rutas de entrada y salida.
    input_path = r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Fill Rate\Historic'
    output_path=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Fill_Rate'
    path_country=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Shared_Information_for_Projects\Country\Region_Country_codes.xlsx'
    # Leer los archivos de datos históricos y consolidarlos en un DataFrame.
    df_consolidated = read_files(input_path)
    # Leer el archivo de códigos de país.
    df_country = pd.read_excel(path_country,
                               sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
    # Definir las columnas relevantes para el procesamiento.    
    lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                   'fk_date_country_customer_clasification',
                   'Fill Rate First Pass Order Qty', 'Fill Rate First Pass Invoice Qty',
                   'Fill Rate First Pass Order $', 'Fill Rate First Pass Invoice $']
    df_consolidated = asign_country_code(df_consolidated, df_country)
    df_processed=process_columns(df_consolidated,lst_columns)
    group_parquet(df_processed, output_path, name='fill_rate')



# --- EJECUCION DEL SCRIPT ---
# Es una buena práctica envolver la ejecución principal en un bloque if __name__ == "__main__":
if __name__ == "__main__":
    main()