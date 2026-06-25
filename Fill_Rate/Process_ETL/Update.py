"""
ACTUALIZADOR INCREMENTAL: Módulo de Upsert Inteligente para Fill Rate.

Este script es el encargado de mantener la historia al día sin tener que procesar 
todo desde cero. Su superpoder es la "Actualización por Reemplazo": identifica qué 
periodos (meses) traen datos nuevos, limpia solo esos archivos y los sobrescribe.

¿Cómo funciona este proceso?
 1. ESCANEO: Lee los archivos nuevos y detecta qué meses (fk_year_month) se deben actualizar.
 2. LIMPIEZA QUIRÚRGICA (delete_parquet_files): Entra a la carpeta de históricos y 
    elimina ÚNICAMENTE los archivos .parquet de los meses afectados para evitar duplicados.
 3. TRANSFORMACIÓN: Usa las funciones core de 'Process_Files' para que la data nueva 
    tenga exactamente el mismo ADN que la histórica.
 4. REINSERCIÓN: Guarda los nuevos registros particionados, dejando el repositorio 
    actualizado y listo para Power BI.

💡 Regla de Oro: Este método garantiza integridad total. Si actualizas Enero 2025, 
el script borra el Enero viejo y pone el Enero nuevo, asegurando que no queden 
registros "fantasma" o duplicados en el camino.
"""

def delete_parquet_files(folder_path, lst_year_month_files_update, name_file=str):
    """
    Elimina del disco solo los periodos que vamos a actualizar. 
    Busca el patrón '{name_file}_{periodo}.parquet' y los borra para dejar el espacio 
    limpio antes de la nueva carga.
    """


# Librerias
import pandas as pd
import glob
import os
import sys
from pathlib import Path

# Importamos las funciones ya creadas que usaremos nuevamente
from .Process_Files import read_files, asign_country_code, process_columns, group_parquet, format_columns,clean_sku



def delete_parquet_files(folder_path, lst_year_month_files_update,name_file=str):
    """
    Elimina archivos Parquet cuyos nombres coincidan con los periodos a actualizar.

    Busca archivos con el patrón 'sales_{periodo}.parquet' para cada elemento en 
    lst_year_month_files_update y los elimina del directorio especificado.

    Args:
        folder_path (str): Ruta del directorio de los archivos.
        lst_year_month_files_update (list): Lista de periodos (ej. ['2025-01', '2024-12']).

    Returns:
        None
    """
    directory = Path(folder_path)
    if not directory.is_dir():
        print(f"Ruta no válida: {folder_path}")
        return

    count = 0
    try:
        for period in lst_year_month_files_update:
            # Construir el patrón específico: sales_2025-01.parquet
            file_pattern = f"{name_file}_{period}.parquet"
            for file in directory.glob(file_pattern):
                file.unlink()
                print(f"Archivo eliminado: {file.name}")
                count += 1

        print(f"--- Limpieza selectiva completada: {count} archivos eliminados. ✅ ---")

    except Exception as e:
        print(f"Error al eliminar archivos: {e}")
        sys.exit(1)


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
        fill_rate_historic_processed_dir = FillRatePaths.OUTPUT_PROCESSED_PARQUETS_DIR
        
        fill_rate_update_raw_dir = FillRatePaths.INPUT_RAW_UPDATE_DIR
        #historico
        #fill_rate_update_raw_dir = FillRatePaths.INPUT_RAW_HISTORIC_DIR
        
        country_code_file = FillRatePaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
        
        # --- PROCESAMIENTO DE ARCHIVOS DE ACTUALIZACIÓN ---
        df_country = pd.read_excel(country_code_file, sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
        for col in df_country.columns:
            df_country[col] = df_country[col].astype(str).str.upper().str.strip()

        lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                    'Fill Rate First Pass Order Qty', 'Fill Rate First Pass Invoice Qty',
                    'Fill Rate First Pass Order $', 'Fill Rate First Pass Invoice $']

        df_update = read_files(fill_rate_update_raw_dir)
    
        #========================
        # Limpieza sku
        #========================
        df_update = clean_sku(df_update, 'Country Material')
        
    
    
    
    
        df_update = df_update[
            ~df_update['Fiscal Year'].isin(['NAN', 'NONE', '', 'NAT'])
        ]
        if df_update is None or df_update.empty:
            print("No hay archivos para actualizar. Finalizando proceso.")
            return
    
    
        df_update = asign_country_code(df_update, df_country)
        df_update = process_columns(df_update, lst_columns)
        
   




        # Defino formato de las columnas
        lst_columns_str=['fk_Date', 'fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code',
        'fk_SKU']
        
        lst_columns_float=['Fill Rate First Pass Order Qty', 'Fill Rate First Pass Invoice Qty',
        'Fill Rate First Pass Order $', 'Fill Rate First Pass Invoice $']
        
        df_final=format_columns(df_update,lst_columns_str,lst_columns_float)
        
        # --- ESCRITURA DE LOS DATOS ACTUALIZADOS ---
        # Aquellos archivos existentes los sobreescribe en su totalidad
        group_parquet(df_final, fill_rate_historic_processed_dir,name='fill_rate')
        print("Fill Rate ETL Update completed successfully.")
        pass
    except Exception as e:
        print(f"Error en procesamiento de datos de Fill Rate: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
    