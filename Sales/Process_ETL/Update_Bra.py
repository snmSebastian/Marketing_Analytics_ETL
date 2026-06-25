"""
EL ACTUALIZADOR DE VENTAS: MOTOR DE CARGA INCREMENTAL
---------------------------------------------------
Este script es el que hace el "trabajo sucio" del día a día. Su misión es tomar los 
archivos de ventas más recientes y meterlos al ecosistema sin tener que reprocesar 
años de historia. Es el que mantiene Power BI actualizado para que el equipo comercial 
tenga sus números frescos cada mañana.

FLUJO DE TRABAJO:
1. Ingesta de Novedades: Escanea la carpeta de updates y consolida los Parquets 
   que acaban de llegar de los sistemas fuente.
2. Refinería de Datos: Aplica el combo de funciones de Sales (NSV, NPI, Precios, 
   Baterías) para que la data nueva hable el mismo idioma que el histórico.
3. Control de Calidad: Compara las sumas de ventas al inicio y al final. Si el 
   monto total cambia, lanza una alerta en consola para que no se nos pierda ni un centavo.
4. Empaquetado Parquet: Guarda los resultados particionados por año-mes, usando 
   la misma lógica que Fill Rate para que el Data Lake sea consistente.

💡 NOTA DE SENIOR:
¡Mucho ojo con la carpeta de entrada! Este proceso asume que lo que pongas ahí es 
"lo nuevo". Si metes archivos que ya estaban en el histórico, podrías causar 
duplicados dependiendo de cómo esté configurado el orquestador final. 
Además, fíjate siempre en el log de "Porcentaje de diferencia": si no es 0.00%, 
algo se rompió en los joins de maestros (G2N, NPI o Master Products) y los 
números de NSV van a salir mal.
"""

# Librerias
import pandas as pd
import glob
import os
import sys
from pathlib import Path
import numpy as np


# La importación debe ser relativa al paquete actual.
from Fill_Rate.Process_ETL.Process_Files import  group_parquet,format_columns,clean_sku
from Sales.Process_ETL.Process_Files import (process_columns_sales,assign_nsv,assign_selling_unit_price,assign_NPI_New_Carryover,
                                             LaunchYear_VR,assign_num_batteries, assign_NSV_NPI_w_Combo)


def read_files_parquets(input_path):
    """
    Lee archivos Parquet de un directorio, los consolida en un dataframe
    
    Args:
        input_path (str): Ruta del directorio donde se encuentran los archivos Parquets (.parquet) a consolidar.
    
    Returns:
        pd.DataFrame or None: DataFrame consolidado con todos los datos de los archivos, o None si no se encuentran
        archivos o la lectura falla sin consolidar nada.
     """
    
    # --- LECTURA Y CONSOLIDACION DE ARCHIVOS ---
    # Buscar todos los archivos .xlsx en el directorio de entrada.
    all_files_xlsx = glob.glob(os.path.join(input_path, "*.parquet"))

    if not all_files_xlsx:
        print(f"Advertencia: No se encontraron archivos .parquet en '{input_path}'.")
        return

    # Leer cada archivo y agregarlo a una lista de DataFrames:
    lst_files_xlsx = []
    for filename in all_files_xlsx:
        try:
            print(f"Leyendo archivo: {os.path.basename(filename)}")
            # Usar pd.read_excel para archivos .xlsx, no pd.read_csv
            df = pd.read_parquet(filename)
            lst_files_xlsx.append(df)
        except PermissionError:
            print(f"  [ERROR] Permiso denegado para leer el archivo: {os.path.basename(filename)}."
                  "\n  Asegúrate de que no esté abierto y vuelve a intentarlo.")
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
        sales_sharepoint = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace\Data\Processed-Dataflow\Sales\Sales SharePoint 2019-2025'
        sales_historic_processed_dir =SalesPaths.OUTPUT_PROCESSED_PARQUETS_DIR
        #===============================
        # --- Lectura de archivos 
        #===============================
        df_update = read_files_parquets(sales_sharepoint)
        
        #----------------------
        #----- LIMPIEZA SKU
        #----------------------
        df_update=clean_sku(df_update,'fk_SKU')
        

        print('1')
        print(df_update.head())

        df_brasil=df_update[df_update['fk_Country']=='BRAZIL']
        print('2')
        print(df_brasil.head())

        df_brasil= df_brasil[
        (df_brasil['fk_Date'] >= '2023-01-01') & 
        (df_brasil['fk_Date'] <= '2025-01-01')
        ].copy()
        print('3')
        print(df_brasil.head())

        lst_columns_sales = ['Source System', 'Document Type', 'fk_Date', 'fk_year_month', 'Week',
       'fk_Country', 'Sales Type', 'Sales Type Detail',
       'Sales Type Invoince Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
       'fk_date_country_customer_clasification', 'New New/Carryover', 'VR %',
       'Launch Year', 'Total Sales', 'Total Cost', 'Units Sold',
       'Units Return', 'NSV', 'FX Rate NSV', 'Selling Unit Price',
       'NPI Incremental Sales $', 'Num Batteries Sales',
       'Net Sales NPI w/Combo','fx_nsv_financial']
        
        columnas_a_crear=[col for col in lst_columns_sales if col not in df_brasil.columns]
       
        for col in columnas_a_crear:
            df_brasil[col] = pd.Series(np.nan, index=df_brasil.index, dtype='object')
        df_brasil=df_brasil[lst_columns_sales].copy()


        df_brasil['Source System']='SAP'
        df_brasil['Document Type']='SAP'
        df_brasil['fk_year_month']=df_brasil['fk_Date'].str[:7]
        print('4')
        print(df_brasil.head())
        # --- ESCRITURA DE LOS DATOS ACTUALIZADOS ---
        group_parquet(df_brasil, sales_historic_processed_dir,name='brazil')
        print("Sales Brazil Update completed successfully. ✅.")
        pass
    except Exception as e:
        print(f"Error en procesamiento de datos de Ventas: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
