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
        sales_update_raw_dir = SalesPaths.INPUT_RAW_UPDATE_DIR
        country_code_file = SalesPaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
        processed_gross_to_net=SalesPaths.INPUT_PROCESSED_GROSS_TO_NET_FILE
        npi=SalesPaths.INPUT_PROCESSED_NPI_FILE
        md_product_processed_file=SalesPaths.INPUT_PROCESSED_MASTER_PRODUCTS_FILE
        
        #sales_historic_processed_dir =SalesPaths.OUTPUT_PROCESSED_PARQUETS_DIR_PRUEBA
        sales_historic_processed_dir =SalesPaths.OUTPUT_PROCESSED_PARQUETS_DIR
        #===============================
        # --- Lectura de archivos 
        #===============================
        df_update = read_files_parquets(sales_update_raw_dir)
        
        df_country = pd.read_excel(country_code_file, sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
        
        df_md_product=pd.read_excel(md_product_processed_file,dtype=str, engine='openpyxl')
        df_gross_to_net=pd.read_excel(processed_gross_to_net,dtype=str, engine='openpyxl')
        # Asegurar que npi se lea correctamente según los nombres de las pestañas
        df_npi=pd.read_excel(npi,dtype=str,sheet_name='Database', engine='openpyxl')
        df_filter_npi=pd.read_excel(npi,sheet_name='NPI Concat', dtype=str, engine='openpyxl')
       
        #----------------------
        #----- LIMPIEZA SKU
        #----------------------
        df_update=clean_sku(df_update,'fk_SKU')
        df_md_product=clean_sku(df_md_product,'SKU')
        df_npi=clean_sku(df_npi,'SKU')
        df_filter_npi=clean_sku(df_filter_npi,'SKU')
        
        # Elimina duplicados
        df_md_product.drop_duplicates(subset=['SKU'], keep='first', inplace=True)

        len_init = len(df_update)
        suma_init=df_update["Total Sales"].astype(float).sum()

        #=========================================================
        # --- PROCESAMIENTO DE ARCHIVOS DE ACTUALIZACIÓN ---
        #=========================================================
        # Definir las columnas relevantes para el procesamiento. 

        lst_columns = ['Source System', 'Document Type','fk_Date','fk_year_month', 'Week','fk_Country','Country Detail',
                       'Sales Type', 'Sales Type Detail','Sales Type Invoince Country',
                       'fk_Sold_To_Customer_Code','fk_SKU',
                       'Total Sales', 'Total Cost', 'Units Sold','Units Return','NSV','FX Rate NSV','fx_nsv_financial','Outbound',
                       'fk_date_country_customer_clasification'
                      ]

        if df_update is None or df_update.empty:
            print("No hay archivos para actualizar. Finalizando proceso.")
            return

        df_update = process_columns_sales(df_update, lst_columns)
        print( "process columns")
        #=========================================================
        #--- ASIGNACIÓN COLUMNAS CALCULADAS
        #=========================================================
        df_update=assign_selling_unit_price(df_update)
        print( "assing selling price")
        print(f'{df_update['Total Sales'].astype(float).sum()}')
        print(f'{len(df_update)}')
        df_update=assign_NPI_New_Carryover(df_update,df_npi,df_country)
        print(f'{len(df_update)}')
        print(f'{df_update['Total Sales'].astype(float).sum()}')
        print( "assing npi")
        df_update=LaunchYear_VR(df_update,df_npi,df_country)
        print( "assing launch year")
        print(f'{df_update['Total Sales'].astype(float).sum()}')
        print(f'{len(df_update)}')
        df_update=assign_num_batteries(df_update,df_md_product)
        print( "assing num batteries")
        print(f'{df_update['Total Sales'].astype(float).sum()}')
        print(f'{len(df_update)}')
        df_update=assign_NSV_NPI_w_Combo(df_update,df_filter_npi)
        print( "assing npi w combo")
        print(f'{df_update['Total Sales'].astype(float).sum()}')
        print(f'{len(df_update)}')
        
        #====================================
        # --- Formato de columnas ---
        #====================================
        lst_columns_srt = ['Source System', 'Document Type','fk_Date','fk_year_month', 'Week','fk_Country','Country Detail',
                          'Sales Type', 'Sales Type Detail','Sales Type Invoince Country',
                          'fk_Sold_To_Customer_Code', 'fk_SKU',
                          'fk_date_country_customer_clasification',
                          'New New/Carryover',
                          'VR %','Launch Year']
        lst_columns_float = ['Total Sales', 'Total Cost', 'Units Sold','Units Return','NSV','FX Rate NSV',
                            'Selling Unit Price',
                            'NPI Incremental Sales $',
                            'Num Batteries Sales',
                            'Net Sales NPI w/Combo',
                            'fx_nsv_financial',
                            'Outbound']
        
        df_final=format_columns(df_update,lst_columns_srt,lst_columns_float)
        
        suma_end=df_final["Total Sales"].astype(float).sum()
        if len_init == len(df_final) and round(suma_init, 0) == round(suma_end, 0):
            print(f'{"*"*55}')
            print("La longitud del DataFrame procesado coincide con la del DataFrame original.")
            print(f'El dataframe original y final tienen la misma suma de ventas: {suma_init}')
            print(f'{"*"*55}')
        
        else:
            print(f'{"*"*55}')
            print("⚠️ ADVERTENCIA: Los totales o registros no coinciden.")
            print(f'longitud dataset inicial: {len_init}')
            print(f'longitud dataset procesado: {len(df_final)}')
            print(f'diferencia en registros: {len(df_final) - len_init}')
            
            diff_len = len(df_final) - len_init
            print(f'Porcentaje de diferencia (filas): {(diff_len / len_init) * 100:.2f}%' if len_init != 0 else "0%")

            print(f'la suma inicial es: {suma_init}')
            print(f'la suma final es: {suma_end}')
            print(f'la diferencia es: {suma_init-suma_end}')
            print(f'El porcentaje de diferencia es: {(suma_init-suma_end)/suma_init*100:.2f}%')
            print(f'{"*"*55}')



        # --- ESCRITURA DE LOS DATOS ACTUALIZADOS ---
        group_parquet(df_final, sales_historic_processed_dir,name='sales')
        print("Sales ETL Update completed successfully. ✅.")
        pass
    except Exception as e:
        print(f"Error en procesamiento de datos de Ventas: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
