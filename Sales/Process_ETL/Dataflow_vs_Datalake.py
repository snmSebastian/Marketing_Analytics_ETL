
# Librerias
import pandas as pd
import glob
import os
import sys
from pathlib import Path

from Sales.Process_ETL.Update import read_files_parquets

def read_files_parquets_proccesed(input_path,df,name_file,metrics):
    """
    Lee archivos Parquet del directorio que contiene los archivos ya procesados
    Leo solo los archivos cuyo year-month, contiene una semana que debe ser actualizada o agregada
    
    Args:
        input_path (str): Ruta del directorio donde se encuentran los archivos Parquets (.parquet) a consolidar.
        df_new (pd.DataFrame): DataFrame con la data nueva para identificar meses requeridos.

    Returns:
        pd.DataFrame or None: DataFrame consolidado con todos los datos de los archivos, o None si no se encuentran
        archivos o la lectura falla sin consolidar nada.
     """
    lst_year_month = df['fk_year_month'].unique().tolist()
    lst_year_month.sort()

    lst_files=[]
    for date in lst_year_month:
        lst_files.append(name_file+date+'.parquet')
    # --- LECTURA Y CONSOLIDACION DE ARCHIVOS ---
    all_files_parquets = glob.glob(os.path.join(input_path, "*.parquet"))


    archivos_filtrados = [
    archivo for archivo in all_files_parquets 
    if os.path.basename(archivo) in lst_files
    ]

    if not archivos_filtrados:
        print(f"Info: No se encontraron archivos históricos previos para los meses solicitados en '{input_path}'.")
        # Retornamos un DataFrame vacío con las mismas columnas para no romper el concat posterior
        return pd.DataFrame(columns=df.columns)

    # Leer cada archivo y agregarlo a una lista de DataFrames:
    lst_files_xlsx = []
    for filename in archivos_filtrados:
        try:
            print(f"Leyendo archivo: {os.path.basename(filename)}")
            # Evitar sobrescribir la variable 'df' que viene como argumento
            df_temp = pd.read_parquet(filename)
            lst_files_xlsx.append(df_temp)
        except PermissionError:
            print(f"  [ERROR] Permiso denegado para leer el archivo: {os.path.basename(filename)}."
                  "\n  Asegúrate de que no esté abierto y vuelve a intentarlo.")
        except Exception as e:
            print(f"  [ERROR] No se pudo procesar el archivo {os.path.basename(filename)}: {e}")

    # Concatenar todos los DataFrames en uno solo
    if not lst_files_xlsx:
        return pd.DataFrame(columns=df.columns)

    df_consolidated = pd.concat(lst_files_xlsx, axis=0, ignore_index=True)

    # Normalización robusta: Convertimos métricas a float y fechas a datetime.
    # Solo aplicamos transformaciones de texto (upper/strip) a columnas que no son métricas ni fechas.
    for col in df_consolidated.columns:
        if col in metrics:
            df_consolidated[col] = pd.to_numeric(df_consolidated[col], errors='coerce').fillna(0.0)
        elif col == 'Date':
            df_consolidated[col] = pd.to_datetime(df_consolidated[col], errors='coerce')
        else:
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
        historicalChanges=SalesPaths.INPUT_RAW_HISTORICAL_CHANGES_FILE
        sales_dataflow=SalesPaths.OUTPUT_PROCESSED_PARQUETS_DIR
        #===============================
        # --- Lectura de archivos 
        #===============================
        df_datalake = read_files_parquets(sales_update_raw_dir)
        metrics=['Total Sales','NSV']
        df_dataflow=read_files_parquets_proccesed(sales_dataflow,df_datalake,'sales_',metrics)

        df_datalake=df_datalake[['Region Consolidated','fk_Country','fk_year_month','Total Sales','NSV']]
        df_dataflow=df_dataflow[['fk_Country','fk_year_month','Total Sales','NSV']]
        
        df_datalake['Total Sales']=df_datalake['Total Sales'].astype(float)
        df_datalake['NSV']=df_datalake['NSV'].astype(float)
        df_dataflow['Total Sales']=df_dataflow['Total Sales'].astype(float)
        df_dataflow['NSV']=df_dataflow['NSV'].astype(float)


        df_datalake=df_datalake.rename(columns={'Region Consolidated':'Region',
                                         'fk_Country':'Country',
                                         'fk_year_month':'Year Month',
                                         'Total Sales':'GSV'})
        
        df_dataflow=df_dataflow.rename(columns={
                                         'fk_Country':'Country',
                                         'fk_year_month':'Year Month',
                                         'Total Sales':'GSV'})
        
        df_group_datalake=(df_datalake.groupby(['Year Month','Country']).
                    agg({
                        'GSV':'sum',
                        'NSV':'sum'
                        }).reset_index())
        df_group_dataflow=(df_dataflow.groupby(['Year Month','Country']).
                    agg({
                        'GSV':'sum',
                        'NSV':'sum'
                        }).reset_index())
        df_result=pd.merge(df_group_dataflow,df_group_datalake,how='left',on=['Year Month','Country'],suffixes=('_dataflow', '_datalake'))
        
        df_result=df_result[['Year Month','Country','GSV_dataflow','NSV_dataflow','GSV_datalake','NSV_datalake']]
        
        df_result['Dif GSV']=df_result['GSV_dataflow']-df_result['GSV_datalake']
        df_result['Dif NSV']=df_result['NSV_dataflow']-df_result['NSV_datalake']
        df_result['% Dif GSV']=df_result['Dif GSV']/df_result['GSV_datalake']*100
        df_result['% Dif NSV']=df_result['Dif NSV']/df_result['NSV_datalake']*100

        df_result=df_result.sort_values(by=['Country','Year Month',])
        df_result.to_excel(historicalChanges,index=False)

        print("Historical Changes ETL Update completed successfully. ✅.")





    except Exception as e:
        print(f"Error en procesamiento de datos de Ventas: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()