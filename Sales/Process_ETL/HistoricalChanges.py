
# Librerias
import pandas as pd
import glob
import os
import sys
from pathlib import Path

from Sales.Process_ETL.Update import read_files_parquets

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
        
        #===============================
        # --- Lectura de archivos 
        #===============================
        print('lectura sales query.parquet')
        df_update = read_files_parquets(sales_update_raw_dir)
        print('lectura historical changes')
        df_historical_changes=pd.read_excel(historicalChanges,dtype=str, engine='openpyxl')
        print('agrupamiento de datos')
        print(df_historical_changes.head())
        df_update['Total Sales']=df_update['Total Sales'].astype(float)
        print('change type')
        df_update['NSV']=df_update['NSV'].astype(float)
        df_update=df_update.rename(columns={'Region Consolidated':'Region',
                                         'fk_Country':'Country',
                                         'fk_year_month':'Year Month',
                                         'Total Sales':'GSV'})
        
        
        
        df_group=(df_update.groupby(['Year Month','Region','Country']).
                    agg({
                        'GSV':'sum',
                        'NSV':'sum'
                        }).reset_index())
        print('fecha ejecucion query')
        df_group['Date Query']=pd.Timestamp.now()
        
        df_group=df_group[['Date Query','Year Month','Region','Country','GSV','NSV']]
        print('concat')
        df_result=pd.concat([df_historical_changes,df_group])
        df_result=df_result.sort_values(by=['Country','Date Query','Year Month',],ascending=[True, False, True])
        df_result.to_excel(historicalChanges,index=False)

        print("Historical Changes ETL Update completed successfully. ✅.")





    except Exception as e:
        print(f"Error en procesamiento de datos de Ventas: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()