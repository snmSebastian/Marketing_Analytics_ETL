# Librerias
import pandas as pd
import numpy as np
import glob
import os
import sys
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
# Biblioteca para ver barra de progreso en consola (Instalar con: pip install tqdm)
from tqdm import tqdm 

# La importación debe ser relativa al paquete actual.
from Fill_Rate.Process_ETL.Process_Files import  group_parquet,format_columns,clean_sku
from Sales.Process_ETL.Update import (read_files_parquets)


#=======================================
#---- SELL IN -----
#=======================================
def process_sell_in(paths_sell_in, df_products, df_customers):
    # 1. Leer archivos parquet de forma eficiente
    df_sell_in = read_files_parquets(paths_sell_in)
    
    # 2. Filtrar columnas iniciales para ahorrar memoria
    cols_sell_in = ['Week', 'fk_Country', 'fk_SKU', 'fk_Sold_To_Customer_Code','Total Sales']
    df_sell_in = df_sell_in[cols_sell_in]
    
    # 3. Primer Merge: Productos
    cols_prod = ['SKU', 'Brand', 'GPP SBU', 'GPP Category Description', 'Corded / Cordless', 'Dewalt XR', 'Power Connect']
    df_sell_in = pd.merge(
        df_sell_in,
        df_products[cols_prod],
        left_on='fk_SKU',
        right_on='SKU',
        how='left'
    )
    
    df_sell_in['Week'] = df_sell_in['Week'].astype(str)
    
    # Convertimos temporalmente a numérico para el filtro de fecha
    week_numeric = pd.to_numeric(df_sell_in['Week'], errors='coerce').fillna(0)
    
    # Aplicar filtros combinados
    brands_validas = ['DEWALT', 'BLACK + DECKER', 'STANLEY', 'IRWIN']
    filtro_brand = df_sell_in['Brand'].str.upper().isin(brands_validas)
    filtro_week = week_numeric > 202401
    
    df_sell_in = df_sell_in[filtro_brand & filtro_week]
    
    # 4. Segundo Merge: Clientes
    cols_cust = ['fk_Sold-To Customer Code', 'Sold-To Customer Name', 'fk_Dist_Type']
    df_sell_in = pd.merge(
        df_sell_in,
        df_customers[cols_cust],
        left_on='fk_Sold_To_Customer_Code',
        right_on='fk_Sold-To Customer Code',
        how='left'
    )
    
    # 5. Seleccionar y ordenar columnas finales
    lst_columns = [
        'Week', 'fk_Country', 'fk_SKU', 'fk_Sold_To_Customer_Code', 'Brand',
        'GPP SBU', 'GPP Category Description', 'Corded / Cordless', 'Dewalt XR', 
        'Power Connect', 'Sold-To Customer Name', 'fk_Dist_Type','Total Sales'
    ]
    df_sell_in = df_sell_in[lst_columns]

    df_sell_in=df_sell_in.rename(columns={
        'fk_Country':'Country',
        'GPP SBU':'SBU',
        'GPP Category Description':'Category',
        'Sold-To Customer Name':'Customer Name',
        'fk_Dist_Type':'Channel'
    })
    
    # 6. Separar subconjuntos por año de forma eficiente
    df_sellin_2024 = df_sell_in[df_sell_in['Week'].str.startswith('2024')]
    df_sellin_2025 = df_sell_in[df_sell_in['Week'].str.startswith('2025')]
    df_sellin_2026 = df_sell_in[df_sell_in['Week'].str.startswith('2026')]

    lst_df_sell_in=[df_sellin_2024, df_sellin_2025, df_sellin_2026]
    #lst_df_sell_in=[df_sellin_2025]
        
    return lst_df_sell_in

#=======================================
#---- SELL OUT -----
#=======================================
def process_sell_out(path_sell_out, df_products):
    # 1. Leer archivos parquet de forma eficiente
    df_sell_out = read_files_parquets(path_sell_out)
    
    # 2. Filtrar columnas iniciales para ahorrar memoria
    cols_sell_out = ['Year-Week', 'Country','Brand', 'SKU', 'Retailer','canal_venta','Venta bruta']
    df_sell_out = df_sell_out[cols_sell_out]

    df_sell_out['Year-Week'] = df_sell_out['Year-Week'].str.replace('-', '', regex=False)
    df_sell_out=df_sell_out.rename(columns={'Brand':'Brand_x', 'Year-Week':'Week'})
    # 3. Primer Merge: Productos
    cols_prod = ['SKU', 'Brand', 'GPP SBU', 'GPP Category Description', 'Corded / Cordless', 'Dewalt XR', 'Power Connect']
    df_sell_out = pd.merge(
        df_sell_out,
        df_products[cols_prod],
        on='SKU',
        how='left'
    )
    #Completo Brands
    df_sell_out['Brand_final'] = np.where(
    df_sell_out['Brand_x'].str.upper() == 'OTHER', # Condición fila por fila
    df_sell_out['Brand'],                          # Si es Verdadero
    df_sell_out['Brand_x']                         # Si es Falso
    )

    df_sell_out=df_sell_out.drop(['Brand_x', 'Brand'], axis=1)
    df_sell_out=df_sell_out.rename(columns={'Brand_final':'Brand'})

    
    df_sell_out['Week'] = df_sell_out['Week'].astype(str)
    
    # Convertimos temporalmente a numérico para el filtro de fecha
    week_numeric = pd.to_numeric(df_sell_out['Week'], errors='coerce').fillna(0)
    
    # Aplicar filtros combinados
    brands_validas = ['DEWALT', 'BLACK + DECKER', 'STANLEY', 'IRWIN']
    filtro_brand = df_sell_out['Brand'].str.upper().isin(brands_validas)
    filtro_week = week_numeric > 202401
    
    df_sell_out = df_sell_out[filtro_brand & filtro_week]
    
  
    
    # 5. Seleccionar y ordenar columnas finales
    lst_columns = [
        'Week', 'Country', 'SKU',  'Brand',
        'GPP SBU', 'GPP Category Description', 'Corded / Cordless', 'Dewalt XR', 
        'Power Connect', 'Retailer', 'canal_venta','Venta bruta'
    ]
    df_sell_out = df_sell_out[lst_columns]

    df_sell_out=df_sell_out.rename(columns={
        'GPP SBU':'SBU',
        'GPP Category Description':'Category',
        'Retailer':'Customer Name',
        'canal_venta':'Channel',
        'Venta bruta':'Total Sales'
    })
    
    # 6. Separar subconjuntos por año de forma eficiente
    df_sellout_2024 = df_sell_out[df_sell_out['Week'].str.startswith('2024')]
    df_sellout_2025 = df_sell_out[df_sell_out['Week'].str.startswith('2025')]
    df_sellout_2026 = df_sell_out[df_sell_out['Week'].str.startswith('2026')]

    lst_df_sell_out=[df_sellout_2024, df_sellout_2025, df_sellout_2026]
    #lst_df_sell_out=[df_sellout_2025]
    # Opción correcta y limpia
    

    return lst_df_sell_out

def campaigns_by_year(path_campaigns):
    # 1. Leer el archivo Excel de forma normal
    df_campaigns = pd.read_excel(path_campaigns, sheet_name='Sheet1', dtype=str, engine='openpyxl')
    
    # Se extraen los primeros 11 caracteres y se transforman a formato datetime
    df_campaigns['Date'] = pd.to_datetime(df_campaigns['Semana'].str[:10], errors='coerce')
    
    # .dt.strftime('%G%V') genera el año y número de semana ISO como string (ej. "202401")
    df_campaigns['Week'] = df_campaigns['Date'].dt.strftime('%G%V')
    
    # 2. Separar los subconjuntos por año usando el string de la columna 'Week'
    df_campaigns_2024 = df_campaigns[df_campaigns['Week'].str.startswith('2024', na=False)]
    df_campaigns_2025 = df_campaigns[df_campaigns['Week'].str.startswith('2025', na=False)]
    df_campaigns_2026 = df_campaigns[df_campaigns['Week'].str.startswith('2026', na=False)]
    
    # 3. Retornar la lista con los DataFrames filtrados
    lst_df_campaigns = [df_campaigns_2024, df_campaigns_2025, df_campaigns_2026]
    #lst_df_campaigns = [ df_campaigns_2025]

    return lst_df_campaigns

def select_columns(row):
    """
    Recibe una fila del DataFrame.
    Retorna la lista de columnas cuyo valor NO es 'ALL'.
    """
    # 1. Definir el mapeo entre la variable y el nombre exacto de la columna
    mapeo_columnas = {
        'Week':row['Week'],
        'Country': row['Country'],
        'Brand':row['Brand'],

        'SBU': row['SBU'],
        'Category': row['Category'],
        'Corded / Cordless': row['Corded / Cordless'],
        'Power Connect': row['Power Connect'],
        'Dewalt XR': row['Dewalt XR'],
        'Customer Name': row['Customer Name'],
        'Channel': row['Channel']
    }
    
    lst_columns = [col for col, val in mapeo_columnas.items() if str(val).upper().strip() != 'ALL']

    
    return lst_columns

def get_sales(row, df_sellin):
    # 1. Obtener la lista de columnas que NO tienen valor 'ALL'
    lst_columns_filter = select_columns(row)
    
    # 2. Hacer una copia temporal de la matriz de Sell-In para aplicar los filtros
    df_filtered = df_sellin.copy()
    
    # 3. FILTRADO FILA POR FILA:
    for col in lst_columns_filter:
        # Obtener el valor del filtro, limpiar espacios y pasarlo a mayúsculas
        valor_original = str(row[col]).upper().strip()
        
        # Normalizar la columna correspondiente del Sell-In (Texto, Mayúsculas, sin espacios)
        columna_normalizada = df_filtered[col].astype(str).str.upper().str.strip()
        
        # CASO ESPECIAL: Si el valor contiene guiones (ej: 'PWT-FAS-ANF')
        if '-' in valor_original:
            # Separamos el texto por el guion para crear una lista: ['PWT', 'FAS', 'ANF']
            lista_valores = [v.strip() for v in valor_original.split('-')]
            
            # Filtramos usando .isin() para conservar cualquiera de esos valores
            df_filtered = df_filtered[columna_normalizada.isin(lista_valores)]
        
        # CASO NORMAL: Valor único (ej: 'PWT' o 'MEXICO')
        else:
            df_filtered = df_filtered[columna_normalizada == valor_original]
            
        
    # 4. Sumar la columna 'Total Sales' únicamente de los registros que pasaron todos los filtros
    total_sales = pd.to_numeric(df_filtered['Total Sales'], errors='coerce').sum()
    
    return total_sales

def _parallel_worker(df_chunk, df_sellin):
    """Procesa un fragmento del DataFrame de campañas."""
    df_chunk['Sell-In'] = df_chunk.apply(lambda row: get_sales(row, df_sellin), axis=1)
    return df_chunk

def execute_sales(lst_campaigns, lst_sellin):
    # Lista vacía para acumular los DataFrames de todos los años si decides unificarlos
    all_years_results = []
    
    for campaing, sellin in zip(lst_campaigns, lst_sellin):
        df_campaigns = campaing.copy()
        df_sellin = sellin.copy()
        
        # Validación de seguridad: Si el año actual no tiene datos, saltar al siguiente
        if df_campaigns.empty or df_sellin.empty:
            print("Saltando un bloque por falta de registros.")
            continue
            
        num_cores =  min(4, os.cpu_count() or 4)
        print(f"Utilizando {num_cores} núcleos de procesamiento.")

        # Dividir df_campaigns en partes iguales
        df_chunks = np.array_split(df_campaigns, num_cores)

        # Lanzar los procesos en paralelo reales
        results = []
        with ProcessPoolExecutor(max_workers=num_cores) as executor:
            futures = [executor.submit(_parallel_worker, chunk, df_sellin) for chunk in df_chunks]
            
            for future in tqdm(futures, desc="Procesando registros"):
                results.append(future.result())

        # Volver a unir los fragmentos procesados del año ACTUAL
        df_campaigns_final = pd.concat(results, ignore_index=True)
        
        all_years_results.append(df_campaigns_final)
        
    # Fuera del bucle for: Consolidar y retornar el resultado de todos los años juntos
    if all_years_results:
        df_final_consolidado = pd.concat(all_years_results, ignore_index=True)
        return df_final_consolidado
    else:
        print("No se procesó ningún dato.")
        return pd.DataFrame()

def channel_customers(lst):
    for df in lst:
        # Validación por si el DataFrame viene vacío
        if df.empty:
            continue
            
        # Asegurar que la columna sea string y esté en mayúsculas para las búsquedas
        cust_upper = df['Customer Name'].astype(str).str.upper()
        
        # 1. OPTIMIZACIÓN: Estandarizar 'Customer Name' usando np.select (más rápido que anidar np.where)
        condiciones_clientes = [
            cust_upper.str.contains('EASY', na=False),
            cust_upper.str.contains('COPPEL', na=False),
            cust_upper.str.contains('LIVERPOOL', na=False)
        ]
        resultados_clientes = ['EASY', 'COPPEL', 'LIVERPOOL']
        
        df['Customer Name'] = np.select(condiciones_clientes, resultados_clientes, default=df['Customer Name'])
        
        # Volver a capturar los nombres ya limpios para la asignación del canal
        cust_clean_upper = df['Customer Name'].astype(str).str.upper()
        
        # 2. CORRECCIÓN Y ASIGNACIÓN DE CANAL:
        condiciones_canales = [
            cust_clean_upper.str.contains('LIVERPOOL', na=False),
            cust_clean_upper.str.contains('COPPEL', na=False),
            cust_clean_upper.str.contains('EASY', na=False)
        ]
        # CORRECCIÓN: Ajustado a 'MODERN CHANNEL' y removido el error de duplicación
        resultados_canales = ['MODERM CHANNEL', 'MODERM CHANNEL', 'MODERM CHANNEL'] 
        
        df['Channel'] = np.select(condiciones_canales, resultados_canales, default=df['Channel'])

    # CORRECCIÓN CRÍTICA: El return debe ir FUERA del bucle for para que procese toda la lista
    return lst
      
def main():
    try:
        from config_paths import SalesPaths,MasterCustomersPaths, Campaigns_PATHS, Datamind_PATHS       
        country_code_file = SalesPaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
        md_product_processed_file=SalesPaths.INPUT_PROCESSED_MASTER_PRODUCTS_FILE
        md_customers=MasterCustomersPaths.OUTPUT_FILE_PROCESSED_MASTER_CUSTOMERS_FILE
        #sales_historic_processed_dir =r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace\Data\Processed-Dataflow\Campaigns\sell-in-prueba'

        path_campaigns=Campaigns_PATHS.OUTPUT_FILE_PROCESSED_EXCEL_CAMPAIGNS
        sales_historic_processed_dir =SalesPaths.OUTPUT_PROCESSED_PARQUETS_DIR
        path_sell_out=Datamind_PATHS.OUTPUT_PROCESSED_PARQUETS_DIR
        #path_sell_out=r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace\Data\Processed-Dataflow\Campaigns\sellout-prueba'
        # --- Lectura de archivos 
        #===============================

        df_product=pd.read_excel(md_product_processed_file,dtype=str, engine='openpyxl')
        df_customers=pd.read_excel(md_customers,dtype=str, engine='openpyxl')
        print('read listo')    

        #lst_df_sellin=process_sell_in(sales_historic_processed_dir,df_product,df_customers)
        lst_df_sellout=process_sell_out(path_sell_out,df_product)
        lst_df_sellout=channel_customers(lst_df_sellout)
        lst_df_campaigns=campaigns_by_year(path_campaigns)

       
        df_campaigns_final_sellout = execute_sales(lst_df_campaigns,lst_df_sellout)
        df_campaigns_final_sellout=df_campaigns_final_sellout.rename(columns={'Sell-In':'Sell-Out'})
        #df_campaigns_final_sellin = execute_sales(lst_df_campaigns,lst_df_sellin)

        ruta=r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace\Data\Processed-Dataflow\Campaigns\exito.xlsx'
        df_campaigns_final_sellout.to_excel(ruta)
        print('exito')

    except Exception as e:
        print(f"Error en procesamiento de datos de Ventas: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
