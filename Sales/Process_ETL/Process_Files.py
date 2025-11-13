'''
#=======================================================
# 📦 MÓDULO ETL: ORQUESTACIÓN DE CARGA COMPLETA (FULL LOAD) DE VENTAS
#=======================================================

Propósito: 
    Este script funciona como el ORQUESTADOR (Pipeline Manager) del proceso ETL 
    (Extraer, Transformar, Cargar) para la consolidación histórica de los datos de Ventas.

Reutilización:
    Reutiliza funciones base de lectura, estandarización y escritura modular definidas 
    en el dominio de Fill Rate (Fill_Rate.Process_ETL.Process_Files), estandarizando 
    la metodología de procesamiento de datos transaccionales.

Enriquecimiento de Datos:
    Aplica una serie de transformaciones y cruces clave para enriquecer los datos brutos, 
    incluyendo:
    1. Atribución de NSV (Net Sales Value) mediante el factor G2N (Gross-to-Net).
    2. Clasificación de NPI (New Product Introduction) y asignación de VR (Valoración de Referencia).
    3. Cálculo de métricas secundarias (Precio Unitario, Baterías Vendidas, etc.).

Datos Maestros Utilizados:
    - Códigos de País (Country Codes)
    - Maestro de Productos (Master Data Product)
    - Tabla Gross-to-Net (G2N%)
    - Tablas de Clasificación NPI (New/Carryover, Combo %)

Salida:
    Archivos Parquet particionados por Año y Mes.

Funciones Propias de este Módulo (Cálculos y Transformaciones Específicas):
    - assign_nsv
    - assign_selling_unit_price
    - assign_NPI_New_Carryover
    - LaunchYear_VR
    - assign_num_batteries
    - assign_NSV_NPI_w_Combo
'''

#--------------------------------------------------
#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
# Liberia
import pandas as pd
import numpy as np
# Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico.
import glob
import os

# Importo funciones creadas que seran usadas nuevamente
from Fill_Rate.Process_ETL.Process_Files import read_files, asign_country_code, process_columns, group_parquet,format_columns


import pandas as pd # Asumo que pandas está importado

#====================================================
#--- FUNCIONES PARA CREAR COLUMNAS CALCULADAS
#====================================================

def assign_nsv(df_processed, df_md_product, df_gross_to_net):
    """
    Asigna la Venta Neta (NSV) al DataFrame principal (Sales/Demand) mediante dos cruces Left Join:
    1. Cruce con el Maestro de Productos para obtener Brand y GPP SBU.
    2. Cruce con el factor Gross-to-Net (G2N%) usando una clave compuesta de fecha/categoría.
    
    Args:
        df_processed (pd.DataFrame): DataFrame principal (ej. Sales) con la columna 'fk_SKU'.
        df_md_product (pd.DataFrame): Maestro de Productos con datos de clasificación (Brand, SBU).
        df_gross_to_net (pd.DataFrame): Tabla de referencia con el porcentaje G2N.
    
    Returns:
        pd.DataFrame: DataFrame original con la nueva columna 'NSV' calculada.
    """
    #  Estandarización y Renombre (Master Data)
    df_md_product=df_md_product.copy()
    df_md_product.rename(columns={'SKU':'SKU_x', 'Brand':'Brand_x', 'GPP SBU':'GPP SBU_x'}, inplace=True)
    df_md_product['SKU_x'] = df_md_product['SKU_x'].astype(str).str.upper().str.strip()

    # Aplicar estandarización a la clave de cruce del DataFrame principal
    df_processed['fk_SKU'] = df_processed['fk_SKU'].astype(str).str.upper().str.strip()

    # --- CRUCE 1: Obtener Brand y SBU del Master Product ---
    df_processed = pd.merge(
        df_processed,
        df_md_product[['SKU_x', 'Brand_x', 'GPP SBU_x']],
        how='left',
        left_on='fk_SKU',
        right_on='SKU_x',
    )
     
    # Renombrar columnas del G2N y aplicar inplace
    df_gross_to_net.rename(columns={'Date':'Date_y', 'Country':'Country_y', 'Brand':'Brand_y', 'SBU':'SBU_y'}, inplace=True)
    
    # Crear clave de cruce en el DataFrame principal
    df_processed['fk_g2n'] = (df_processed['fk_Date'].astype(str) + 
                             df_processed['fk_Country'].str[:3] + 
                             df_processed['Brand_x'] + 
                             df_processed['GPP SBU_x'])
    
    # Crear clave de cruce en el DataFrame Gross To Net
    df_gross_to_net['fk_g2n_y'] = (df_gross_to_net['Date_y'].astype(str) + 
                                   df_gross_to_net['Country_y'] + 
                                   df_gross_to_net['Brand_y'] + 
                                   df_gross_to_net['SBU_y'])
    
    df_processed['fk_g2n'] = df_processed['fk_g2n'].str.upper().str.strip()
    df_gross_to_net['fk_g2n_y'] = df_gross_to_net['fk_g2n_y'].str.upper().str.strip()
    
    # --- CRUCE 2: Obtener el G2N% ---
    df_processed = pd.merge(
        df_processed,
        df_gross_to_net[['fk_g2n_y', 'G2N%']], # Seleccionar fk_g2n_y como clave
        how='left',
        left_on='fk_g2n',
        right_on='fk_g2n_y',
    )
    
    
    df_processed['Total Sales'] = pd.to_numeric(df_processed['Total Sales'], errors='coerce').fillna(0)
    df_processed['G2N%'] = df_processed['G2N%'].fillna(0)
    df_processed['G2N%'] = df_processed['G2N%'].astype(float)
    df_processed['NSV'] = df_processed['Total Sales'] * (1 - df_processed['G2N%'])
    
    # 3. Limpieza y Retorno
    cols_to_drop = ['SKU_x','Brand_x', 'GPP SBU_x',  'Date_y', 'Country_y', 'Brand_y', 'SBU_y', 'fk_g2n_y', 'fk_g2n','G2N%']
    df_processed.drop(columns=[col for col in cols_to_drop if col in df_processed.columns], inplace=True)
    
    return df_processed

def assign_selling_unit_price(df_processed):
    """
    Calcula el Precio Unitario de Venta ('Selling Unit Price') para cada transacción.
    
    Asegura que los campos de cálculo ('Total Sales' y 'Units Sold') sean numéricos,
    sustituye nulos (NaN) por cero y utiliza np.where para evitar errores de 
    división por cero.

    Args:
        df_processed (pd.DataFrame): DataFrame principal (ej. Sales) que contiene
                                     las columnas 'Total Sales' y 'Units Sold'.

    Returns:
        pd.DataFrame: El DataFrame modificado con la nueva columna 
                      'Selling Unit Price'.
    """
    # --- Aseguramos el tipo de dato y completamos los nulos con 0
    df_processed['Total Sales'] = pd.to_numeric(df_processed['Total Sales'], errors='coerce').fillna(0)
    df_processed['Units Sold'] = pd.to_numeric(df_processed['Units Sold'], errors='coerce').fillna(0).astype(int)
    
    df_processed['Selling Unit Price'] = np.where(
        # Condición: Si Units Sold es mayor que 0
        df_processed['Units Sold'] > 0,     
        # Valor si es True: La división
        df_processed['Total Sales'] / df_processed['Units Sold'],
        # Valor si es False: 0
        0.0
    )
    
    return df_processed
    
def assign_NPI_New_Carryover(df_processed,df_npi):
    """
    Asigna las clasificaciones de Nuevos Productos (NPI) e información incremental.
    
    El cruce se realiza en base a una clave compuesta (año-mes, país, SKU), 
    y posteriormente calcula las ventas incrementales de NPI.

    Args:
        df_processed (pd.DataFrame): DataFrame principal (ej. Sales) con las claves 
                                     'fk_year_month', 'fk_Country', 'fk_SKU' y 'NSV'.
        df_npi (pd.DataFrame): Tabla de referencia de Nuevos Productos (NPI) 
                               que contiene 'fk_YearMonthCountrySku' y 'Incremental %'.

    Returns:
        pd.DataFrame: El DataFrame modificado con las columnas 'New New/Carryover', 
                      'Incremental %', y 'NPI Incremental Sales $'.
    """
    df_processed['fk_NPI'] = (df_processed['fk_year_month'].astype(str) + '-'+
                             df_processed['fk_Country'] + '-'+
                             df_processed['fk_SKU'])
    df_processed['fk_NPI'] = df_processed['fk_NPI'].str.upper().str.strip()
    df_npi=df_npi.copy()
    df_npi['fk_YearMonthCountrySku']=df_npi['fk_YearMonthCountrySku'].str.upper().str.strip()
    
    df_processed=pd.merge(
        df_processed,
        df_npi[['fk_YearMonthCountrySku','New New/Carryover','Incremental %']],
        how='left',
        left_on='fk_NPI',
        right_on='fk_YearMonthCountrySku',
    )
    df_processed['New New/Carryover']=df_processed['New New/Carryover'].fillna('Core')
    df_processed['NSV']=df_processed['NSV'].fillna(0)
    df_processed['NSV']=df_processed['NSV'].astype(float)
    df_processed['Incremental %'] = pd.to_numeric(df_processed['Incremental %'], errors='coerce')
    df_processed['Incremental %']=df_processed['Incremental %'].fillna(0)
    df_processed['Incremental %'] = df_processed['Incremental %'].astype(float)
    df_processed['NPI Incremental Sales $']=df_processed['NSV']*df_processed['Incremental %']
    cols_to_drop = ['fk_YearMonthCountrySku','Incremental %','fk_NPI']
    df_processed.drop(columns=[col for col in cols_to_drop if col in df_processed.columns], inplace=True)
    
    return df_processed

def LaunchYear_VR(df_processed,df_npi):
    """
    Clasifica cada registro como 'VR%' (NPI "New New") o 'Core' y asigna 
    el año de lanzamiento (Launch Year) del producto.

    Args:
        df_processed (pd.DataFrame): DataFrame principal (ventas/demanda).
        df_npi (pd.DataFrame): Tabla de referencia de Nuevos Productos (NPI).

    Returns:
        pd.DataFrame: DataFrame modificado con las columnas 'VR' y 'Launch Year'.
    """
    df_npi_new_new=df_npi[df_npi['New New/Carryover']=='New New'].copy()
    df_npi_new_new['fk_YearCountrySku']=(df_npi_new_new['Fiscal Year']+'-'+
                                         df_npi_new_new['Country']+'-'+
                                         df_npi_new_new['SKU'])
    df_npi_new_new['fk_YearCountrySku']=df_npi_new_new['fk_YearCountrySku'].str.upper().str.strip()

    df_processed['fk_YearCountrySku']=(df_processed['fk_year_month'].str[:4]+'-'+
                                       df_processed['fk_Country']+'-'+
                                       df_processed['fk_SKU'])
    df_processed['fk_YearCountrySku']=df_processed['fk_YearCountrySku'].str.upper().str.strip()

    df_processed['year']=df_processed['fk_year_month'].str[:4]
    df_processed['Launch Year']=""
    
    es_npi_new_new = df_processed['fk_YearCountrySku'].isin(df_npi_new_new['fk_YearCountrySku'])

    df_processed['VR'] = np.where(
    es_npi_new_new, 
    "VR%", 
    "Core"
    )
    
    df_processed['Launch Year'] = np.where(
    es_npi_new_new, 
    'NPI'+df_processed['year'], 
    "Core"
    )

    cols_to_drop = ['fk_YearCountrySku','year']
    df_processed.drop(columns=[col for col in cols_to_drop if col in df_processed.columns], inplace=True)
    
    return df_processed
    
def assign_num_batteries(df_processed,df_md_product):
    """
    Asigna la cantidad de baterías por SKU y calcula el total de baterías vendidas.

    Realiza un Left Join entre el DataFrame de transacciones y el Maestro de Productos 
    utilizando la clave SKU.

    Args:
        df_processed (pd.DataFrame): DataFrame principal (ej. Sales) con la clave 'fk_SKU' 
                                     y la métrica 'Units Sold'.
        df_md_product (pd.DataFrame): Maestro de Productos con 'SKU' y 'Batteries Qty'.

    Returns:
        pd.DataFrame: El DataFrame modificado con las columnas 'Batteries Qty' y 
                      'Num Batteries Sales'.
    """
    df_processed['fk_SKU'] = df_processed['fk_SKU'].astype(str).str.upper().str.strip()
    df_md_product['SKU'] = df_md_product['SKU'].astype(str).str.upper().str.strip()
    df_processed=pd.merge(
        df_processed,
        df_md_product[['SKU','Batteries Qty']],
        how='left',
        left_on='fk_SKU',
        right_on='SKU',
    )
    df_processed['Batteries Qty']=df_processed['Batteries Qty'].fillna(0)
    df_processed['Batteries Qty'] = pd.to_numeric(
        df_processed['Batteries Qty'], errors='coerce'
    ).fillna(0).astype(int)
    
    df_processed['Units Sold']=df_processed['Units Sold'].fillna(0)
    df_processed['Units Sold']=df_processed['Units Sold'].astype(int)
    
    df_processed['Num Batteries Sales']=df_processed['Batteries Qty']*df_processed['Units Sold']
    cols_to_drop = ['SKU','Batteries Qty']
    df_processed.drop(columns=[col for col in cols_to_drop if col in df_processed.columns], inplace=True)
    
    return df_processed

def assign_NSV_NPI_w_Combo(df_processed,df_filter_npi):
    """
    Calcula las Ventas Netas ajustadas por el factor 'Combo %' (asociado a NPI/promociones).
    
    El cruce se realiza a nivel de la clave compuesta: Año Fiscal, País y SKU.

    Args:
        df_processed (pd.DataFrame): DataFrame principal de ventas/demanda. Debe contener 
                                     'fk_year_month', 'fk_Country', 'fk_SKU' y 'NSV'.
        df_filter_npi (pd.DataFrame): Tabla de referencia NPI/Combo, conteniendo 
                                      'fk_YearCountrySku' y 'Combo %'.

    Returns:
        pd.DataFrame: El DataFrame modificado con la columna calculada 
                      'Net Sales NPI w/Combo'.
    """
    df_processed['fk_YearCountrySku']=(df_processed['fk_year_month'].str[:4]+'-'+
                                       df_processed['fk_Country']+'-'+
                                       df_processed['fk_SKU'])
    df_processed['fk_YearCountrySku']=df_processed['fk_YearCountrySku'].str.upper().str.strip()
    df_filter_npi['fk_YearCountrySku']=df_filter_npi['fk_YearCountrySKU'].str.upper().str.strip()
    df_processed=pd.merge(
        df_processed,
        df_filter_npi[['fk_YearCountrySku','Combo %']],
        how='left',
        left_on='fk_YearCountrySku',
        right_on='fk_YearCountrySku'
    )
    df_processed['Combo %'] = pd.to_numeric(df_processed['Combo %'], errors='coerce')
    df_processed['Combo %']=df_processed['Combo %'].fillna(1)
    df_processed['Combo %']=df_processed['Combo %'].astype(float)
    df_processed['NSV']=df_processed['NSV'].fillna(0)
    df_processed['NSV']=df_processed['NSV'].astype(float)
    df_processed['Net Sales NPI w/Combo']=df_processed['NSV']*df_processed['Combo %']
    df_processed['Net Sales NPI w/Combo']=df_processed['Net Sales NPI w/Combo'].fillna(0)
    cols_to_drop = ['fk_YearCountrySku','Combo %']
    df_processed.drop(columns=[col for col in cols_to_drop if col in df_processed.columns], inplace=True)
    
    return df_processed

def main():
    """
    Función principal que orquesta el flujo ETL completo para los datos de Ventas (Sales).
    Define las rutas de entrada/salida y las columnas de métricas específicas
    ('Total Sales', 'Total Cost', 'Units Sold') antes de ejecutar el pipeline reutilizado.
    Returns: None: La función orquesta el proceso y no devuelve un valor.
    """
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: SALES FULL LOAD ETL ---")
    print("=" * 55)
    # --- CONFIGURACIÓN DE RUTAS ---
    # Importamos las rutas
    from config_paths import SalesPaths
    sales_historic_raw_dir = SalesPaths.INPUT_RAW_HISTORIC_DIR
    #sales_historic_raw_dir=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Sales\Historic\prueba'
    country_code_file = SalesPaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
    processed_gross_to_net=SalesPaths.INPUT_PROCESSED_GROSS_TO_NET_FILE
    npi=SalesPaths.INPUT_PROCESSED_NPI_FILE
    filter_npi=SalesPaths.INPUT_PROCESSED_FILTER_NPI_FILE
    md_product_processed_file=SalesPaths.INPUT_PROCESSED_MASTER_PRODUCTS_FILE
    processed_parquet_dir = SalesPaths.OUTPUT_PROCESSED_PARQUETS_DIR
    #===============================
    # --- Lectura de archivos 
    #===============================
    df_consolidated = read_files(sales_historic_raw_dir)
    # Leer el archivo de códigos de país.
    df_country = pd.read_excel(country_code_file,
                               sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
    
    df_md_product=pd.read_excel(md_product_processed_file,dtype=str, engine='openpyxl')
    df_gross_to_net=pd.read_excel(processed_gross_to_net,dtype=str, engine='openpyxl')
    df_npi=pd.read_excel(npi,dtype=str, engine='openpyxl')
    df_filter_npi=pd.read_excel(filter_npi,dtype=str, engine='openpyxl')
    
    # Definir las columnas relevantes para el procesamiento.    
    lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                   'fk_date_country_customer_clasification',
                   'Total Sales', 'Total Cost', 'Units Sold']
    df_consolidated = asign_country_code(df_consolidated, df_country)
    df_processed=process_columns(df_consolidated,lst_columns)
    #=========================================================
    #--- ASIGNACIÓN COLUMNAS CALCULADAS
    #=========================================================
    df_processed=assign_nsv(df_processed,df_md_product,df_gross_to_net)
    df_processed=assign_selling_unit_price(df_processed)
    df_processed=assign_NPI_New_Carryover(df_processed,df_npi)
    df_processed=LaunchYear_VR(df_processed,df_npi)
    df_processed=assign_num_batteries(df_processed,df_md_product)
    df_processed=assign_NSV_NPI_w_Combo(df_processed,df_filter_npi)
    
    #====================================
    # --- Formato de columnas ---
    #====================================
    lst_columns_srt = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                   'fk_date_country_customer_clasification',
                   'New New/Carryover','VR','Launch Year']
    lst_columns_float = ['Total Sales', 'Total Cost', 'Units Sold',
                         'NSV','Selling Unit Price',
                         'NPI Incremental Sales $',
                         'Num Batteries Sales',
                         'Net Sales NPI w/Combo']
    df_processed=format_columns(df_processed,lst_columns_srt,lst_columns_float)
    
    #  --- ESCRITURA DE ARCHIVOS PARQUET SEGMENTADOS --
    group_parquet(df_processed, processed_parquet_dir,name='sales')
    #group_parquet(df_processed, sales_historic_raw_dir,name='sales')


# --- EJECUCION DEL SCRIPT ---
# Es una buena práctica envolver la ejecución principal en un bloque if __name__ == "__main__":
if __name__ == "__main__":
    main()
    print("Processing of historical Sales data completed successfully. ✅.")
