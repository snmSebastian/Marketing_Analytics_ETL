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
def process_columns_sales(df_consolidated,lst_columns):
    """    
        Renombra, calcula columnas clave ('fk_year_month', 'clasification', 'fk_date_country_customer_clasification',
        'fk_Date'), y selecciona el subconjunto final de columnas para el DataFrame procesado.
    Args:
        df_consolidated (pd.DataFrame): DataFrame consolidado que contiene todas las columnas sin procesar.
        lst_columns (list): Lista de strings con los nombres de las columnas finales deseadas, incluyendo las recién creadas (e.g., 'fk_Date', 'fk_Country').
    Returns:
        pd.DataFrame: DataFrame final, filtrado por lst_columns, listo para ser guardado.
    """
    try:

        
        
        df_consolidated['clasification']=(df_consolidated['GPP Division'] + '-' +
                                         df_consolidated['GPP Category'] + '-' +
                                         df_consolidated['GPP Portafolio'])
        
        
        df_consolidated['fk_date_country_customer_clasification'] = (df_consolidated['fk_year_month'] + '-' +
                                                            df_consolidated['fk_Country']+ '-' +
                                                            df_consolidated['fk_Sold_To_Customer_Code']+ '-' +
                                                            df_consolidated['clasification']).str.upper().str.strip()
        
        
        
        df_processed = df_consolidated[lst_columns].copy()                                                                                                                                                                           
        # Convertir todas las columnas a mayúsculas y eliminar espacios
        for col in df_processed.columns:        
            df_processed.loc[:,col] = df_consolidated[col].astype(str).str.upper().str.strip()    
       
    except KeyError as e:
                print(f"Error: La columna {e} no se encontró en los archivos. ")
    return df_processed

def assign_nsv(df_processed, df_md_product, df_gross_to_net,df_country):
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
    df_md_product.drop_duplicates(subset=['SKU_x'], inplace=True)
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
    # CRUCE 2: Obtener la region de venta, dado que gtonet tiene cca como pais(no cada uno de los paises)
    df_country['CountryRegion']=df_country['Country']+'-'+df_country['Region']
    df_country['CountryRegion']=df_country['CountryRegion'].str.upper().str.strip()
    df_country = df_country.drop_duplicates(subset=['CountryRegion'], keep='first')

    df_processed=pd.merge(df_processed,
                       df_country[['Country','Region']],
                       how='left',
                       left_on='fk_Country',
                       right_on='Country')
   
   
    # Crear clave de cruce en el DataFrame principal
    df_processed['fk_g2n'] = (df_processed['fk_Date'].astype(str) + 
                             df_processed['Region'].str[:3] + 
                             df_processed['Brand_x'] + 
                             df_processed['GPP SBU_x'])
    # Renombrar columnas del G2N y aplicar inplace
    df_gross_to_net.rename(columns={'Date':'Date_y', 'Country':'Country_y', 'Brand':'Brand_y', 'SBU':'SBU_y'}, inplace=True)

    # Crear clave de cruce en el DataFrame Gross To Net
    df_gross_to_net['fk_g2n_y'] = (df_gross_to_net['Date_y'].astype(str) + 
                                   df_gross_to_net['Country_y'] + 
                                   df_gross_to_net['Brand_y'] + 
                                   df_gross_to_net['SBU_y'])
    df_gross_to_net.drop_duplicates(subset=['fk_g2n_y'], inplace=True)
    
    df_processed['fk_g2n'] = df_processed['fk_g2n'].str.upper().str.strip()
    df_gross_to_net['fk_g2n_y'] = df_gross_to_net['fk_g2n_y'].str.upper().str.strip()
    
    # --- CRUCE 3: Obtener el G2N% ---
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
    cols_to_drop = ['SKU_x','Brand_x', 'GPP SBU_x',
                    'Country','Region',
                    'Date_y', 'Country_y', 'Brand_y', 'SBU_y', 'fk_g2n_y', 'fk_g2n','G2N%']
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
    
def assign_NPI_New_Carryover(df_processed,df_npi,df_country):
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
    #Elimina duplicados en df_npi
    df_country['CountryRegion']=df_country['Country']+'-'+df_country['Region']
    df_country['CountryRegion']=df_country['CountryRegion'].str.upper().str.strip()
    df_country = df_country.drop_duplicates(subset=['CountryRegion'], keep='first')
    # Asignacion de la region segun pais
    df_processed=pd.merge(
        df_processed,
        df_country[['Country','Region']],
        how='left',
        left_on='fk_Country',
        right_on='Country',
    )
    
    # Para aquellos paises de cca y pub, su pais asignado es la region
    condiciones=[
        df_processed['Region'].isin(['CCA','PUB']),
    ]
    valores=[
        df_processed['Region']
    ]
    df_processed['fk_CountryRegion'] = np.select(condiciones, valores, default=df_processed['fk_Country'])

    # llave para cruzar ventas con npi
    df_processed['fk_NPI'] = (df_processed['fk_year_month'].astype(str) + '-'+
                             df_processed['fk_CountryRegion'] + '-'+
                             df_processed['fk_SKU'])
    df_processed['fk_NPI'] = df_processed['fk_NPI'].str.upper().str.strip()
    #clave para cruzar sales con npi
    df_npi=df_npi.copy()
    df_npi['fk_YearMonthCountrySku']=df_npi['fk_YearMonthCountrySku'].str.upper().str.strip()
    #cruce para obtener New New/Carryover y Incremental %
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
    cols_to_drop = ['fk_YearMonthRegionySku','Incremental %','fk_NPI','Country','Region']
    df_processed.drop(columns=[col for col in cols_to_drop if col in df_processed.columns], inplace=True)
    
    return df_processed

def LaunchYear_VR(df_processed,df_npi,df_country):
    """
    Asigna clasificación de 'Launch Year' y 'VR %' al DataFrame principal mediante cruces Left Join:
    1. Cruce con Country para obtener la Región.
    2. Cruce con Maestro NPI usando llave compuesta (Año-Región-SKU) para coincidencia estricta.
    
    Args:
        df_processed (pd.DataFrame): DataFrame principal con 'fk_year_month', 'fk_Country', 'fk_SKU'.
        df_npi (pd.DataFrame): Maestro de NPI filtrado por 'New New'.
        df_country (pd.DataFrame): Maestro de Países.
    
    Returns:
        pd.DataFrame: DataFrame original con columnas 'VR %' y 'Launch Year'.
    """
    # ======== TRATAMIENTO NPI ==========================================
    # año des, para determinar el rango de NPI
    start_year = 2021
    valid_years = list(range(start_year, start_year + 7)) # [2021, ..., 2027]
    #filtra npi para obtener solo los nuevos productos de los ultimos 3 años
    mask_npi = (
        (df_npi['New New/Carryover'] == 'New New') & 
        (df_npi['Fiscal Year'].astype(int).isin(valid_years)) 
    )
    df_npi_new = df_npi[mask_npi].copy()
    df_npi_new.rename(columns={'Fiscal Year':'Launch Year'}, inplace=True)
    df_npi_new=df_npi_new[['Launch Year','Region','SKU']].copy()
    df_npi_new['Region']=df_npi_new['Region'].str.upper().str.strip()
    df_npi_new['SKU']=df_npi_new['SKU'].str.upper().str.strip()

    #Ordenar y mantener solo el año de lanzamiento más antiguo para cada Region-SKU único
    df_npi_new.sort_values(by='Launch Year', ascending=False, inplace=True)
    df_npi_new.drop_duplicates(subset=['Region','SKU'],keep='first', inplace=True)


    #llaves para cruzar ventas con npi
    df_npi_new['fk_RegionSku']=(df_npi_new['Region']+'-'+
                                df_npi_new['SKU'])
    df_npi_new['fk_RegionSku']=df_npi_new['fk_RegionSku'].str.upper().str.strip()
    serie_new_map=df_npi_new.set_index('fk_RegionSku')['Launch Year']

    #============ TRATAMIENTO SALES =============================

    #===ASIGNACION DE REGION DE VENTA
    #Elimina duplicados en df_country
    df_country['CountryRegion']=df_country['Country']+'-'+df_country['Region']
    df_country['CountryRegion']=df_country['CountryRegion'].str.upper().str.strip()
    df_country = df_country.drop_duplicates(subset=['CountryRegion'], keep='first')
    serie_regiones_map = df_country.set_index('Country')['Region']
    df_processed['Region'] = df_processed['fk_Country'].map(serie_regiones_map)

    # fk para saber si es un nuevo producto en algunos de los 3 años de interes
    df_processed['fk_RegionSku']=(df_processed['Region']+'-'+
                                  df_processed['fk_SKU'])
    
    #========== MAPEO DE LAUNC YEAR by RegionSku ================
    df_processed['npi_year'] = df_processed['fk_RegionSku'].map(serie_new_map)
    df_processed['npi_year'] = pd.to_numeric(df_processed['npi_year'], errors='coerce').astype('Int64')
    df_processed['sale_year'] = pd.to_numeric(df_processed['fk_year_month'].astype(str).str[:4], errors='coerce').astype('Int64')

    diferencia_años = df_processed['sale_year'] - df_processed['npi_year']
    condicion_es_npi = diferencia_años.notna() & diferencia_años.between(0, 2)
    valor_npi_str = 'npi' + df_processed['npi_year'].fillna(0).astype(int).astype(str)

    df_processed['Launch Year'] = np.select(
        [condicion_es_npi],
        [valor_npi_str],
        default="core"
    )

    df_processed['VR %']=np.where(
        df_processed['Launch Year'].str.lower().str.contains('npi'),
        "VR %",
        ""
    )

    #columnas a eliminar
    #cols_to_drop = ['fk_RegionSku', 'Region']
    cols_to_drop = ['fk_RegionSku', 'Region', 'npi_year', 'sale_year'] 
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

def assign_fk_YearRegionSku(df_processed, df_country):
    """
    Asigna la clave compuesta 'Año-Región-SKU' optimizada para 2026.
    """
    # Preparación de la tabla maestra (df_country)
    # Limpieza proactiva: evitamos problemas de mayúsculas/espacios antes de crear el índice
    df_country = df_country.copy()
    df_country['Country'] = df_country['Country'].astype(str).str.upper().str.strip()
    df_country['Region'] = df_country['Region'].astype(str).str.upper().str.strip()
    
    # Eliminamos duplicados directamente sobre 'Country' para asegurar un mapeo 1:1
    mapping_dict = df_country.drop_duplicates('Country').set_index('Country')['Region'].to_dict()

    # Mapeo de Región
    # Normalizamos la columna de búsqueda en el DF principal
    df_processed['fk_Country'] = df_processed['fk_Country'].astype(str).str.upper().str.strip()
    df_processed['Region'] = df_processed['fk_Country'].map(mapping_dict)

    # Gestión de nulos (Crucial para que la llave no se rompa)
    # Si un país no existe en la maestra, asignamos 'UNKNOWN' para evitar llaves rotas
    df_processed['Region'] = df_processed['Region'].fillna('UNKNOWN')

    # Creación de la clave compuesta
    # Usamos .astype(str) para prevenir errores si fk_SKU o el año vienen como números
    df_processed['fk_YearRegionSku'] = (
        df_processed['fk_year_month'].astype(str).str[:4] + '-' +
        df_processed['Region'] + '-' +
        df_processed['fk_SKU'].astype(str)
    ).str.upper().str.strip()

    # 5. Limpieza
    df_processed.drop(columns=['Region'], inplace=True)

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
    #sales_historic_raw_dir=r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Sales\prueba'
    
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
    print(f'longitud archivos leidos: {len(df_consolidated)}')
    suma_init=df_consolidated["Total Sales"].astype(float).sum()

    # Leer el archivo de códigos de país.
    df_country = pd.read_excel(country_code_file,
                               sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
    df_md_product=pd.read_excel(md_product_processed_file,dtype=str, engine='openpyxl')
    df_gross_to_net=pd.read_excel(processed_gross_to_net,dtype=str, engine='openpyxl')
    df_npi=pd.read_excel(npi,sheet_name='Database',dtype=str, engine='openpyxl')
    df_filter_npi=pd.read_excel(filter_npi,dtype=str, engine='openpyxl')
    
    # Definir las columnas relevantes para el procesamiento.    
    lst_columns = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                   'fk_date_country_customer_clasification',
                   'Total Sales', 'Total Cost', 'Units Sold']
    df_consolidated = asign_country_code(df_consolidated, df_country)
    
    '''ESTA LINEA ES DE CONTROL PARA VER EL NUMERO DE REGISTROS SIN ASOCIACION DE PAIS Y CUANTO SUMA SU VENTA'''
    df_sin_pais = df_consolidated[df_consolidated['fk_Country'].isna()]
    num_filas = len(df_sin_pais)
    venta_sin_pais = df_sin_pais['Total Sales'].astype(float).sum()
    print(f"Número de filas de ventas sin country: {num_filas}")
    print(f"Suma de ventas sin país: {venta_sin_pais}")
    if suma_init != 0:
        porcentaje = (venta_sin_pais / suma_init) * 100
        print(f"¿Cuánto representa la venta sin país en el total?: {porcentaje:.2f}%")
    else:
        print("¿Cuánto representa la venta sin país en el total?: 0.00% (Total inicial es 0)")

    print("=" * 55)
    '''FIN DE LA LINEA DE CONTROL'''


    df_processed=process_columns(df_consolidated,lst_columns)
    print(f'longitud df dataset procesado: {len(df_processed)}')
    #=========================================================
    #--- ASIGNACIÓN COLUMNAS CALCULADAS
    #=========================================================
    df_processed=assign_nsv(df_processed, df_md_product, df_gross_to_net,df_country)
    print(f'longitud posterior a asignación NSV: {len(df_processed)}')

    df_processed=assign_selling_unit_price(df_processed)
    print(f'longitud posterior a asignación Selling Unit Price: {len(df_processed)}')
    
    df_processed=assign_NPI_New_Carryover(df_processed,df_npi,df_country)
    print(f'longitud posterior a asignación NPI: {len(df_processed)}')
    
    df_processed=LaunchYear_VR(df_processed,df_npi,df_country)
    
    print(f'longitud dataset procesado con LaunchYear_VR: {len(df_processed)}')   
    df_processed=assign_num_batteries(df_processed,df_md_product)
    df_processed=assign_NSV_NPI_w_Combo(df_processed,df_filter_npi)
    df_processed=assign_fk_YearRegionSku(df_processed, df_country)
    

    suma_end=df_processed["Total Sales"].astype(float).sum()
    if len(df_processed) == len(df_consolidated) and suma_init==suma_end:
        print(f'{"*"*55}')
        print("La longitud del DataFrame procesado coincide con la del DataFrame original.")
        print("El DataFrame procesado tiene la misma longitud que el DataFrame original.")
        print(f'El dataframe original y final tienen la misma suma de ventas: {suma_init}')
        print(f'{"*"*55}')
       
    else:
        print(f'{"*"*55}')
        print("La longitud del DataFrame procesado no coincide con la del DataFrame original.")
        print(f'longitud dataset crudo {len(df_consolidated)}')
        print(f'longitud dataset procesado: {len(df_processed)}')
        print(f'diferencia: {len(df_consolidated)-len(df_processed)}')
        print(f'Porcentaje de diferencia: {(len(df_consolidated)-len(df_processed))/len(df_consolidated)*100:.2f}%')

        print(f'la suma inicial es: {suma_init}')
        print(f'la suma final es: {suma_end}')
        print(f'la diferencia es: {suma_init-suma_end}')
        print(f'El porcentaje de diferencia es: {(suma_init-suma_end)/suma_init*100:.2f}%')
        print(f'{"*"*55}')
    #====================================
    # --- Formato de columnas ---
    #====================================
    lst_columns_srt = ['fk_Date','fk_year_month', 'fk_Country', 'fk_Sold_To_Customer_Code', 'fk_SKU',
                   'fk_date_country_customer_clasification',
                   'New New/Carryover',
                   'Launch Year','VR %',
                   'fk_YearRegionSku']
    lst_columns_float = ['Total Sales', 'Total Cost', 'Units Sold',
                         'NSV','Selling Unit Price',
                         'NPI Incremental Sales $',
                         'Num Batteries Sales',
                         'Net Sales NPI w/Combo' 
                         ]
    df_processed=format_columns(df_processed,lst_columns_srt,lst_columns_float)
    #  --- ESCRITURA DE ARCHIVOS PARQUET SEGMENTADOS --  
    group_parquet(df_processed, processed_parquet_dir,name='sales')
    #group_parquet(df_processed, sales_historic_raw_dir,name='sales')

# --- EJECUCION DEL SCRIPT ---
# Es una buena práctica envolver la ejecución principal en un bloque if __name__ == "__main__":
if __name__ == "__main__":
    main()
    print("Processing of historical Sales data completed successfully. ✅.")
