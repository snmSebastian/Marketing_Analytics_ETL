'''
#=======================================================
# 📦 MÓDULO ETL: ORQUESTACIÓN DE CARGA COMPLETA (FULL LOAD) DE VENTAS
#=======================================================

Propósito: 
    Este script funciona como el ORQUESTADOR (Pipeline Manager) del proceso ETL 
    (Extraer, Transformar, Cargar) para la consolidación históri
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

💡 NOTA DE SENIOR:
Mucho ojo con la función `assign_nsv`. Si la tabla de `Gross-to-Net` no tiene la 
combinación exacta de (Fecha + Región + Marca + SBU), el NSV se va a calcular mal 
o quedará en cero.
    
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


def assing_region(df_country,df_processed):
    """
    Realiza un mapeo optimizado de Región mediante un diccionario de búsqueda (Hash Map).
    Normaliza strings para asegurar consistencia y evitar duplicados en el cruce.
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
   
    df_processed=assing_region(df_country,df_processed)
   
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
    df_processed=assing_region(df_country,df_processed)
    # llave para cruzar ventas con npi
    df_processed['fk_NPI'] = (df_processed['fk_year_month'].astype(str) + '-'+
                             df_processed['Region'] + '-'+
                             df_processed['fk_SKU'])
    
    #clave para cruzar sales con npi
    df_processed['fk_NPI']=df_processed['fk_NPI'].str.upper().str.strip()

    # Convertimos el array de únicos a un DataFrame de una columna y exportamos
    #pd.Series(df_processed['fk_NPI'].unique(), name='fk_NPI').to_excel(r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Sebastian\fk_npi_unicos.xlsx', index=False)

    

    df_npi=df_npi.copy()
    df_npi['fk_YearMonthCountrySku']=df_npi['fk_YearMonthCountrySku'].str.upper().str.strip()
    df_npi.drop_duplicates(subset=['fk_YearMonthCountrySku'], inplace=True)
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
   
    df_processed=assing_region(df_country,df_processed)
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
    df_md_product = df_md_product.drop_duplicates(subset=['SKU'])
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
    df_filter_npi['fk_YearCountrySku']=df_filter_npi['fk_YearCountrySku'].str.upper().str.strip()
    df_filter_npi = df_filter_npi.drop_duplicates(subset=['fk_YearCountrySku'])
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
    
    df_processed=assing_region(df_country,df_processed)

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
