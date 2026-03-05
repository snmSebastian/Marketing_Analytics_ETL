"""
Módulo de funciones de transformación (T) para la construcción y enriquecimiento del Maestro de Productos (Master Products).
Contiene la lógica central para identificar nuevos SKUs de las fuentes de datos (Fill Rate, Sales, Demand),
clasificar estos SKUs (asignación de SKU Base, GPP, Corded/Cordless, Bare, etc.) y generar una tabla
de revisión para el analista.
"""

#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
# Liberia
import pandas as pd
import numpy as np
# Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico.
import glob
import os


import pandas as pd
from typing import List, Union

from Fill_Rate.Process_ETL.Process_Files import asign_country_code, read_files

def obtain_new_products(df_fill_rate, df_sales, df_demand, df_new_products, df_master_products):
    """
    Identifica y formatea SKUs nuevos comparando fuentes transaccionales contra el maestro.

    Consolida Fill Rate, Sales y Demand, estandariza nombres de columnas, filtra 
    registros existentes en el maestro y estructura el DataFrame con el esquema final.

    Args:
        df_fill_rate, df_sales, df_demand (pd.DataFrame): Fuentes de datos de entrada.
        df_new_products (pd.DataFrame): Acumulado anterior de productos nuevos.
        df_master_products (pd.DataFrame): Maestro actual para validación de existencia.

    Returns:
        pd.DataFrame: SKUs nuevos con la estructura completa del maestro de productos.
    """

    # 1. Selección y Estandarización
    # Nota: Se corrigieron los nombres de columnas para evitar el KeyError detectado
    f_rate = df_fill_rate[['Country Material', 'Country Material Name', 'LAG Brand', 
                        'GPP Division', 'GPP Category', 'GPP Portfolio']].rename(
        columns={'Country Material': 'SKU', 'Country Material Name': 'SKU Description', 
                 'LAG Brand': 'Brand', 'GPP Division': 'GPP Division Description', 
                 'GPP Category': 'GPP Category Description', 'GPP Portfolio': 'GPP Portfolio Description'})

    sales = df_sales[['fk_SKU', 'SKU Description', 'Brand', 
                      'GPP Division', 'GPP Category', 'GPP Portafolio']].rename(
        columns={'fk_SKU': 'SKU', 'GPP Division': 'GPP Division Description', 
                 'GPP Category': 'GPP Category Description', 'GPP Portafolio': 'GPP Portfolio Description'})

    demand = df_demand[['Global Material']].rename(columns={'Global Material': 'SKU'})
    cols_to_create = [
        'SKU Description', 'Brand', 'GPP Division Description', 
        'GPP Category Description', 'GPP Portfolio Description'
    ]

    # Crear columnas con valor nulo de una sola vez
    df_demand[cols_to_create] = pd.NA
    

    # 2. Consolidación y Filtrado
    df_new = pd.concat([sales, f_rate, demand], ignore_index=True)
    df_new = df_new.drop_duplicates(subset=['SKU'], keep='first')
    
    # Solo mantener SKUs que no están en el maestro
    df_new = df_new[~df_new['SKU'].isin(df_master_products['SKU'])]

    # 3. Estructuración Final (Esquema del Maestro)
    target_cols = ['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP', 'GPP SBU',
                   'GPP SBU Description', 'SBU Type', 'GPP Division Code',
                   'GPP Division Description', 'GPP Category Code',
                   'GPP Category Description', 'GPP Portfolio Code',
                   'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
                   'Voltaje', 'Bare', 'Sub-Brand', 'Project Name', 'Dewalt XR', 'origen_sku', 'check_sku']
    
    for col in [c for c in target_cols if c not in df_new.columns]:
        df_new[col] = np.nan

    df_new = df_new[target_cols].copy()
    df_new['origen_sku'] = 'new sku'
    
    return df_new

def assign_sku_base(sku_completo, skus_base_set):
    """
    Busca el SKU base probando prefijos decrecientes del SKU completo (desde la longitud total hasta una longitud mínima
    de 3 o (longitud total - 11)). Retorna el prefijo más largo que se encuentra en el conjunto de SKUs base conocidos
    
    Args:
        sku_completo (str): El SKU a clasificar.
        skus_base_set (set): Conjunto de SKUs base únicos del maestro histórico.
    """
    
    #  Iterar sobre las longitudes de los prefijos del SKU nuevo, DE REVERSA.
    # El rango va desde la longitud total (n_sku) hasta 1.
    len_sku = len(sku_completo)
    min_length = max(3,len_sku-11)  # Longitud mínima de un SKU base
    for length in range(len_sku, min_length-1, -1):
        
        # 2. Extraer el prefijo (subcadena desde el inicio hasta 'length')
        posible_sku_base = sku_completo[:length]
        
        # 3. Verificar si este prefijo es un SKU base conocido (búsqueda O(1) en el set)
        if posible_sku_base in skus_base_set:
            # Encontrado: Es el SKU base más largo y específico que coincide.
            return posible_sku_base
                
    # Si el bucle termina, no se encontró ningún prefijo que sea un SKU base
    return "-"

def assign_info_by_key(
    df_target: pd.DataFrame, 
    df_source: pd.DataFrame, 
    key_column: Union[str, List[str]], 
    columns_to_merge: List[str]
) -> pd.DataFrame:
    """ 
    Realiza una fusión (left join) entre el DataFrame destino y el DataFrame fuente usando una o varias columnas clave.
    Rellena las columnas especificadas en el destino solo si el valor actual es NaN. La función es defensiva: crea las
    columnas faltantes en el destino si es necesario.

    Args:
        df_target (pd.DataFrame): DataFrame destino (donde se llenarán los NaN).
        df_source (pd.DataFrame): DataFrame fuente (contiene la información de look-up).
        key_column (Union[str, List[str]]): Columna(s) clave para la fusión.
        columns_to_merge (List[str]): Lista de columnas a transferir de la fuente al destino.
    Returns: pd.DataFrame: El DataFrame destino con los valores NaN rellenados de la fuente.
    Raises: KeyError: Si la clave o las columnas a fusionar no existen en el DataFrame fuente.
    """
    
    # ---------------- Preparación de Claves y Copia ----------------
    # Asegúrate de que key_column sea una lista, dado que drop_duplicates y set_index requieren una lista de columnas.
    key_list = [key_column] if isinstance(key_column, str) else key_column
    suffix = '_SRC'
    df_result = df_target.copy() 

    # ---------------- VERIFICACIÓN Y CREACIÓN DE COLUMNAS FALTANTES ----------------
    # Si el dataframe target no tiene las columnas a fusionar, las crea.
    # Esto es útil para evitar errores si las columnas no existen en el DataFrame destino.

    # Crear columnas faltantes como 'object' para evitar conflicto de tipos
    for col in columns_to_merge:
        if col not in df_result.columns:
            df_result[col] = pd.Series(dtype='object')
        else:
            df_result[col] =  df_result[col].astype(object)

    # ---------------- 1. Preparar el DataFrame Fuente (Maestro) ----------------
    # Se añade una verificación de que las claves y columnas a fusionar existan en la fuente.
    # Esto es defensivo, pero importante para una función genérica.
    source_cols = columns_to_merge + key_list
    if not all(col in df_source.columns for col in source_cols):
        missing_source = [col for col in source_cols if col not in df_source.columns]
        raise KeyError(f"El DataFrame fuente (df_source) no contiene las siguientes columnas requeridas: {missing_source}")

    # Continuamos con la lógica de join, ahora sabiendo que las columnas existen en df_result
    df_gpp_map = (
        df_source[source_cols]
        .drop_duplicates(subset=key_list, keep='first')
        
    )
    
    # ---------------. Join y Look-up ----------------
    # 2a. Convertir el mapa de regreso a columnas para el merge
    df_gpp_map_merged = df_gpp_map.add_suffix(suffix)
    
    # Asegúrate de que las columnas clave del mapa tengan el sufijo:
    key_list_suffixed = [k + suffix for k in key_list]
    for k_suffix, k_orig in zip(key_list_suffixed, key_list):
        df_gpp_map_merged = df_gpp_map_merged.rename(columns={k_suffix: k_orig})

    # . Realizar el merge (Fusionar el destino con la fuente por la clave)
    df_merged = df_result.merge(
        df_gpp_map_merged, 
        on=key_list, 
        how='left'
    )
    
    # ---------------- 3. Rellenar los NaN y Limpiar ----------------
    for col in columns_to_merge:
        col_map = col + suffix
        
        #  Usar .loc para la asignación segura basada en NaN (Ahora en df_merged)
        is_nan_mask = df_merged[col].isna()
        df_merged.loc[is_nan_mask, col] = df_merged[col_map]
        
        #  Eliminar la columna temporal
        del df_merged[col_map]
        
    # ---------------- 4. Retornar el resultado ----------------
    return df_merged # Ya tiene el índice original, no necesita reset_index()

def assign_gpp_by_portafolio(portafolio, lst_portafolio,df_gpp):
    """
    Asigna el código GPP a un SKU que no tiene SKU Base, buscando una coincidencia por prefijo en la descripción del Portafolio
    y mapeando el primer resultado válido. Retorna el GPP encontrado o "-".
    
    Args:
        portafolio (str): El portafolio a clasificar.
        lst_portafolio (list): Lista de portafolios únicos del maestro histórico.
    Return:
        El GPP que corresponde al portafolio que viene de SAP
    """
    if pd.isna(portafolio):
        return "-"

    if isinstance(portafolio, float) and portafolio.is_integer():
        port_str = str(int(portafolio))
    else:
        port_str = str(portafolio)

    portafolio_a_buscar = port_str.strip().upper().replace(' ', '') 

    for port in lst_portafolio:
        port_hist = str(port).strip().upper().replace(' ', '')
        if port_hist.startswith(portafolio_a_buscar) and not( portafolio_a_buscar.startswith('INVALID')):
            gpp = df_gpp[df_gpp['fk_GPP_Portfolio'] == port]['GPP'].values
            if len(gpp) > 0:
                return gpp[0]
            else:
                return "-"

def verify_psd(sku, lst_psd):
    """
    Verifica si un SKU se encuentra en la lista compartida de PSD. Si es así, le asigna el código GPP específico de PSD;
    de lo contrario, retorna "-".
    
    Args:
        sku (str): El SKU a verificar.
        lst_psd (list): Lista de SKUs únicos de PSD.
    Return:
        GPP de PSD si existe en la base, si no "-"
    """
    sku_a_buscar = sku.strip().upper().replace(' ', '')
    if sku_a_buscar in lst_psd:
        return "PSD-70-70X-70999"
    else:
        return "-"
    
def verify_gpp(gpp, lst_gpp):
    """
    Comprueba la validez de un código GPP asegurando que exista en la lista de GPPs conocidos. Retorna el GPP si es válido,
    o "-".
    
    Args:
        gpp (str): El código GPP a verificar.
        lst_gpp (list): Lista de códigos GPP únicos.
    """
    gpp_a_buscar = str(gpp).strip().upper().replace(' ', '')
    if gpp_a_buscar in lst_gpp:
        return gpp_a_buscar
    else:
        return "-"

def corded_or_cordless_or_gas(sku,description,category_description,portfolio_description,corded_or_cordless):
    """ 
    Determina el tipo de energía (Corded, Cordless, o Gas) del SKU. Utiliza una jerarquía de reglas basada en:
        1) Prefijos del SKU.
        2) Palabras clave en la descripción.
        3) Palabras clave en la Categoría/Portafolio.
    """
    sku = sku.strip().upper().replace(' ', '')
    description = description.strip().upper().replace(' ', '')
    category_description = category_description.strip().upper().replace(' ', '')
    portfolio_description = portfolio_description.strip().upper().replace(' ', '')
    sku = str(sku).strip().upper().replace(' ', '')
    description = str(description).strip().upper().replace(' ', '')
    category_description = str(category_description).strip().upper().replace(' ', '')
    portfolio_description = str(portfolio_description).strip().upper().replace(' ', '')
    category_portafolio = category_description + portfolio_description

    lst_cdl_description = [
        'CORDLESS', 'CDL', 'INALAMBRIC', 'BATTERY', 'BATT', 'BRUSHLESS', 'XR', 'MAX', 'LI-ION',
        '2.4V', '3.6V', '3.8V', '4V', '4.8V', '6V', '7.2V', '8V', '9.6V', '10.8V', 
        '12V', '14.4V', '16V', '18V', '20V', '24V', '36V', '40V', '54V', '60V',
        'CHARGER', 'CARGADOR'
    ]
    lst_cdl_sku=['BDC','CMC','DWC','PCC','STC','DC']
    lst_cdl_cat_por_description=['CDL','CORDLESS','20V','12V']

    lst_crd_description=[
        'CRD','CORDED','ALAMBRICO', 'ELECTRIC', 'WATT','AMPER', 'AMP',
        'STATIONARY', 'BENCHTOP', 'COMPRESSOR',
        '0W','110V', '120V', '220V', '230V','127V']
    lst_crd_sku=['DWE','FME','BEW','KS']
    lst_crd_cat_por_description=['CORDED','CRD']

    lst_gas_description = [
        'GAS', 'GASOLINE', 'GASOLINA', '0CC','1CC','2CC','3CC','4CC','5CC',
        '6CC','7CC','8CC','9CC','10CC','0PSI']
    # Prioridad 1: Verificar si el SKU inicia con los prefijos clave de 'Cordless' Corded
    if any(sku.startswith(elemento) for elemento in lst_cdl_sku):
        return 'CORDLESS'
    if any(sku.startswith(elemento) for elemento in lst_crd_sku):
        return 'CORDED'
    # Prioridad 2: Verificar si la descripción contiene palabras clave de 'Cordless' o 'Corded'
    if any(elemento in description for elemento in lst_crd_description):
        return 'CORDED'
    if any(elemento in description for elemento in lst_cdl_description) and not '220V' in description:
        return 'CORDLESS'
    
    # Prioridad 3: Verificar si la categoría o portafolio contiene palabras clave de 'Cordless' o 'Corded'
    if any(elemento in category_portafolio for elemento in lst_cdl_cat_por_description):
        return 'CORDLESS'
    if any(elemento in category_portafolio for elemento in lst_crd_cat_por_description):
        return 'CORDED'
    # Prioridad 4: Verificar si la descripcion indica que es un producto a gas     
    if any(elemento in description for elemento in lst_gas_description):
        return 'GAS'
    # Si no se encuentra ninguna coincidencia, retorna el valor original
    return corded_or_cordless

def assing_qty_batteries(sku, description, batteries_qty):
    """
    Asigna la cantidad de baterías. Utiliza un mapeo de sufijos específicos del SKU (ej., X1, L2) para inferir la
    cantidad de baterías, o asigna 0 si el SKU termina en B (Bare Tool).

    Args:
        sku (str): El SKU a clasificar.
        description (str): La descripción del SKU.
        batteries_qty (list): Lista de cantidades de baterías.

    """
    sku = str(sku).strip().upper().replace(' ', '')
    if '/' in sku:
        sku= sku.split('/')[0]  # Toma la parte antes de la barra diagonal
    
    description = description.strip().upper().replace(' ', '')
    description = str(description).strip().upper().replace(' ', '')
    
    # Lista de palabras clave para identificar la cantidad de baterías
    lst_battery_keywords = ['S1','S2','C1','C2','E1','E2',
                            'D1','D2','F1','F2','L1','L2',
                            'G1','G2','M1','M2','Q1','Q2',
                            'P1','P2','R1','R2','J1','J2',
                            'R1','R2','T1','T2','W1','W2',
                            'X1','X2','U1','U2','Y1','Y2','Z1','Z2'
    ]
    
    # Verifica si la descripción contiene alguna palabra clave relacionada con baterías
    if any(elemento in sku[-2:] for elemento in lst_battery_keywords):
        return str(batteries_qty[-1])
    if sku.endswith('B'):
        # Si el SKU termina con 'B', significa que no tiene batería
        return "0"
    return batteries_qty  # Retorna el valor original si no se encuentra información relevante

def assing_voltaje(description, voltaje):
    """
    Asigna el valor del Voltaje al SKU extrayéndolo de las palabras clave de la descripción (ej., '20V', '54V').
    
    Args:
        description (str): La descripción del SKU.
        voltaje (list): Lista de voltajes.
        
    """
    description = description.strip().upper().replace(' ', '')
    description = str(description).strip().upper().replace(' ', '')
    lst_voltajes = [
        '2.4V', '3.6V', '3.8V', '4V', '4.8V', '6V', '7.2V', '8V', '9.6V', '10.8V', 
        '12V', '14.4V', '16V', '18V', '20V', '24V', '36V', '40V', '54V', '60V','120V'
    ]
    for vol in lst_voltajes:
        if vol in description and not('220V' in description):
            return vol
            
    # Si no se encuentra voltaje en la descripción, se devuelve el valor original
    if voltaje is not None and str(voltaje).strip() not in ['', '-']:
        return voltaje
        
    return "-" # Devolver None o '-' si no se encuentra nada
    # Si no se encuentra información relevante, retorna el valor original
    
def assign_bare(sku,quantity_batteries,corded_or_cordless):
    """
    Asigna el valor de Bare (la herramienta viene sin baterías/cargador) según una lógica que combina el tipo de
    herramienta (CORDLESS) con la cantidad de baterías. Puede resultar en 'Bare', 'Non Bare', o 'Bare + Batteries'.
    
        Bare: el sku indica que no trae baterias
        non bare: el sku indica que trae baterias
        bare+batteries: la descripcion  indica que trae baterias y es el sku es bare
    """
    sku = str(sku).strip().upper().replace(' ', '')
    sku = sku.split('/')[0]  # Toma la parte antes de la barra diagonal
    if corded_or_cordless == 'CORDLESS':
        if '-' in quantity_batteries:
            quantity_batteries=0
        q_str = str(quantity_batteries)
        if '-' in q_str:
            quantity_batteries = 0
        else:
            quantity_batteries=int(quantity_batteries)

            try:
                quantity_batteries = int(float(q_str))
            except ValueError:
                quantity_batteries = 0
        if quantity_batteries == 0 and (sku.endswith('B') or not sku.endswith('B')) :
            return 'Bare'
        elif quantity_batteries > 0 and not sku.endswith('B'):
            return 'Non Bare'
        elif quantity_batteries > 0 and  sku.endswith('B'):
            return 'Bare + Batteries'
        else:
            return '-'
        
def assign_sub_brand(sku,description,brand):
    """
    Asigna la Sub-Marca del producto basada en reglas específicas del negocio (ej., FATMAX, IAR EXPERT) verificando la
    descripción y el código SKU.
    """

    description = description.strip().upper().replace(' ', '')
    brand = brand.strip().upper().replace(' ', '')
    description = str(description).strip().upper().replace(' ', '')
    brand = str(brand).strip().upper().replace(' ', '')
    if 'FATMA' in description:
        return 'FATMAX'
    elif sku.startswith('E') and brand == 'FACOM':
        return 'IAR EXPERT'
    elif sku.startswith('STA82'):
        return 'MASS'
    else:
        return "-"  # Retorna la marca original si no se encuentra una sub-marca específica

def review_sku_base_with_diferent_category(df_master_products,lst_colums_gpp):


    """
    Identifica y extrae los registros del Maestro de Productos histórico donde el mismo SKU Base está asociado a múltiples combinaciones
    de SBU y Categoría (SBU_Category).
    
    Esto genera una lista de SKUs que requieren revisión manual para asegurar la coherencia en la jerarquía del producto.

    Args:
        df_master_products (pd.DataFrame): El Maestro de Productos histórico.
        lst_colums_gpp (list): Lista de columnas de clasificación GPP deseadas.
    """
    # Creo  la columna de combinación única SBU+Category
    df_master_products['SBU_Category'] = df_master_products['GPP SBU'].astype(str) + '-' + df_master_products['GPP Category Description'].astype(str)
    # Contar la cantidad de combinaciones únicas (SBU + Category) por cada 'SKU Base'
    df_sku_base_counts = df_master_products.groupby('SKU Base')['SBU_Category'].nunique().reset_index(name='count')
    #Filtro aquellos sbu que tienen mas de un sbu-category
    df_sku_base_review=df_sku_base_counts[df_sku_base_counts['count']>1]['SKU Base']
    # Tomo toda la informacion existente para aquellos sbu que debemos revisar
    df_resultado=df_master_products[df_master_products['SKU Base'].isin(df_sku_base_review)].copy()
    # Asigno de donde viene el sku y su gpp
    df_resultado.loc[:,'origen_sku'] = 'SKU Base con diferentes sbu-category'
    df_resultado.loc[:,'¿como se asigno gpp?'] = 'Es el gpp que esta actualmente en el master product'
    df_resultado.loc[:,'check_sku']="-"
    
    # tomo las columnas que debemos revisar
    df_resultado=df_resultado[lst_colums_gpp]  
    return df_resultado 

def assign_proyects_xr(df_master, df_proyects, df_dewaltXR):
    """
    Clasifica productos como 'Dewalt XR' y asigna proyectos por SKU o SKU Base.

    Prioriza la asignación de proyecto por SKU; si es nulo, intenta por SKU Base. 
    Limpia columnas temporales de forma segura.

    Args:
        df_master, df_proyects, df_dewaltXR (pd.DataFrame): DataFrames de entrada.

    Returns:
        pd.DataFrame: DataFrame con 'Dewalt XR' y 'Project Name' consolidados.
    """
    # 1. Clasificación XR
    xr_sku = set(df_dewaltXR['SKU'])
    xr_sku_base = set(df_dewaltXR['SKU Base'].dropna())
    
    condicion_xr = df_master['SKU'].isin(xr_sku) | df_master['SKU Base'].isin(xr_sku_base)
    df_master['Dewalt XR'] = np.where(condicion_xr, 'XR', '-')

    # 2. Cruce por SKU
    df_master = pd.merge(df_master, df_proyects[['SKU', 'Project Name']].drop_duplicates('SKU'), 
                         on='SKU', how='left')
    
    # 3. Cruce por SKU Base
    df_master = pd.merge(df_master, df_proyects[['SKU Base', 'Project Name']].drop_duplicates('SKU Base'), 
                         on='SKU Base', how='left', suffixes=('', '_base'))

    # 4. Consolidación y Limpieza Segura
    if 'Project Name_base' in df_master.columns:
        df_master['Project Name'] = df_master['Project Name'].fillna(df_master['Project Name_base'])
        df_master.drop(columns=['Project Name_base'], inplace=True)
    
    return df_master

  


def main():
    """	
    Función principal que orquesta el pipeline de identificación y clasificación de nuevos productos.	
    El flujo incluye:
        1) Consolidación de nuevos SKUs.
        2) Asignación de SKU Base (si aplica).	
        3) Look-up de GPP (por SKU Base o por Portafolio).
        4) Asignación de atributos (Corded/Cordless, Voltaje, Bare).	
        5) Generación de un archivo de revisión Excel (WORKFILE_NEW_PRODUCTS_REVIEW_FILE) que incluye nuevos SKUs y SKUs Base con clasificaciones inconsistentes.	
    Returns: None: La función orquesta el proceso y no devuelve un valor,
                   guardando el resultado en un archivo Excel        
    """
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: MD PRODUCTS UPDATE ETL ---")
    print("=" * 55)
    #-------------------------------
    #---- RUTAS DE LOS ARCHIVOS
    #-------------------------------
    from config_paths import MasterProductsPaths
    #path_fill_rate_update=MasterProductsPaths.INPUT_RAW_UPDATE_FILL_RATE_DIR
    #path_sales_update=MasterProductsPaths.INPUT_RAW_UPDATE_SALES_DIR
    #path_demand_update=MasterProductsPaths.INPUT_RAW_UPDATE_DEMAND_DIR
    path_ConsultaSku=MasterProductsPaths.INPUT_RAW_ConsultaSKU_FILE

    path_producst_hts=MasterProductsPaths.WORKFILE_HTS_FILE
    path_producst_pwt=MasterProductsPaths.WORKFILE_PWT_FILE
    
    path_New_Products=MasterProductsPaths.WORKFILE_NEW_PRODUCTS_REVIEW_FILE    
    path_gpp=MasterProductsPaths.INPUT_PROCESSED_GPP_BRAND_FILE
    path_psd=MasterProductsPaths.INPUT_RAW_SHARED_PSD_FILE
    path_sku_snowflake=MasterProductsPaths.INPUT_RAW_SkuName_FILE
    path_proyects=MasterProductsPaths.INPUT_PROCESSED_PROYECTS_FILE
   
    #path_master_products=MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE_PRUEBA
    path_master_products=MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE
    
    #-----------------------------
    #----  Cargo dataframes
    #-----------------------------
    #df_fill_rate=read_files(path_fill_rate_update)
    #df_sales=read_files(path_sales_update)
    #df_demand=read_files(path_demand_update)
    df_consultaSku=pd.read_excel(path_ConsultaSku, dtype=str, engine='openpyxl')
    

    df_master_products=pd.read_excel(path_master_products, dtype=str, engine='openpyxl')
    df_new_products=pd.read_excel(path_New_Products, dtype=str, engine='openpyxl')

    df_gpp=pd.read_excel(path_gpp, dtype=str, engine='openpyxl',sheet_name='GPP')
    df_brand=pd.read_excel(path_gpp, dtype=str, engine='openpyxl',sheet_name='Brand')
    df_psd=pd.read_excel(path_psd, dtype=str, engine='openpyxl')
    df_snowflake=pd.read_parquet(path_sku_snowflake, engine='pyarrow')
    df_proyects=pd.read_excel(path_proyects, dtype=str, engine='openpyxl',sheet_name='Proyects')
    df_dewaltXR=pd.read_excel(path_proyects, dtype=str, engine='openpyxl',sheet_name='DW_XR')
    
    #---------------------------------------------------
    #--- Genero el archivo con los nuevos productos
    #----------------------------------------------------
    df_new_products= df_consultaSku.copy()
    
    lst_colums_gpp=['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP', 'GPP SBU',
       'GPP SBU Description', 'SBU Type', 'GPP Division Code',
       'GPP Division Description', 'GPP Category Code',
       'GPP Category Description', 'GPP Portfolio Code',
       'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
       'Voltaje', 'Bare', 'Sub-Brand','Project Name','Dewalt XR','origen_sku','check_sku']
    
    columnas_a_crear=[col for col in lst_colums_gpp if col not in df_new_products.columns]
    for col in columnas_a_crear:
        df_new_products[col] = pd.Series(np.nan, index=df_new_products.index, dtype='object')
    df_new_products = df_new_products[lst_colums_gpp]
    df_new_products['origen_sku'] = 'new sku'


    #----------------------------------------------------
    #---- Procesamiento de los nuevos productos
    #----------------------------------------------------

    # Asigno el sku base a los nuevos productos    
    sku_base_set = set(df_master_products['SKU Base'].dropna().unique())
    df_new_products['SKU Base'] = df_new_products['SKU'].apply(lambda x: assign_sku_base(x, sku_base_set))
    
    # Genero dos dataframes, uno con sku base y otro sin sku base
    df_new_products_con_base = df_new_products[df_new_products['SKU Base'] != '-'].copy()
    df_new_products_sin_base = df_new_products[df_new_products['SKU Base'] == '-'].copy() 

    #...................................................................
    #----- Procesamiento para los nuevos productos con sku base
    #...................................................................

    #Asigno el gpp para los nuevos productos con sku base
    key_column = ['SKU Base']
    columns_merge = [
        'Brand', 'GPP', 'GPP SBU', 'GPP SBU Description', 'SBU Type', 
        'GPP Division Code', 'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code', 
        'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
        'Voltaje', 'Bare'
    ]
    df_new_products_con_base.loc[:, columns_merge] = np.nan
    df_new_products_con_base = assign_info_by_key(
        df_new_products_con_base, 
        df_master_products, 
        key_column, 
        columns_merge
    )

    #...................................................................
    #----- Procesamiento para los nuevos productos SIN sku base
    #...................................................................

    # Asigno el gpp por medio del portafolio para los nuevos productos sin sku base
    df_gpp['fk_GPP_Portfolio'] = df_gpp['GPP Portfolio Description'].str.strip().str.upper().str.replace(' ', '') 
    df_new_products_sin_base['GPP'] = df_new_products_sin_base['GPP Portfolio Description'].apply(
        lambda x: assign_gpp_by_portafolio(x, df_gpp['fk_GPP_Portfolio'].unique(), df_gpp))
    
    # Asigno "-" a los gpp que no existen
    lst_gpp=(
    pd.Series(df_gpp['GPP'].dropna().unique())  # 1. Convertir el arreglo de NumPy de vuelta a Serie
    .str.strip()
    .str.upper()
    .str.replace(' ', '')
    .tolist()
    )
    df_new_products_sin_base['GPP'] = df_new_products_sin_base['GPP'].apply(
        lambda x: verify_gpp(x, lst_gpp))
    
    #============================================================================================================
    # Asigno la info de descripcion y gpp existente en snowflake, para aquellos sku que aun no poseen gpp

    #Asigno el gpp para los nuevos productos con sku base
    rename_columns={
        'BRAND':'Brand',
        'SBU Code':'GPP SBU',
        'SBU Description':'GPP SBU Description',
        'Division Code':'GPP Division Code',
        'Division Description':'GPP Division Description',
        'Category Code':'GPP Category Code',
        'Category  Description':'GPP Category Description',
        'Portafolio Code':'GPP Portfolio Code',
        'Portafolio Description':'GPP Portfolio Description'
    }
    df_snowflake=df_snowflake.rename(columns=rename_columns)
    key_column = ['SKU']
    columns_merge_snowflake = [
         'SKU Description','Brand', 'GPP SBU', 'GPP SBU Description',  
        'GPP Division Code', 'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code', 
        'GPP Portfolio Description'
    ]
    df_new_products_sin_info=df_new_products_sin_base[df_new_products_sin_base['GPP'] == '-'].copy()
    
    df_new_products_sin_info.loc[:, columns_merge_snowflake] = np.nan
    df_new_products_sin_info = assign_info_by_key(
        df_new_products_sin_info, 
        df_snowflake, 
        key_column, 
        columns_merge_snowflake
    )
    df_new_products_sin_info['GPP'] =(df_new_products_sin_info['GPP SBU']+'-'+
                                        df_new_products_sin_info['GPP Division Code']+'-'+
                                        df_new_products_sin_info['GPP Category Code']+'-'+
                                        df_new_products_sin_info['GPP Portfolio Code']
                                    )  

    df_new_products_sin_info['GPP'] = df_new_products_sin_info['GPP'].apply(
        lambda x: verify_gpp(x, lst_gpp))
    



    #-------------- Verifico si los sku que aun no tienen gpp, estan en la base compartida por PSD    
    df_posibble_psd = df_new_products_sin_info[df_new_products_sin_info['GPP'] == '-'].copy()
    # elimino los sku que no tienen gpp para no genrar duplicados al concatenar
    df_new_products_sin_info = df_new_products_sin_info[df_new_products_sin_info['GPP'] != '-']
    
    # Asigno la notacion existe en la tabla principal de clasificaciones, segun el gpp que existe en snowflake
    columns_merge_sku_sin_info = [ 'GPP SBU', 'GPP SBU Description', 'SBU Type', 
        'GPP Division Code', 'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code', 
        'GPP Portfolio Description']
    df_new_products_sin_info.loc[:,columns_merge_sku_sin_info]= np.nan  # Inicializo las columnas a NaN
    key_column = ['GPP']
    df_new_products_sin_base = assign_info_by_key(
        df_new_products_sin_base, 
        df_gpp, 
        key_column, 
        columns_merge_sku_sin_info
    )


    lst_psd = (
    pd.Series(df_psd['SKU'].dropna().unique())  # 1. Convertir el arreglo de NumPy de vuelta a Serie
    .str.strip()
    .str.upper()
    .str.replace(' ', '')
    .tolist()
    )
    
    df_posibble_psd['GPP']=df_posibble_psd['SKU'].apply(
        lambda x: verify_psd(x, lst_psd))
    # Asigno toda la informacion de clasificacion tomadno en cuenta el gpp de PSD
    key_column = ['GPP']
    columns_merge = [
        'GPP SBU', 'GPP SBU Description', 'SBU Type', 
        'GPP Division Code', 'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code', 
        'GPP Portfolio Description']
    df_posibble_psd.loc[:,columns_merge] = np.nan
    df_posibble_psd = assign_info_by_key(
        df_posibble_psd, 
        df_gpp, 
        key_column, 
        columns_merge
    )

    # Asigno ¿como se asigno gpp? a los dataframes
    df_new_products_con_base['¿como se asigno gpp?'] = 'sku base'
    df_new_products_sin_base['¿como se asigno gpp?'] = 'por portafolio dado por SAP'
    df_new_products_sin_info['¿como se asigno gpp?se']='snowflake'
    df_posibble_psd['¿como se asigno gpp?'] = 'sku esta en la base compartida por PSD'
    # Ordeno las columnas de los dataframes
    lst_colums_gpp=['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP', 'GPP SBU',
    'GPP SBU Description', 'SBU Type', 'GPP Division Code',
    'GPP Division Description', 'GPP Category Code',
    'GPP Category Description', 'GPP Portfolio Code',
    'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
    'Voltaje', 'Bare', 'Sub-Brand','origen_sku','¿como se asigno gpp?','check_sku']
    df_new_products_con_base = df_new_products_con_base[lst_colums_gpp]
    df_new_products_sin_base = df_new_products_sin_base[lst_colums_gpp]
    df_posibble_psd = df_posibble_psd[lst_colums_gpp]
    # Genero el nuevo dataframe con los nuevos productos a partir de los dataframes con y sin sku base
    df_new_products_gpp=pd.concat([df_new_products_con_base, df_new_products_sin_base,df_new_products_sin_info,df_posibble_psd], ignore_index=True)    
    
    # Tratamiento de los espacios en blanco y NaN
    df_new_products_gpp = df_new_products_gpp.fillna(value='-')
    df_new_products_gpp = df_new_products_gpp.astype("string")

    # ---------------------------------------------------------------------------------------
    # Tratamiento de columnas corded / cordless, qyt batteries, voltaje,bare,sun brand
    # ---------------------------------------------------------------------------------------
    #Asigno el corded o cordless a los nuevos productos
    df_new_products_gpp['Corded / Cordless'] = df_new_products_gpp.apply(
        lambda row: corded_or_cordless_or_gas(row['SKU'], row['SKU Description'], row['GPP Category Description'], row['GPP Portfolio Description'],
                                        row['Corded / Cordless']), axis=1)
    
    # Asigno la cantidad de baterías a los nuevos productos
    df_new_products_gpp['Batteries Qty'] = df_new_products_gpp.apply(
        lambda row: assing_qty_batteries(row['SKU'], row['SKU Description'], row['Batteries Qty']), axis=1)
    
    # Asigno el voltaje a los nuevos productos
    df_new_products_gpp['Voltaje'] = df_new_products_gpp.apply(
        lambda row: assing_voltaje(row['SKU Description'], row['Voltaje']), axis=1)
    
    # Asigno el valor de Bare a los nuevos productos
    df_new_products_gpp['Bare'] = df_new_products_gpp.apply(
        lambda row: assign_bare(row['SKU'], row['Batteries Qty'], row['Corded / Cordless']), axis=1)
    #Asigno la sub-marca a los nuevos productos
    df_new_products_gpp['Sub-Brand'] = df_new_products_gpp.apply(
        lambda row: assign_sub_brand(row['SKU'], row['SKU Description'], row['Brand']), axis=1)
    # Asigno Proyects y Dewalt XR
    df_new_products_gpp=assign_proyects_xr(df_new_products_gpp, df_proyects, df_dewaltXR)
    

    # Extraigo los sku base que tienen diferente sbu-category para su revision
    df_sku_base_review=review_sku_base_with_diferent_category(df_master_products,lst_colums_gpp)
    
    # Creo el dataframe que contiene tanto los nuevos sku como los sku a revisar
    df_review_products=pd.concat([df_new_products_gpp,df_sku_base_review], ignore_index=True)
    

    # Exporto a excel el dataframe de nuevos productos
    path_result_ConsultSku=r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Products\other\ResultConsultSku.xlsx'
    df_review_products.to_excel(path_result_ConsultSku, index=False)

if __name__ == "__main__":
    main()
    print("Proceso de actualización de productos completado exitosamente.")