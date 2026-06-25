"""
EL CEREBRO DE PRODUCTOS: Motor de Clasificación, ADN y Enriquecimiento
----------------------------------------------------------------------
Este script actúa como el "Traductor Universal" de la operación regional. Su misión es crítica: 
toma los SKUs que vienen "desnudos" (sin contexto) de las transacciones y les asigna una identidad 
completa. Básicamente, es el filtro que evita que el reporte de Power BI se llene de códigos 
huérfanos, garantizando que cada producto tenga marca, familia y categoría clara.

¿POR QUÉ ES VITAL ESTE PROCESO?
 1. CACERÍA DE SKUS: Detecta en tiempo real qué códigos nuevos han aparecido en Fill Rate, Sales 
    o Demand que aún no existen en nuestro Maestro de Productos.
 2. ÁRBOL GENEALÓGICO: Ejecuta el algoritmo de "SKU Base" para agrupar variantes (kits, combos) 
    bajo un mismo padre, manteniendo la coherencia del catálogo.
 3. INYECCIÓN DE GPP: Cruza la data con Snowflake para inyectar la jerarquía oficial de 
    negocio (SBU, División, Categoría y Portafolio).
 4. MINERÍA DE ATRIBUTOS: Realiza un "escaneo inteligente" de descripciones para extraer datos 
    técnicos críticos: Voltaje, Corded/Cordless, cantidad de Baterías y si es equipo "Bare".
 5. FILTRO DE CALIDAD: Genera la tabla de revisión para que el analista valide solo lo nuevo, 
    minimizando el error humano.

💡 CONSEJO DE SENIOR:
Este módulo es una LIBRERÍA CORE. Cualquier cambio en las reglas de clasificación (como el 
diccionario de 'Cordless' o las funciones de 'Regex') impactará retroactivamente a miles 
de productos en todo el ecosistema. Si vas a meterle mano a la lógica, haz un backup de 
la última corrida; aquí un pequeño ajuste reclasifica toda la historia de un plumazo.
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
                   'Voltaje', 'Bare',  'origen_sku', 'check_sku']
    
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

def assing_qty_batteries(sku):
    """
    Asigna la cantidad de baterías. Utiliza un mapeo de sufijos específicos del SKU (ej., X1, L2) para inferir la
    cantidad de baterías, o asigna 0 si el SKU termina en B (Bare Tool).

    Args:
        sku (str): El SKU a clasificar.
        batteries_qty (list): Lista de cantidades de baterías.

    """
    # 1. Limpieza inicial: Convertir a string, quitar espacios y pasar a Mayúsculas
    sku_clean = str(sku).strip().upper().replace(' ', '')
    
    # 2. Manejo de la barra diagonal: Si existe, toma lo de la izquierda
    if '/' in sku_clean:
        sku_clean = sku_clean.split('/')[0]
    
    # 3. PRIORIDAD: Si termina en 'B', es Bare Tool (0 baterías)
    # Ponemos esto primero para que no lo confunda con sufijos de batería
    if sku_clean.endswith('B'):
        return "0"
    
    # 4. Lista de sufijos (Keywords)
    lst_battery_keywords = [
        'S1','S2','C1','C2','E1','E2','D1','D2','F1','F2','L1','L2',
        'G1','G2','M1','M2','Q1','Q2','P1','P2','R1','R2','J1','J2',
        'T1','T2','W1','W2','X1','X2','U1','U2','Y1','Y2','Z1','Z2'
    ]
    
    # 5. Verificación de sufijo de batería (últimos 2 caracteres)
    suffix = sku_clean[-2:]
    if suffix in lst_battery_keywords:
        # Retorna el último número del sufijo (el '1' o '2')
        return suffix[-1] 
    
    # 6. Retorno por defecto: Si no es Bare Tool ni tiene sufijo conocido
    # Si batteries_qty es una lista, podrías querer el primer elemento o un default
    return "0"
def assing_voltaje(description):
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




def assign_proyects(df_master, df_proyects, df_dewaltXR, df_dw_ind):
    """
    Realiza una clasificación jerárquica de productos y asignación de proyectos.
    * Dewalt XR
    * Project Name
    * Industrial Type
    * B+D Power Connect 20V


    La función consolida información de múltiples fuentes (Dewalt XR, Proyectos e Industrial) 
    utilizando una lógica de prioridad: primero intenta asignar por 'SKU' y, si no hay coincidencia, 
    por 'SKU Base'. Finalmente, aplica reglas de negocio específicas para la marca Black+Decker.

    Optimizada mediante mapeo de índices (Series mapping) para maximizar el rendimiento 
    y reducir el consumo de memoria en comparación con cruces (merges) tradicionales.
    """
    # 1. Preparar Diccionarios de Mapeo (Mucho más rápidos que merge)
    # Creamos mapeos para SKU y SKU Base de cada categoría
    
    # Proyectos
    proj_map_sku = df_proyects.dropna(subset=['SKU']).drop_duplicates('SKU').set_index('SKU')['Project Name']
    proj_map_base = df_proyects.dropna(subset=['SKU Base']).drop_duplicates('SKU Base').set_index('SKU Base')['Project Name']
    # Dewalt XR (incluye el del df_proyects según tu lógica original)
    xr_map_sku = df_dewaltXR.dropna(subset=['SKU']).drop_duplicates('SKU').set_index('SKU')['Dewalt XR']
    xr_map_base = df_dewaltXR.dropna(subset=['SKU Base']).drop_duplicates('SKU Base').set_index('SKU Base')['Dewalt XR']
    # Industrial Type
    ind_map_sku = df_dw_ind.dropna(subset=['SKU']).drop_duplicates('SKU').set_index('SKU')['Industrial Type']
    ind_map_base = df_dw_ind.dropna(subset=['SKU Base']).drop_duplicates('SKU Base').set_index('SKU Base')['Industrial Type']
    # 2. Aplicar asignación con .map() y .fillna()
    # Este método evita crear columnas duplicadas (_base) y tener que borrarlas luego
    
    # Asignar Project Name
    df_master['Project Name'] = df_master['SKU'].map(proj_map_sku)
    df_master['Project Name'] = df_master['Project Name'].fillna(df_master['SKU Base'].map(proj_map_base))
    # Asignar Dewalt XR
    df_master['Dewalt XR'] = df_master['SKU'].map(xr_map_sku)
    df_master['Dewalt XR'] = df_master['Dewalt XR'].fillna(df_master['SKU Base'].map(xr_map_base))
    # Asignar Industrial Type
    df_master['Dewalt Industrial'] = df_master['SKU'].map(ind_map_sku)
    df_master['Dewalt Industrial'] = df_master['Dewalt Industrial'].fillna(df_master['SKU Base'].map(ind_map_base))
    # 3. Lógica B+D (Vectorizada)
    # Limpiamos strings una sola vez para ganar eficiencia
    clean_brand = df_master['Brand'].str.lower().str.strip()
    clean_proj = df_master['Project Name'].str.lower().str.strip()
    
    mask_bdk = (clean_brand == 'black+decker') & (clean_proj == 'power connect 20v')
    df_master.loc[mask_bdk, 'B+D Power Connect 20V'] = 'B+D Power Connect 20V'
    return df_master




