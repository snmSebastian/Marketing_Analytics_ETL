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

def obtain_new_products(df_fill_rate, df_sales, df_demand, df_new_products,df_master_products):
    lst_columns_fill_and_sales=['Country Material', 'Country Material Name','LAG Brand',
                                'GPP Division Code', 'GPP Division', 'GPP Category', 'GPP Portfolio']
    lst_columns_demand=['Global Material', 'Global Material Description','LAG Brand',
                        'GPP Division Code','GPP Division', 'GPP Category', 'GPP Portfolio']
    df_fill_rate=df_fill_rate[lst_columns_fill_and_sales]
    df_sales=df_sales[lst_columns_fill_and_sales]
    df_demand=df_demand[lst_columns_demand]

    df_sales_fill_rate=pd.concat([df_fill_rate, df_sales], ignore_index=True)
    df_sales_fill_rate = df_sales_fill_rate.rename(columns={
        'Country Material': 'SKU',
        'Country Material Name': 'SKU Description',
        'GPP Division':'GPP Division Description',
        'GPP Category': 'GPP Category Description',
        'GPP Portfolio': 'GPP Portfolio Description',
        'LAG Brand': 'Brand'})

    df_demand = df_demand.rename(columns={
        'Global Material': 'SKU',
        'Global Material Description': 'SKU Description',
        'GPP Division':'GPP Division Description',
        'GPP Category': 'GPP Category Description',
        'GPP Portfolio': 'GPP Portfolio Description',
        'LAG Brand': 'Brand'})
    
    df_new_products = pd.concat([df_sales_fill_rate, df_demand], ignore_index=True)
    df_new_products = df_new_products.drop_duplicates(subset=['SKU'], keep='first')

    df_new_products = df_new_products[~df_new_products['SKU'].isin(df_master_products['SKU'])]
    
    lst_colums_gpp=['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP', 'GPP SBU',
       'GPP SBU Description', 'SBU Type', 'GPP Division Code',
       'GPP Division Description', 'GPP Category Code',
       'GPP Category Description', 'GPP Portfolio Code',
       'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
       'Voltaje', 'Bare', 'Sub-Brand','origen_sku','check_sku']
    
    columnas_a_crear=[col for col in lst_colums_gpp if col not in df_new_products.columns]
    for col in columnas_a_crear:
        df_new_products[col] = pd.Series(np.nan, index=df_new_products.index, dtype='object')
    df_new_products = df_new_products[lst_colums_gpp]
    df_new_products['origen_sku'] = 'new sku'
    return df_new_products

def assign_sku_base(sku_completo, skus_base_set):
    """
    Busca el SKU base probando todos los prefijos del SKU completo,
    desde el más largo al más corto. No requiere longitudes mínimas/máximas predefinidas.

    Donde L_nuevo es la longitud del SKU completo.
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
    Rellena los valores NaN de las columnas especificadas en el DataFrame destino 
    (df_target) usando la información del DataFrame fuente (df_source), cruzando
    por la clave o conjunto de claves (key_column). Crea las columnas faltantes 
    en df_target si no existen.
    """
    
    # ---------------- 0. Preparación de Claves y Copia ----------------
    # Asegúrate de que key_column sea una lista, dado que drop_duplicates y set_index requieren una lista de columnas.
    if isinstance(key_column, str):
        key_list = [key_column]
    else:
        key_list = key_column
    # sufijo para las columnas del dataframe fuente
    suffix = '_SRC' 
    df_result = df_target.copy() 

    # ---------------- VERIFICACIÓN Y CREACIÓN DE COLUMNAS FALTANTES ----------------
    # Si el dataframe target no tiene las columnas a fusionar, las crea.
    # Esto es útil para evitar errores si las columnas no existen en el DataFrame destino.

    # crea una lista con las columnas que no existen en el dataframe target
    columns_missing = [col for col in columns_to_merge if col not in df_result.columns]
    
    if columns_missing:
        # Crea las columnas faltantes en df_result y las inicializa a NaN
        for col in columns_missing:
            df_result[col] = pd.Series([np.nan] * len(df_result), dtype='object') 

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
    """ Para aquellos sku que no tienen sku base, asigna el gpp por portafolio.
      Verifica si el portafolio dado por el  sistema existe o no, y asigna el gpp.
      
      retorna el gpp si existe, sino retorna "-".
    """
   
    portafolio_a_buscar = portafolio.strip().upper().replace(' ', '') 
    for port in lst_portafolio:
        if port.startswith(portafolio_a_buscar):
            gpp = df_gpp[df_gpp['fk_GPP_Portfolio'] == port]['GPP'].values
            if len(gpp) > 0:
                return gpp[0]
            else:
                return "-"

def verify_psd(sku, lst_psd):
    """ Verofoca si el sku esta en la base compartida por PSD, si es asi, asigna el gpp de PSD"""
    sku_a_buscar = sku.strip().upper().replace(' ', '')
    if sku_a_buscar in lst_psd:
        return "PSD-70-70X-70999"
    else:
        return "-"
    
def verify_gpp(gpp, lst_gpp):
    """ Verifica si el gpp existe en la lista de gpp, si es asi, retorna el gpp, sino retorna "-" """
    gpp_a_buscar = str(gpp).strip().upper().replace(' ', '')
    if gpp_a_buscar in lst_gpp:
        return gpp_a_buscar
    else:
        return "-"

def corded_or_cordless_or_gas(sku,description,category_description,portfolio_description,corded_or_cordless):
    """ 
    Verifica si el SKU es 'Corded' o 'Cordless' basándose en la presencia de ciertas palabras clave.
    """
    sku = sku.strip().upper().replace(' ', '')
    description = description.strip().upper().replace(' ', '')
    category_description = category_description.strip().upper().replace(' ', '')
    portfolio_description = portfolio_description.strip().upper().replace(' ', '')
    category_portafolio = category_description + portfolio_description

    lst_cdl_description = [
        'CORDLESS', 'CDL', 'INALAMBRIC', 'BATTERY', 'BATT', 'BRUSHLESS', 'XR', 'MAX', 'LI-ION',
        '2.4V', '3.6V', '3.8V', '4V', '4.8V', '6V', '7.2V', '8V', '9.6V', '10.8V', 
        '12V', '14.4V', '16V', '18V', '20V', '24V', '36V', '40V', '54V', '60V','120V',
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
    if any(elemento in description for elemento in lst_cdl_description) and not '220V' in description:
        return 'CORDLESS'
    if any(elemento in description for elemento in lst_crd_description):
        return 'CORDED'
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
    Asigna la cantidad de baterías a un SKU basado en su descripción.
    Si la descripción contiene información sobre baterías, se extrae y se asigna.
    """
    sku = str(sku).strip().upper().replace(' ', '')
    if '/' in sku:
        sku= sku.split('/')[0]  # Toma la parte antes de la barra diagonal
    
    description = description.strip().upper().replace(' ', '')
    
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
        return batteries_qty[-1]
    if sku.endswith('B'):
        # Si el SKU termina con 'B', significa que no tiene batería
        return 0
    return batteries_qty  # Retorna el valor original si no se encuentra información relevante

def assing_voltaje(description, voltaje):
    """
    Asigna el voltaje a un SKU basado en su descripción.
    Si la descripción contiene información sobre voltaje, se extrae y se asigna.
    """
    description = description.strip().upper().replace(' ', '')
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
    Asigna el valor de 'Bare' a un SKU basado en la cantidad de baterías.
    Bare: el sku indica que no trae baterias
    non bare: el sku indica que trae baterias
    bare+batteries: la descripcion  indica que trae baterias y es el sku es bare
    """
    sku = str(sku).strip().upper().replace(' ', '')
    sku = sku.split('/')[0]  # Toma la parte antes de la barra diagonal
    if corded_or_cordless == 'CORDLESS':
        if '-' in quantity_batteries:
            quantity_batteries=0
        else:
            quantity_batteries=int(quantity_batteries)

        if quantity_batteries == 0 and (sku.endswith('B') or not sku.endswith('B')) :
            return 'Bare'
        elif quantity_batteries > 0 and not sku.endswith('B'):
            return 'Non Bare'
        elif quantity_batteries > 0 and  sku.endswith('B'):
            return 'Bare + Batteries'
        else:
            return '-'
        
def assign_sub_brand(sku,description,brand):
    description = description.strip().upper().replace(' ', '')
    brand = brand.strip().upper().replace(' ', '')
    if 'FATMA' in description:
        return 'FATMAX'
    elif sku.startswith('E') and brand == 'FACOM':
        return 'IAR EXPERT'
    elif sku.startswith('STA82'):
        return 'MASS'
    else:
        return "-"  # Retorna la marca original si no se encuentra una sub-marca específica

def main():
    #-------------------------------
    #---- RUTAS DE LOS ARCHIVOS
    #-------------------------------
    
    # Archivos para obtener new products
    path_fill_rate=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Fill Rate\Mothly_Update'
    path_sales=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Sales\Mothly_Update'
    path_demand=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Demand\Mothly_Update'
    
    # Archivos para almacenar y actualizar los productos    
    path_master_products=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Master_Products\Master_Product-prueba.xlsx'
    path_New_Products=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Products\Sku_for_Review_prueba.xlsx'
    path_gpp=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Master_Products\GPP-Brand.xlsx'
    path_psd=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Products\sku_shared_of_PSD.xlsx'

    #-----------------------------
    #----  Cargo dataframes
    #-----------------------------
    df_fill_rate=read_files(path_fill_rate)
    df_sales=read_files(path_sales)
    df_demand=read_files(path_demand)

    df_master_products=pd.read_excel(path_master_products, dtype=str, engine='openpyxl')
    df_new_products=pd.read_excel(path_New_Products, dtype=str, engine='openpyxl')

    df_gpp=pd.read_excel(path_gpp, dtype=str, engine='openpyxl',sheet_name='GPP')
    df_brand=pd.read_excel(path_gpp, dtype=str, engine='openpyxl',sheet_name='Brand')
    df_psd=pd.read_excel(path_psd, dtype=str, engine='openpyxl')
    
    #---------------------------------------------------
    #--- Genero el archivo con los nuevos productos
    #----------------------------------------------------
    df_new_products= obtain_new_products(df_fill_rate, df_sales, df_demand, df_new_products,df_master_products)
    
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
    
    #-------------- Verifico si los sku que aun no tienen gpp, estan en la base compartida por PSD    
    df_posibble_psd = df_new_products_sin_base[df_new_products_sin_base['GPP'] == '-'].copy()
    # elimino los sku que no tienen gpp para no genrar duplicados al concatenar
    df_new_products_sin_base = df_new_products_sin_base[df_new_products_sin_base['GPP'] != '-']
    
    columns_merge_sku_sin_base = [ 'GPP SBU', 'GPP SBU Description', 'SBU Type', 
        'GPP Division Code', 'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code', 
        'GPP Portfolio Description']
    df_new_products_sin_base.loc[:,columns_merge_sku_sin_base]= np.nan  # Inicializo las columnas a NaN
    key_column = ['GPP']
    df_new_products_sin_base = assign_info_by_key(
        df_new_products_sin_base, 
        df_gpp, 
        key_column, 
        columns_merge_sku_sin_base
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
    df_new_products_con_base['como se asigno gpp'] = 'sku base'
    df_new_products_sin_base['como se asigno gpp'] = 'por portafolio dado por SAP'
    df_posibble_psd['como se asigno gpp'] = 'sku esta en la base compartida por PSD'

    # Ordeno las columnas de los dataframes
    lst_colums_gpp=['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP', 'GPP SBU',
    'GPP SBU Description', 'SBU Type', 'GPP Division Code',
    'GPP Division Description', 'GPP Category Code',
    'GPP Category Description', 'GPP Portfolio Code',
    'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
    'Voltaje', 'Bare', 'Sub-Brand','origen_sku','check_sku','¿como se asigno gpp?']
    df_new_products_con_base = df_new_products_con_base[lst_colums_gpp]
    df_new_products_sin_base = df_new_products_sin_base[lst_colums_gpp]
    df_posibble_psd = df_posibble_psd[lst_colums_gpp]
    # Genero el nuevo dataframe con los nuevos productos a partir de los dataframes con y sin sku base
    df_new_products_gpp=pd.concat([df_new_products_con_base, df_new_products_sin_base,df_posibble_psd], ignore_index=True)    
    
    # Tratamiento de los espacios en blanco y NaN
    df_new_products_gpp = df_new_products_gpp.fillna(value='-')
    
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

    # Exporto a excel el dataframe de nuevos productos
    df_new_products_gpp.to_excel(path_New_Products, index=False)

if __name__ == "__main__":
    main()
    print("Proceso de actualización de productos completado exitosamente.")