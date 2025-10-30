"""
Módulo de Carga y Actualización Final del Maestro de Productos.
Este script realiza el proceso de Upsert (Update/Insert) en el archivo
Master Products. Primero, consolida y aplica los cambios verificados
del archivo de revisión. Luego, enriquece y normaliza los datos con
información de Brand Group, Category Group y las clasificaciones HTS/PWT.
"""

# ---------------- LIBRERIAS -----------------------
# --------------------------------------------------
import pandas as pd
import numpy as np
import  sys
# mapa de estandarización de marcas 
BRAND_STANDARD_MAP = {
    "BLACK + DECKER": ["B+D", "BLACK&DECKER", "BLACKANDDECKER", "BLACK+DECKER®", "BLACK + DECKER"],
    "DEWALT": ["DEWALT®", "DWLT", "DEWALT"],
    "STANLEY": ["STANLEY®", "STANLEYTOOLS", "STANLEY"],
    "CRAFTSMAN": ["CRAFTSMAN", "CRAFTSMN", "CRAFTSMAN®"],
    "PORTER CABLE": ["PORTERCABLE", "PORTER-CABLE", "PCABLE"],
    "FATMAX": ["FATMAX","FATMAXX", "FAT MAX"],
    "IRWIN": ["IRWIN", "IRWININDUSTRIAL"],
    "PROTO": ["PROTO", "PROTOTOOLS","PROTOTOOL","PROTOTOO"],
    "FACOM": ["FACOM", "FACOMS.A.",'FACOMS','FACON','FACONS'],
    "BOSTITCH": ["BOSTITCH","BOSTICH","BOSTICH","BOSTICTH", "BOSTITCHSTANLEY"],
    "IAR EXPERT": ["IAR EXPERT", "IAREXPERT"],
    "LENOX": ["LENOX", "LENOXTOOLS","LNX"],
    "GRIDEST": ["GRIDEST", "GRYDEST"],
    "DEWALT POWERS": ["DEWALTPOWERS", "DWLTPOWERS", "DEWALTPOWER"],
    "TROY-BILT": ["TROYBILT", "TROY-BILT"],
    "YARD MACHINES": ["YARDMACHINES", "YARDMACH"],
    "GENUINE FACTORY PAR": ["GENUINEFACTORYPART", "GENUINEFACTORY", "GFPARTS"],
    "OTHER": ["OTHERS", "OTHERBRAND", "OTRA","OTH", "OTHER"],
    "SAT (SSS)": ["SAT", "SSS", "SATSSS"],
    "CUB CADET": ["CUB CADET", "CUBCADET"],
    "DELTA": ["DELTA", "DELTAPOWER"],
    "BIESEMEYER": ["BIESEMEYER"],
    "TRIMMER PLUS": ["TRIMMERPLUS", "TRIMMER+"],
    "SIDCHROME": ["SIDCHROME", "SIDCHROMETOOLS", "SIDCHROME"]
}

def create_inverse_brand_map(standard_map):
    """"
    Genera un diccionario de mapeo inverso y normalizado a partir de un mapa estándar de marcas. Esto facilita el
    proceso de estandarización de marcas, permitiendo una búsqueda rápida y vectorizada de las variaciones de marca a su
    nombre estándar (ej., 'BLACK&DECKER' -> 'BLACK + DECKER').
    
    Args: standard_map (dict): Diccionario de marcas estándar con listas de variaciones

    Returns: dict: Diccionario con variaciones normalizadas (clave) y marcas estándar (valor).
    """
    inverse_map = {}
    for standard_brand, variations in standard_map.items():
        # Normaliza la clave estándar (la marca final)
        standard_key_normalized = standard_brand.replace(' ', '').upper()
        
        # Llena el diccionario inverso
        for variation in variations:
            variation_normalized = variation.replace(' ', '').upper()
            inverse_map[variation_normalized] = standard_key_normalized
    return inverse_map

def fill_missing_columns(df: pd.DataFrame, target_columns: list, fill_value: str = '-') -> pd.DataFrame:
    """
    Función utilitaria que asegura que un DataFrame (generalmente el de nuevos registros) contenga todas las columnas de la
    estructura final esperada. Las columnas faltantes son creadas e inicializadas con un valor por defecto ('-').

    Args:
        df (pd.DataFrame): DataFrame objetivo.
        target_columns (list): Lista de columnas deseadas.
        fill_value (str): Valor para rellenar las nuevas columnas.

    Returns: pd.DataFrame: DataFrame con las columnas completadas y ordenadas.
    """
    cols_to_create = [col for col in target_columns if col not in df.columns]
    if cols_to_create:
        for col in cols_to_create:
             df[col] = fill_value
    # Asegura que las columnas estén en el orden correcto
    return df[target_columns]


def update_master_products(path_md_product, path_sku_review, lst_col_md_product, lst_colums_gpp, col_key='SKU'):
    """
    Implementa la lógica de Upsert (Update/Insert) en el Maestro de Productos. Filtra el archivo de revisión por SKUs marcados
    como 'verified' u 'ok', y luego:
        1) Actualiza in place los registros existentes con los nuevos datos de clasificación GPP.
        2) Concatena los registros nuevos, asegurando la estructura de columnas completa.
    
    Args:
        path_md_product (str): Ruta al Maestro de Productos actual.
        path_sku_review (str): Ruta al archivo de revisión con los SKUs ya clasificados.
        lst_col_md_product (list): Columnas finales esperadas del Maestro.
        lst_colums_gpp (list): Columnas de clasificación a actualizar/insertar.
        col_key (str): Columna clave ('SKU') para el Upsert.
    Returns: pd.DataFrame: El Maestro de Productos actualizado con registros modificados y nuevos.
    """
    
    # --- LECTURA Y PREPROCESAMIENTO ---
    df_md_product = pd.read_excel(path_md_product, dtype=str, engine='openpyxl')
    df_sku_review = pd.read_excel(path_sku_review, dtype=str, engine='openpyxl')
    
    df_sku_review['check_sku'] = df_sku_review['check_sku'].str.lower().str.strip().str.replace(' ', '')
    df_updates = df_sku_review[
        (df_sku_review['check_sku'] == 'verified') | (df_sku_review['check_sku'] == 'ok')
    ].copy() 
    
    # --- ACTUALIZACIÓN VECTORIZADA DE PRODUCTOS EXISTENTES ---
    
    # Preparar el Maestro (df_md_product)
    df_master_updated = df_md_product.set_index(col_key).copy()
    
    # Preparar los cambios
    sku_existing_mask = df_updates[col_key].isin(df_master_updated.index)
    df_updates_existing = df_updates[sku_existing_mask].set_index(col_key)
    
    # 3. Aplicar actualización (solo columnas GPP)
    df_master_updated.update(df_updates_existing[lst_colums_gpp])
    df_master_updated = df_master_updated.reset_index()

    # --- INCORPORACIÓN DE PRODUCTOS NUEVOS ---
    df_new_products = df_updates[~sku_existing_mask].copy()

    # Gestionar columnas faltantes (Usando la nueva función modular)
    df_new_products = fill_missing_columns(df_new_products, lst_col_md_product)
    
    # --- CONSOLIDACIÓN ---
    df_master_products_final = pd.concat([df_master_updated, df_new_products], 
                                         ignore_index=True)
    
    # Aseguro de que el DataFrame final solo tenga las columnas correctas
    return df_master_products_final[lst_col_md_product]


def update_master_data(df_final: pd.DataFrame, df_source: pd.DataFrame, join_key: str, update_columns: list) -> pd.DataFrame:
    """
    Función utilitaria genérica que realiza una actualización basada en el índice (df.update()).
    Utiliza una columna clave (join_key) para alinear df_final con df_source y sobrescribe los valores de las
    update_columns en el destino con los valores del origen.
    
    Args:
        df_final (pd.DataFrame): El DataFrame que recibirá la actualización.
        df_source (pd.DataFrame): El DataFrame fuente con los datos a usar.
        join_key (str): Columna clave para indexar la actualización.
        update_columns (list): Lista de columnas a actualizar.
    
    Returns: pd.DataFrame: El DataFrame destino con los valores actualizados.
    """
    
    # Solo seleccionamos las columnas necesarias del DF de origen
    df_source_update = df_source[[join_key] + update_columns].set_index(join_key)
    
    df_final_temp = df_final.set_index(join_key).copy()
    df_final_temp.update(df_source_update)
    
    return df_final_temp.reset_index()


def main():
    """	
    Orquesta el proceso completo de Carga (L) y Consolidación del Maestro de Productos.	
    El flujo incluye:
        1) Ejecución del Upsert de SKUs base.
        2) Incorporación de datos HTS y PWT validados.	
        3) Estandarización de la columna Brand.
        4) Look-up final de Brand Group y Category Group (vía GPP).	
        5) Exportación del Maestro de Productos finalizado.	
    Returns: None: La función orquesta el proceso y sobrescribe el archivo Maestro de Productos final.
    """
    print("=" * 55)
    print("---  INICIANDO PROCESO: MD PRODUCTS UPDATE ETL ---")
    print("=" * 55)
    try:
        # --- 1. DEFINICIONES Y CONFIGURACIÓN ---
        COL_KEY = 'SKU'
        lst_colums_gpp = [ 'SKU Base', 'SKU Description', 'Brand', 'GPP', 'GPP SBU',
        'GPP SBU Description', 'SBU Type', 'GPP Division Code',
        'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code',
        'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
        'Voltaje', 'Bare', 'Sub-Brand']

        lst_col_md_product = [COL_KEY] + lst_colums_gpp + ['Brand Group', 'Brand + SBU', 'Group 1',
            'Group 2', 'Category Group', 'Big Rock', 'Top Category', 'NPI Project',
            'Categoria HTS', 'Familia HTS', 'Sub Familia HTS', 'Clase HTS',
            'NPI Project HTS', 'Posicionamiento HTS', 'Link']

        from config_paths import MasterProductsPaths
        path_md_product = MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE_PRUEBA
        path_sku_review = MasterProductsPaths.WORKFILE_NEW_PRODUCTS_REVIEW_FILE
        path_hts=MasterProductsPaths.WORKFILE_HTS_FILE
        path_pwt=MasterProductsPaths.WORKFILE_PWT_FILE
        path_brand_gpp=MasterProductsPaths.INPUT_PROCESSED_GPP_BRAND_FILE # Usamos una variable para el archivo

        # ---  LECTURA DE FUENTES ---
        df_hts = pd.read_excel(path_hts, dtype=str, engine='openpyxl')
        df_pwt = pd.read_excel(path_pwt, dtype=str, engine='openpyxl')
        df_brand = pd.read_excel(path_brand_gpp, sheet_name='Brand', dtype=str, engine='openpyxl')
        df_gpp = pd.read_excel(path_brand_gpp, sheet_name='GPP', dtype=str, engine='openpyxl')

        # ---  PROCESO ETL CENTRAL ---
        
        #  Actualizar/Agregar SKUs base
        df_final = update_master_products(path_md_product, path_sku_review, lst_col_md_product, lst_colums_gpp)
        
        #  Agregar info HTS
        lst_columns_hts=['Big Rock', 'Top Category', 'NPI Project', 'Categoria HTS', 'Familia HTS',
                        'Sub Familia HTS', 'Clase HTS', 'NPI Project HTS', 'Posicionamiento HTS']
        df_final = update_master_data(df_final, df_hts, COL_KEY, lst_columns_hts)
        
        #  Agregar info PWT
        lst_columns_pwt=['Group 1','Group 2']
        df_final = update_master_data(df_final, df_pwt, COL_KEY, lst_columns_pwt)

        #  TRATAMIENTO DE BRAND Y ASIGNACIÓN DE BRAND GROUP
        
        # Crear el mapa inverso solo una vez
        brand_map_inverse = create_inverse_brand_map(BRAND_STANDARD_MAP)
        
        #  Normalizar y mapear Brand (vectorizado y rápido)
        df_final['Brand_Normalized'] = df_final["Brand"].str.upper().str.strip().str.replace(' ', '')
        df_final['Brand'] = df_final['Brand_Normalized'].map(brand_map_inverse).fillna(df_final['Brand'])
        df_final = df_final.drop(columns=['Brand_Normalized'])

        # ASIGNACION DE BRAND GROUP (actualiza df_brand)
        #df_brand['Brand'] = df_brand['Brand'].str.replace(' ', '').str.upper().str.strip() # Normalizar Brand del source
        df_final = update_master_data(df_final, df_brand, 'Brand', ['Brand Group'])
        
        #  Columna calculada
        df_final['Brand + SBU'] = df_final['Brand'] + '-' + df_final['GPP SBU']
        
        # --- CATEGORY GROUP, BIG ROCK, TOP CATEGORY (actualiza df_gpp por 'GPP')
        lst_columns_gpp_cat=['Category Group','Big Rock','Top Category']
        df_gpp['GPP_Key'] = df_gpp['GPP'].str.upper().str.strip().str.replace(' ', '')
        df_gpp = df_gpp.drop(columns=['GPP'])


        df_final['GPP_Key'] = df_final['GPP'].str.upper().str.strip().str.replace(' ', '')

        df_final = update_master_data(df_final, df_gpp, 'GPP_Key', lst_columns_gpp_cat)
        df_final = df_final.drop(columns=['GPP_Key'])
        df_final =df_final[lst_col_md_product]
        df_final=df_final.sort_values(by=['GPP','SKU Base','SKU','Brand','SKU Description'])

        # ---EXPORTACIÓN ---
        df_final.to_excel(path_md_product, index=False)
        print("Proceso de actualización de productos completado exitosamente.")
        pass
    except Exception as e:
        print(f'Error en procesamiento de datos de Maestro de Productos: {e}')
        sys.exit(1)

if __name__ == "__main__":
    main()