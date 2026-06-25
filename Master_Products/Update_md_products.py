"""
MAESTRO DE PRODUCTOS: El Gran Consolidador y Estandarizador Regional
-------------------------------------------------------------------
Este script representa la etapa final del pipeline de productos. Su misión es ejecutar 
el proceso de "Upsert" (Update + Insert) para integrar los SKUs validados desde los 
archivos de revisión (Workfiles) al Maestro de Productos oficial. 

Es el filtro de calidad final: asegura que cada registro tenga una marca normalizada, 
una jerarquía GPP correcta y las clasificaciones técnicas (HTS/PWT) completas antes 
de que la data llegue a los tableros de Power BI.

FLUJO DE TRABAJO:
1. Sincronización de Revisiones: Filtra y carga únicamente los SKUs marcados como 
   'Verified' u 'OK' en el archivo de revisión de nuevos productos.
2. Enriquecimiento Técnico: Cruza la data con los módulos de HTS y PWT para 
   inyectar metadatos de categorías específicas y proyectos NPI.
3. Normalización Marcaria: Utiliza un motor de mapeo inverso para estandarizar 
   variaciones de nombres de marca (ej: 'B+D' o 'DewaltPower' -> 'DEWALT').
4. Clasificación Jerárquica: Asigna Brand Groups y Category Groups de forma 
   vectorizada para garantizar coherencia en el reporte regional.
5. Persistencia Final: Consolida, ordena y sobrescribe el Maestro de Productos.

💡 NOTA DE SENIOR:
El diccionario `BRAND_STANDARD_MAP` es crítico. Cualquier variante nueva detectada en 
fuentes externas debe incluirse allí para evitar duplicidad de marcas en el modelo.
"""

# ---------------- LIBRERIAS -----------------------
# --------------------------------------------------
import pandas as pd
import numpy as np
import  sys
from Master_Products.column_processing import  assign_proyects,assign_sub_brand
from Fill_Rate.Process_ETL.Process_Files import clean_sku

# mapa de estandarización de marcas 
BRAND_STANDARD_MAP = {
    "Black + Decker": ["BLACK + DECKER","BLACK+DECKER","B+D", "BLACK&DECKER", "BLACKANDDECKER", "BLACK+DECKER®", "BLACK + DECKER","BLACK+DECKER"],
    "DEWALT": ["DEWALT®", "DWLT", "DEWALT","DEWALTPOWERS", "DWLTPOWERS", "DEWALTPOWER"],
    "STANLEY": ["STANLEY®", "STANLEYTOOLS", "STANLEY"],
    "CRAFTSMAN": ["CRAFTSMAN", "CRAFTSMN","CRAFTMAN", "CRAFTSMAN®"],
    "PORTER CABLE": ["PORTERCABLE", "PORTER-CABLE", "PCABLE"],
    "FATMAX": ["FATMAX","FATMAXX", "FAT MAX"],
    "IRWIN": ["IRWIN", "IRWININDUSTRIAL"],
    "PROTO": ["PROTO", "PROTOTOOLS","PROTOTOOL","PROTOTOO"],
    "FACOM": ["FACOM", "FACOMS.A.",'FACOMS','FACON','FACONS'],
    "BOSTITCH": ["BOSTITCH","BOSTICH","BOSTICH","BOSTICTH", "BOSTITCHSTANLEY"],
    "IAR EXPERT": ["IAR EXPERT", "IAREXPERT"],
    "LENOX": ["LENOX", "LENOXTOOLS","LNX"],
    "GRIDEST": ["GRIDEST", "GRYDEST"],
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


def update_master_products(path_md_product, path_sku_review, lst_col_md_product, lst_colums_by_refresh, col_key='SKU'):
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
    
    df_md_product = df_md_product.drop_duplicates(subset=['SKU'])
    df_sku_review = df_sku_review.drop_duplicates(subset=['SKU'])
    

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
    
    # 3. Aplicar actualización (solo columnas por actualizar)
    df_master_updated.update(df_updates_existing[lst_colums_by_refresh])
    df_master_updated = df_master_updated.reset_index()

    # --- INCORPORACIÓN DE PRODUCTOS NUEVOS ---
    df_new_products = df_updates[~sku_existing_mask].copy()

    # Gestionar columnas faltantes 
    df_new_products = fill_missing_columns(df_new_products, lst_col_md_product)
    
    # --- CONSOLIDACIÓN ---
    df_master_products_final = pd.concat([df_master_updated, df_new_products], 
                                         ignore_index=True)
    df_master_products_final.drop_duplicates(subset=['SKU'], inplace=True)
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
    # 1. Limpiar duplicados en la fuente para asegurar mapeo 1 a 1
    df_src_clean = df_source.drop_duplicates(subset=[join_key])

    
    df_source_update = df_src_clean[[join_key] + update_columns].set_index(join_key)
    
    df_final_temp = df_final.set_index(join_key).copy()
    df_final_temp.update(df_source_update)
    
    return df_final_temp.reset_index()

def assign_seed_type(df):
    # Definir condiciones
    conditions = [
        (df['Corded / Cordless'] == 'Cordless') & (df['GPP Division Code'] == '49'),
        (df['Corded / Cordless'] == 'Cordless') & (df['GPP Division Code'] != '49')
    ]
    
    # Definir resultados
    choices = [
        'Sold Separately & Giveaway', 
        'Sold with Tool'
    ]
    
    # Aplicar lógica (el 'default' es para cuando no cumple ninguna)
    df['Seed Type'] = np.select(conditions, choices, default='Other')
    
    return df


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
        lst_colums_by_refresh = [ 'SKU Base', 'SKU Description', 'Brand', 'GPP', 'GPP SBU',
        'GPP SBU Description', 'SBU Type', 'GPP Division Code',
        'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code',
        'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
        'Voltaje', 'Bare']

        lst_colums_create=[
            # Info tomada del archivo de brand
            'Sub-Brand','Brand Group',
            #Columna creada
            'Brand + SBU',
            #Archivo de Carlos( no lo ha actualizado)
             'Group 1','Group 2',
            #Primero cruzo por GPP y lo vacio lo completo con Archivo Jorge
            'Category Group', 'Big Rock', 

            #Archivo Jorge 
              'NPI Project',
            'Categoria HTS', 'Familia HTS', 'Sub Familia HTS', 'Clase HTS',
            'NPI Project HTS', 'Posicionamiento HTS', 'Project Name',
            #Archivo compartido con los sku
            'Dewalt XR','Dewalt Industrial',
            'B+D Power Connect 20V',

            'SKU Type','Seed Type','Link']
        
        lst_col_md_product = [COL_KEY] + lst_colums_by_refresh + lst_colums_create
        lst_col_gpp=['GPP', 'GPP SBU',
        'GPP SBU Description', 'SBU Type', 'GPP Division Code',
        'GPP Division Description', 'GPP Category Code',
        'GPP Category Description', 'GPP Portfolio Code',
        'GPP Portfolio Description']

        from config_paths import MasterProductsPaths
        path_md_product = MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE
        path_sku_review = MasterProductsPaths.WORKFILE_NEW_PRODUCTS_REVIEW_FILE
        path_hts=MasterProductsPaths.WORKFILE_HTS_FILE
        path_pwt=MasterProductsPaths.WORKFILE_PWT_FILE
        path_brand_gpp=MasterProductsPaths.INPUT_PROCESSED_GPP_BRAND_FILE # Usamos una variable para el archivo
        path_proyects=MasterProductsPaths.INPUT_PROCESSED_PROYECTS_FILE

        # ---  LECTURA DE FUENTES ---
        df_hts = pd.read_excel(path_hts, dtype=str, engine='openpyxl')
        df_pwt = pd.read_excel(path_pwt, dtype=str, engine='openpyxl')
        df_brand = pd.read_excel(path_brand_gpp, sheet_name='Brand', dtype=str, engine='openpyxl')
        df_gpp = pd.read_excel(path_brand_gpp, sheet_name='GPP', dtype=str, engine='openpyxl')
        df_proyects=pd.read_excel(path_proyects, dtype=str, engine='openpyxl',sheet_name='Proyects')
        df_dewaltXR=pd.read_excel(path_proyects, dtype=str, engine='openpyxl',sheet_name='DW_XR')
        df_dw_ind=pd.read_excel(path_proyects, dtype=str, engine='openpyxl',sheet_name='Dewalt Indust_Type')
    


        #-------------------------------
        #--- limpieza sku
        #------------------------------
        
        df_hts = clean_sku(df_hts, 'SKU').drop_duplicates(subset=['SKU'])
        df_pwt = clean_sku(df_pwt, 'SKU').drop_duplicates(subset=['SKU'])
        df_proyects = clean_sku(df_proyects, 'SKU').drop_duplicates(subset=['SKU'])
        df_dewaltXR = clean_sku(df_dewaltXR, 'SKU').drop_duplicates(subset=['SKU'])
        df_dw_ind = clean_sku(df_dw_ind, 'SKU').drop_duplicates(subset=['SKU'])
        df_gpp = clean_sku(df_gpp, 'GPP').drop_duplicates(subset=['GPP'])


       
        # ---  PROCESO ETL CENTRAL ---
        
        #  Actualizar/Agregar SKUs base (solo las columnas lst_colums_by_refresh)
        df_final = update_master_products(path_md_product, path_sku_review, lst_col_md_product, lst_colums_by_refresh)
        df_final = clean_sku(df_final, 'SKU')
        df_final=df_final.drop_duplicates(subset=['SKU'])

        print('update master products')

        df_final=update_master_data(df_final,df_gpp,'GPP',lst_col_gpp)
        print('update master gpp')
       
        #  TRATAMIENTO DE BRAND Y ASIGNACIÓN DE BRAND GROUP
        
        # Crear el mapa inverso solo una vez
        brand_map_inverse = create_inverse_brand_map(BRAND_STANDARD_MAP)
        print('create inverse brand map')
        #  Normalizar y mapear Brand (vectorizado y rápido)
        df_final['Brand_Normalized'] = df_final['Brand'].str.upper().str.strip().str.replace(' ', '')
        df_final['Brand'] = df_final['Brand_Normalized'].map(brand_map_inverse).fillna(df_final['Brand'])
        df_final = df_final.drop(columns=['Brand_Normalized'])

        # ASIGNACION DE BRAND GROUP (actualiza df_brand)
        # Usamos merge en lugar de update porque 'Brand' tiene duplicados en df_final
        if 'Brand Group' in df_final.columns:
            df_final = df_final.drop(columns=['Brand Group'])

        df_final['Brand']=df_final['Brand'].str.upper().str.strip().str.replace(' ', '')
        df_brand['Brand'] = df_brand['Brand'].str.upper().str.strip().str.replace(' ', '')
        df_final = pd.merge(df_final, df_brand[['Brand', 'Brand Group']].drop_duplicates('Brand'), on='Brand', how='left')
        df_final['Brand'] = df_final['Brand'].replace('BLACK+DECKER', 'BLACK + DECKER')
        print('update master brand')
        #  Columna calculada
        df_final['Brand + SBU'] = df_final['Brand'] + '-' + df_final['SBU Type']
        
        #  Asigno sub-brand
        df_final['Sub-Brand'] = df_final.apply(
        lambda row: assign_sub_brand(row['SKU'], row['SKU Description'], row['Brand']), axis=1)



        # --- CATEGORY GROUP, BIG ROCK, TOP CATEGORY (actualiza df_gpp por 'GPP')
        # --- Actualizo la notacion de GPP para que complete espacios para GPP= N/A-N/A-N/A
        lst_columns_gpp_cat=['GPP SBU','GPP SBU Description','SBU Type',
                             'GPP Division Code','GPP Division Description', 
                             'GPP Category Code','GPP Category Description',
                             'GPP Portfolio Code','GPP Portfolio Description',
                             'Category Group','Big Rock']
        df_gpp['GPP_Key'] = df_gpp['GPP'].str.upper().str.strip().str.replace(' ', '')
        df_gpp = df_gpp.drop(columns=['GPP'])

        df_final['GPP_Key'] = df_final['GPP'].str.upper().str.strip().str.replace(' ', '')
        # Actualizo las columnas de clasificacion con base en su GPP
        df_final = update_master_data(df_final, df_gpp, 'GPP_Key', lst_columns_gpp_cat)
        print('update master gpp')
        df_final = df_final.drop(columns=['GPP_Key'])
       

         #  Agregar info HTS
         #completa los nulos en 'Big Rock', 'Top Category' con info de jorge
        lst_columns_hts=['Big Rock', 'Category Group', 'Categoria HTS', 'Familia HTS',
                        'Sub Familia HTS', 'Clase HTS', 'NPI Project HTS', 'Posicionamiento HTS']
        df_final = update_master_data(df_final, df_hts, COL_KEY, lst_columns_hts)
        print('update master hts')
        #  Agregar info PWT
        lst_columns_pwt=['Group 1','Group 2']
        df_final = update_master_data(df_final, df_pwt, COL_KEY, lst_columns_pwt)
        print('update master pwt') 


        #Asigno nombre de proyectos
        df_md_products_updated= assign_proyects(df_final, df_proyects, df_dewaltXR, df_dw_ind)

        #Asigno Seed Type
        df_md_products_updated=assign_seed_type(df_md_products_updated)

        #Selecciono y ordeno las columnas que finalmente compornen el md products
        df_md_products_updated =df_md_products_updated[lst_col_md_product]

        # Ordeno md products
        df_md_products_updated=df_md_products_updated.sort_values(by=['GPP','SKU Base','SKU','Brand','SKU Description'])
        df_md_products_updated.drop_duplicates(subset=['SKU'], inplace=True)


        # ---EXPORTACIÓN ---
        df_md_products_updated.to_excel(path_md_product, index=False)
        print("Proceso de actualización de productos completado exitosamente.")
        pass
    except Exception as e:
        print(f'Error en procesamiento de datos de Maestro de Productos: {e}')
        sys.exit(1)

if __name__ == "__main__":
    main()