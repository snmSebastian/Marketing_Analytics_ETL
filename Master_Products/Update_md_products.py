#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
# Liberia
import pandas as pd
import numpy as np

def update_master_products(path_md_product, path_sku_review):    
    # ---------------------------------------------------------------------
    # --- DEFINICIÓN DE COLUMNAS ---
    # ---------------------------------------------------------------------
    COL_KEY = 'SKU'
    lst_colums_gpp = [ 'SKU Base', 'SKU Description', 'Brand', 'GPP', 'GPP SBU',
    'GPP SBU Description', 'SBU Type', 'GPP Division Code',
    'GPP Division Description', 'GPP Category Code',
    'GPP Category Description', 'GPP Portfolio Code',
    'GPP Portfolio Description', 'Corded / Cordless', 'Batteries Qty',
    'Voltaje', 'Bare', 'Sub-Brand']

    lst_col_md_product = [COL_KEY]+lst_colums_gpp + ['Brand Group', 'Brand + SBU', 'Group 1',
        'Group 2', 'Category Group', 'Big Rock', 'Top Category', 'NPI Project',
        'Categoria HTS', 'Familia HTS', 'Sub Familia HTS', 'Clase HTS',
        'NPI Project HTS', 'Posicionamient HTS', 'Link']
    
    # ---------------------------------------------------------------------
    # --- LECTURA Y PREPROCESAMIENTO ---
    # ---------------------------------------------------------------------
    df_md_product = pd.read_excel(path_md_product, dtype=str, engine='openpyxl')
    df_sku_review = pd.read_excel(path_sku_review, dtype=str, engine='openpyxl')
    
    # Limpieza y filtrado (se mantiene la lógica)
    df_sku_review['check_sku'] = df_sku_review['check_sku'].str.lower().str.strip().str.replace(' ', '')
    df_updates = df_sku_review[
        (df_sku_review['check_sku'] == 'verified') | (df_sku_review['check_sku'] == 'ok')
    ].copy() 
    
    lst_sku_to_update = df_updates['SKU'].tolist()
    
    # ----------------------------------------------------------------------
    # --- ACTUALIZACIÓN VECTORIZADA DE PRODUCTOS EXISTENTES ---
    # ---------------------------------------------------------------------
    
    #  Copia del maestro y establecimiento de 'SKU' como índice para la actualización
    df_master_updated = df_md_product.set_index('SKU').copy()
    
    #  DataFrame de cambios: Solo los SKUs que YA existen en el maestro
    sku_existing_mask = df_updates['SKU'].isin(df_master_updated.index)
    df_updates_existing = df_updates[sku_existing_mask].set_index('SKU')
    
    # Se actualizan los sku que no son nuevos pero sufrieron cambios de gpp 
    df_master_updated.update(df_updates_existing[lst_colums_gpp])
    
    # Devolver el índice a columna normal
    df_master_updated = df_master_updated.reset_index()

    # ----------------------------------------------------------------------
    # --- INCORPORACIÓN DE PRODUCTOS NUEVOS ---
    # ---------------------------------------------------------------------

    # Productos nuevos: SKUs que NO existen en el maestro
    df_new_products = df_updates[~sku_existing_mask].copy()

    # Gestionar columnas faltantes en los productos nuevos
    columnas_a_crear = [col for col in lst_col_md_product if col not in df_new_products.columns]
    if columnas_a_crear:
        # Crea todas las columnas faltantes y las rellena con '-' en una sola operación
        for col in columnas_a_crear:
            df_new_products[col] = '-'     
    # Seleccionar y reordenar las columnas del nuevo DataFrame para que coincidan con el maestro
    df_new_products = df_new_products[lst_col_md_product]
    
    # ----------------------------------------------------------------------
    # --- CONSOLIDACIÓN Y EXPORTACIÓN ---
    # ----------------------------------------------------------------------
    
    # Concatenar el maestro actualizado y los nuevos productos
    df_master_products_final = pd.concat([df_master_updated, df_new_products], ignore_index=True)
    
    # Asegurarse de que el DataFrame final solo tenga las columnas correctas, si hubo alguna diferencia
    df_master_products_final = df_master_products_final[lst_col_md_product]
    
    return df_master_products_final


def update_hts_pwt(df_md_product,df_hts_or_pwt,lst_columns_hts_or_pwt):
    df_hts_or_pwt=df_hts_or_pwt.set_index('SKU').copy()
    df_md_product=df_md_product.set_index('SKU').copy()
    df_md_product.update(df_hts_or_pwt[lst_columns_hts_or_pwt])
    df_md_product=df_md_product.reset_index()
    return df_md_product

def assign_brand(row, dic_brand_standardization):
    for key, values in dic_brand_standardization.items():
        if any(value in row['Brand'] for value in values):
            return key



def main():
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: MD PRODUCTS UPDATE ETL ---")
    print("=" * 55)
    from config_paths import MasterProductsPaths
    path_md_product = MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE_PRUEBA
    path_sku_review = MasterProductsPaths.WORKFILE_NEW_PRODUCTS_REVIEW_FILE
    path_hts=MasterProductsPaths.WORKFILE_HTS_FILE
    path_pwt=MasterProductsPaths.WORKFILE_PWT_FILE
    path_brand=MasterProductsPaths.INPUT_PROCESSED_GPP_BRAND_FILE
    path_gpp=MasterProductsPaths.INPUT_PROCESSED_GPP_BRAND_FILE

    df_hts=pd.read_excel(path_hts, dtype=str, engine='openpyxl')
    df_pwt=pd.read_excel(path_pwt, dtype=str, engine='openpyxl')
    df_brand=pd.read_excel(path_brand,sheet_name='Brand', dtype=str, engine='openpyxl')
    df_gpp=pd.read_excel(path_gpp,sheet_name='GPP', dtype=str, engine='openpyxl')

    # creo un df agregando los nuevos sku
    df_with_new_products = update_master_products(path_md_product, path_sku_review)
    
    #----------------------------------------
    # --- AGREGO INFO DE HTS - PWT
    #-------------------------------------
    
    lst_columns_hts=['Big Rock',
       'Top Category', 'NPI Project', 'Categoria HTS', 'Familia HTS',
       'Sub Familia HTS', 'Clase HTS', 'NPI Project HTS',
       'Posicionamiento HTS']
    df_with_new_products_and_hts=update_hts_pwt(df_with_new_products,df_hts,lst_columns_hts)
    
    lst_columns_pwt=['Group 1','Group 2']
    df_with_new_products_hts_and_pwt=update_hts_pwt(df_with_new_products_and_hts,df_pwt,lst_columns_pwt)

    #--------------------------------------------
    # --- TRATAMIENTO DE BRAND - BRAND GROUP 
    #--------------------------------------------
    
    # Diccionario notacion marca
    Brand_Standardization_Map = {
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
    # crea diccionario invertido
    brand_map_inverse={}
    for standard_brand, variations in Brand_Standardization_Map.items():
        # Normalizar la marca estándar para que coincida con la columna df_final['Brand']
        standard_key_normalized = standard_brand.replace(' ', '').upper()
        
        # Llenar el diccionario inverso: {VARIACIÓN_NORMALIZADA: MARCA_ESTÁNDAR_NORMALIZADA}
        for variation in variations:
            # Normalizar las variaciones (errores) también
            variation_normalized = variation.replace(' ', '').upper()
            brand_map_inverse[variation_normalized] = standard_key_normalized

    df_final=df_with_new_products_hts_and_pwt.copy()
    df_final['Brand_Clean']=df_final["Brand"].str.upper().str.strip().str.replace(' ', '')
    df_final['Brand'] = df_final['Brand_Clean'].map(brand_map_inverse).fillna(df_final['Brand'])
    df_final = df_final.drop(columns=['Brand_Clean'])

    # ASIGNACION DE BRAND GROUP
    df_brand=df_brand[['Brand', 'Brand Group']].set_index('Brand').copy()
    df_final=df_final.set_index('Brand').copy()
    df_final.update(df_brand)
    df_final=df_final.reset_index()

    df_final['Brand + SBU']=df_final['Brand']+' - '+df_final['GPP SBU']  
    
    #----------------------------------------------------
    # --- CATEGORY GROUP-BIG ROCK  -TOP CATEGORY
    #----------------------------------------------------
    
    df_gpp=df_gpp[['GPP','Category Group','Big Rock','Top Category']].set_index('GPP').copy()
    df_final=df_final.set_index('GPP').copy()
    df_final.update(df_gpp)

    df_final.to_excel(path_md_product, index=False)


# Ejemplo de cómo usarlo para la limpieza:
# df['Brand_Clean'] = df['Brand'].replace(brand_standardization_map, regex=True)
if __name__ == "__main__":
    main()
    print("Proceso de actualización de productos completado exitosamente.")