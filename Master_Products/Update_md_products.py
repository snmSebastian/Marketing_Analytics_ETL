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
def main():
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: MD PRODUCTS UPDATE ETL (OPTIMIZED) ---")
    print("=" * 55)
    from config_paths import MasterProductsPaths
    path_md_product = MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE_PRUEBA
    path_sku_review = MasterProductsPaths.WORKFILE_NEW_PRODUCTS_REVIEW_FILE
    
    df_master_products_final = update_master_products(path_md_product, path_sku_review)
    df_master_products_final.to_excel(path_md_product, index=False)
    
if __name__ == "__main__":
    main()
    print("Proceso de actualización de productos completado exitosamente.")