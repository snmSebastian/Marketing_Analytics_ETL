import pandas as pd
import numpy as np


def update_file_hts(md_product,lst_columns_hts,df_hts):
    # Filtro de md_products aquellos sku de hts y las columnas que necesito
    df_filter_hts=md_product[md_product['GPP SBU']=='HMT'][lst_columns_hts].copy()
    # determino una lst que indica si el sku del md esta en el archivo de hts
    mask_sku_md=df_filter_hts['SKU'].isin(df_hts['SKU'])
    # asigno si el sku es nuevo o no
    df_filter_hts['check_sku']=np.where(
                                mask_sku_md, 
                                'Verified', 
                                'New sku'
                             )
    # mascara para determinar si el sku pese a ser viejo necesita revision o le falta informacion
    hts_cols = ['Categoria HTS', 'Familia HTS', 'Sub Familia HTS', 'Clase HTS',
                'NPI Project HTS', 'Posicionamiento HTS']
    concat_series = df_filter_hts[hts_cols].fillna('').agg(''.join, axis=1)
    df_filter_hts['concat_info'] = concat_series.str.lower().str.strip().str.replace(' ', '')

    mask_sku_old=((df_filter_hts['check_sku']=='Verified') &
                  (df_filter_hts['concat_info'].str.contains('-')))
    
    df_filter_hts['check_sku']=np.where(
                                mask_sku_old, # Condición
                                'SKU Existente - Revisión: Faltan datos en campos clave', # Valor si la condición es True
                                df_filter_hts['check_sku'] # Valor si la condición es False (mantiene el valor original, que en este caso es 'verified')
                               )
    
    # ordeno dataframe de salida
    df_filter_hts=df_filter_hts.sort_values(by=['check_sku','SKU','SKU Base'])
    df_filter_hts=df_filter_hts[lst_columns_hts+['check_sku']]
    df_filter_hts=df_filter_hts.fillna('-')
    return df_filter_hts

def main():
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: HTS UPDATE ETL ---")
    print("=" * 55)
    from config_paths import MasterProductsPaths
    path_md_product = MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE_PRUEBA
    path_hts=MasterProductsPaths.WORKFILE_HTS_FILE
    
    lst_columns_hts=['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP SBU',
       'GPP Division Code', 'GPP Division Description',
       'GPP Category Description', 'GPP Portfolio Description', 'Big Rock',
       'Top Category', 'NPI Project', 'Categoria HTS', 'Familia HTS',
       'Sub Familia HTS', 'Clase HTS', 'NPI Project HTS',
       'Posicionamiento HTS']
    df_hts=pd.read_excel(path_hts, dtype=str, engine='openpyxl')
    df_md_product=pd.read_excel(path_md_product, dtype=str, engine='openpyxl')
    
    df_filter_hts=update_file_hts(df_md_product,lst_columns_hts,df_hts)
    df_filter_hts.to_excel(path_hts, index=False)
    
if __name__ == "__main__":
    main()
    print("Proceso de actualización de productos completado exitosamente.")

