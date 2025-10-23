import pandas as pd
import numpy as np

def update_file_pwt(md_product,lst_columns_pwt,df_pwt):
    df_filter_pwt=md_product[md_product['GPP SBU']=='PWT'][lst_columns_pwt].copy()
    mask_sku_md=df_filter_pwt['SKU'].isin(df_pwt['SKU'])
    df_filter_pwt['check_sku']=np.where(
                                mask_sku_md, 
                                'Verified', 
                                'New sku'
                             )
    df_filter_pwt['concat_info']=(df_filter_pwt['Group 1']+df_filter_pwt['Group 2']).str.lower().str.strip().str.replace(' ', '')
    mask_sku_old=((df_filter_pwt['check_sku']=='Verified') &
                  (df_filter_pwt['concat_info'].str.contains('-')))
    df_filter_pwt['check_sku']=np.where(
                                mask_sku_old, # Condición
                                'SKU Existente - Revisión: Faltan datos en campos clave', # Valor si la condición es True
                                df_filter_pwt['check_sku'] # Valor si la condición es False (mantiene el valor original, que en este caso es 'verified')
                                )
    df_filter_pwt=df_filter_pwt.sort_values(by=['check_sku','SKU','SKU Base'])
    df_filter_pwt=df_filter_pwt[lst_columns_pwt+['check_sku']]
    df_filter_pwt=df_filter_pwt.fillna('-')
    return df_filter_pwt

def main():
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: PWT UPDATE ETL ---")
    print("=" * 55)
    from config_paths import MasterProductsPaths
    path_md_product = MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE_PRUEBA
    path_pwt=MasterProductsPaths.WORKFILE_PWT_FILE
    lst_columns_pwt=['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP SBU',
       'GPP Division Code', 'GPP Division Description',
       'GPP Category Description', 'GPP Portfolio Description','Group 1','Group 2']
    df_pwt=pd.read_excel(path_pwt, dtype=str, engine='openpyxl')
    df_md_product=pd.read_excel(path_md_product, dtype=str, engine='openpyxl')
    
    df_update_pwt=update_file_pwt(df_md_product,lst_columns_pwt,df_pwt)
    df_update_pwt.to_excel(path_pwt, index=False)
    
if __name__ == "__main__":
    main()
    print("Proceso de actualización de productos completado exitosamente.")

        