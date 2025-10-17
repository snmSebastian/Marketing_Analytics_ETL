#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
# Liberia
import pandas as pd
import numpy as np
# Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico.
import glob
import os

from Fill_Rate.Process_ETL.Process_Files import asign_country_code, read_files

def obtain_new_products(df_master, df_consolidated):

def main():
    #--------------------------------------------------
    #---------------- RUTAS ---------------------------
    
    # Archivos para obtener new products
    path_fill_rate=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Fill Rate\Mothly_Update'
    path_sales=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Sales\Mothly_Update'
    path_demnad=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Demand\Mothly_Update\'Download_Demand.xlsx'

    # Archivos con informacion de PWT-HTS
    path_hts=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Products\Complete_Information_SKUs_PWT-HTS\Assing_Information_HTS.xlsx'
    path_pwt=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Products\Complete_Information_SKUs_PWT-HTS\Assing_Information_PWT.xlsx'
    
    # Archivos para almacenar y actualizar los productos    
    path_master_products=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Master_Products\Master_Product.xlsx'
    path_New_Products=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Products\New_Product.xlsx'

    

    # --- LECTURA DE ARCHIVOS DE CONFIGURACIÓN ---
    df_fill_rate=read_files(path_fill_rate)
    df_sales=read_files(path_sales)
    df_demand=read_files(path_demnad)

    df_master_products=pd.read_excel(path_master_products, dtype=str, engine='openpyxl')
    df_new_products=pd.read_excel(path_New_Products, dtype=str, engine='openpyxl')
    
    # --- LECTURA Y CONSOLIDACIÓN DE DATOS DE ACTUALIZACIÓN --
    lst_columns=['Country Code', 'Destination Country','Sold-To Customer Code','Sold-To Customer','Sold-To Dist Channel']
    df_fill_rate=df_fill_rate[lst_columns]
    df_sales=df_sales[lst_columns]
    
    df_consolidated=pd.concat([df_fill_rate, df_sales], ignore_index=True)