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
    a

def main():
    #--------------------------------------------------
    #---------------- RUTAS ---------------------------
    from config_paths import MasterProductsPaths
    path_fill_rate_update=MasterProductsPaths.INPUT_RAW_UPDATE_FILL_RATE_DIR
    path_sales_update=MasterProductsPaths.INPUT_RAW_UPDATE_SALES_DIR
    path_demand_update=MasterProductsPaths.INPUT_RAW_UPDATE_DEMAND_DIR
    path_country_code_file=MasterProductsPaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
    path_producst_hts=MasterProductsPaths.WORKFILE_HTS_FILE
    path_producst_pwt=MasterProductsPaths.WORKFILE_PWT_FILE
    path_New_Products=MasterProductsPaths.WORKFILE_NEW_PRODUCTS_REVIEW_FILE    
    path_master_products=MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE_PRUEBA
    #path_master_products=MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE
    
    # --- LECTURA DE ARCHIVOS DE CONFIGURACIÓN ---
    df_fill_rate=read_files(path_fill_rate_update)
    df_sales=read_files(path_sales_update)
    df_demand=read_files(path_demand_update)

    df_master_products=pd.read_excel(path_master_products, dtype=str, engine='openpyxl')
    df_new_products=pd.read_excel(path_New_Products, dtype=str, engine='openpyxl')
    
    # --- LECTURA Y CONSOLIDACIÓN DE DATOS DE ACTUALIZACIÓN --
    lst_columns=['Country Code', 'Destination Country','Sold-To Customer Code','Sold-To Customer','Sold-To Dist Channel']
    df_fill_rate=df_fill_rate[lst_columns]
    df_sales=df_sales[lst_columns]
    
    df_consolidated=pd.concat([df_fill_rate, df_sales], ignore_index=True)