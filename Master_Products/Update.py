#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
# Liberia
import pandas as pd
import numpy as np
# Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico.
import glob
import os

from Fill_Rate.Process_ETL.Process_Files import asign_country_code, read_files


def main():
    #--------------------------------------------------
    #---------------- RUTAS ---------------------------
    
    path_fill_rate=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Fill Rate\Mothly_Update'
    path_sales=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Sales\Mothly_Update'
    path_products=
    path_prueba=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Master_Customers\prueba.xlsx'

    # --- LECTURA DE ARCHIVOS DE CONFIGURACIÓN ---
    df_fill_rate=read_files(path_fill_rate)
    df_sales=read_files(path_sales)
    df_master=pd.read_excel(path_customers, dtype=str, engine='openpyxl')

    # --- LECTURA Y CONSOLIDACIÓN DE DATOS DE ACTUALIZACIÓN --
    lst_columns=['Country Code', 'Destination Country','Sold-To Customer Code','Sold-To Customer','Sold-To Dist Channel']
    df_fill_rate=df_fill_rate[lst_columns]
    df_sales=df_sales[lst_columns]
    
    df_consolidated=pd.concat([df_fill_rate, df_sales], ignore_index=True)