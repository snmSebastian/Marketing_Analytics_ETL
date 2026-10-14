
"""
Mercado libre: toma la informacion de one drive que comparte Wiliiam y la pega en sharepoint
No hace ninguna transformacion
"""

# Librerias
import pandas as pd
import numpy as np
import glob
import os
import sys
from datetime import datetime

from pathlib import Path
from Fill_Rate.Process_ETL.Process_Files import clean_sku
from Master_Products.Update_md_products import create_inverse_brand_map,BRAND_STANDARD_MAP
from Datamind.Process_Files import *


def  main():
    """

    """
    from config_paths import DatamindPaths
    path_sales_onedrive = Path(r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Shared Information\Mercado Libre\Analytics LAG ONE TEAM - Meli_No_Seller_Data_V2')
    path_meli_sales = DatamindPaths.INPUT_RAW_MELI_SALES_FILE
    path_sku_onedrive = Path(r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Shared Information\Mercado Libre\Analytics LAG ONE TEAM - Meli_SKU_Clean\Meli_SKU_Clean_V2.xlsx')
    path_meli_sku = DatamindPaths.INPUT_RAW_MELI_SKU_FILE
    try:
        print('read sku')
        df_sku=pd.read_excel(path_sku_onedrive)
        print('read sales')
        df_2024=pd.read_excel(path_sales_onedrive/'meli_sbd_202438_lastweek.xlsx',dtype=str)
        df_2025=pd.read_excel(path_sales_onedrive/'meli_sbd_202501_lastweek.xlsx',dtype=str)
        df_2026=pd.read_excel(path_sales_onedrive/'meli_sbd_202601_lastweek.xlsx',dtype=str)
        print('save sku')
        df_sku.to_excel(path_meli_sku)
        print('save sales')
        df_2024.to_excel(path_meli_sales/'meli_sbd_202438_lastweek.xlsx',index=False)
        df_2025.to_excel(path_meli_sales/'meli_sbd_202501_lastweek.xlsx',index=False)
        df_2026.to_excel(path_meli_sales/'meli_sbd_202601_lastweek.xlsx',index=False)

        print(">>> Proceso finalizado exitosamente. ✅")
    except Exception as e:
        print(f"Error en procesamiento de datos de Ventas Datamind: {e}")
        sys.exit(1)
        

if __name__ == "__main__":
    
    main()