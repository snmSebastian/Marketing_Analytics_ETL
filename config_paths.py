# config_paths_v3.py

from pathlib import Path
from dataclasses import dataclass

# =========================================================================
# 1. DEFINICIÓN DE LA RUTA BASE
# =========================================================================

BASE_PATH = Path(
    #r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics'
   # r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics'
    r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace'
    
    
)

# Definiciones de Directorios de Alto Nivel
RAW_DATA_DIR = BASE_PATH / 'Data' / 'Raw'
PROCESSED_DATAFLOW_DIR = BASE_PATH / 'Data' / 'Processed-Dataflow'

# =========================================================================
# 2. ESTRUCTURA CENTRALIZADA DE RUTAS (PATHS_CONFIG) - Diccionario
# (Claves basadas en el nombre del Archivo/Carpeta final)
# =========================================================================

PATHS_CONFIG = {
    # --- RUTAS USADAS EN VARIOS PROYECTOS ---
    "Shared": {
        # Archivo específico
        "Country_Codes": PROCESSED_DATAFLOW_DIR / 'Shared_Information_for_Projects' / 'Country' / 'Region_Country_codes.xlsx',
        "Gross_to_Net":PROCESSED_DATAFLOW_DIR / 'Shared_Information_for_Projects' / 'Gross_to_Net'/'Gross_to_Net.xlsx',
        "NPI":PROCESSED_DATAFLOW_DIR / 'Shared_Information_for_Projects' / 'NPI'/'NPI.xlsx',
        "fx_rate":PROCESSED_DATAFLOW_DIR / 'Shared_Information_for_Projects' / 'FX_Rate'/'FX_Rate.xlsx',
  
    },
    
    # --- PROCESO ETL: DEMAND ---
    "Demand": {
        "Raw": {
            "Historic": RAW_DATA_DIR / 'Demand' / 'Historic',
            "Mothly_Update": RAW_DATA_DIR / 'Demand' / 'Mothly_Update'
        },
        "Processed": {
            "OUTPUT_DIR_PROCESSED_PARQUETS": PROCESSED_DATAFLOW_DIR / 'Demand'
            }
    },
    
    # --- PROCESO ETL: FILL RATE ---
    "FillRate": {
        "Raw": {
            "Historic": RAW_DATA_DIR / 'Fill Rate' / 'Historic',
            "Mothly_Update": RAW_DATA_DIR / 'Fill Rate' / 'Mothly_Update'
        },
        "Processed": {
            "OUTPUT_DIR_PROCESSED_PARQUETS": PROCESSED_DATAFLOW_DIR / 'Fill_Rate'
        }
    },
    
    # --- PROCESO ETL: SALES ---
    "Sales": {
        "Raw": {
            "Historic": RAW_DATA_DIR / 'Sales' / 'Historic',
            "Mothly_Update": RAW_DATA_DIR / 'Sales' / 'Mothly_Update',            
            "Historic_Sharepoint": RAW_DATA_DIR / 'Sales' / 'Historic'/'SharePoint',
            "HistoricalChanges":RAW_DATA_DIR / 'Sales' / 'Historic'/'DataflowvsDatalake.xlsx'
        },
        "Processed": {
            "OUTPUT_DIR_PROCESSED_PARQUETS": PROCESSED_DATAFLOW_DIR / 'Sales'            
        }
    },

    # --- MASTER DATA: CUSTOMERS ---
    "Master_Customers": {
        "Raw": {
            "Customers_Shared_by_Country": RAW_DATA_DIR / 'Customers' / 'Customers_Shared_by_Country.xlsx',
            "Notation_Name_Customers": RAW_DATA_DIR / 'Customers' / 'Notation_Name_Customers.xlsx',
            "QueryCustomers": RAW_DATA_DIR / 'Customers' / 'QueryCustomers.xlsx',
            "Notation_subchannel_datalake_w_error": RAW_DATA_DIR / 'Customers' / 'Notation_subchannel_datalake_w_error.xlsx'
        
        },
        "Processed": {
            "Classification_Customers": PROCESSED_DATAFLOW_DIR / 'Master_Customers' / 'Classifications_Customers.xlsx',
            "Master_Customers": PROCESSED_DATAFLOW_DIR / 'Master_Customers' / 'Master_Customers.xlsx'
            
       }
    },
    
    # --- MASTER DATA: PRODUCTS ---
    "Master_Products": {
        "Raw": {
            "Sku_for_Review": RAW_DATA_DIR / 'Products' / 'Sku_for_Review.xlsx',
            "sku_shared_of_PSD": RAW_DATA_DIR / 'Products' /'other'/ 'sku_shared_of_PSD.xlsx',
            "Sku_for_Review": RAW_DATA_DIR / 'Products' / 'Sku_for_Review.xlsx',
            "hts_products": RAW_DATA_DIR / 'Products' / 'working_files_for_pwt_hts_products'/'HTS_Classification_Workfile.xlsx',
            "pwt_products": RAW_DATA_DIR / 'Products' / 'working_files_for_pwt_hts_products'/'PWT_Classification_Workfile.xlsx',
            "SkuName": RAW_DATA_DIR / 'Products' / 'other' /'QuerySkuName.parquet',
            "ConsultaSKU": RAW_DATA_DIR / 'Products' / 'other' /'ConsultaSKU.xlsx',
            "Result_ConsultaSku":RAW_DATA_DIR / 'Products' / 'other' /'ResultConsultSku.xlsx',
                
        },
        "Processed": {
            "GPP_Brand": PROCESSED_DATAFLOW_DIR / 'Master_Products' / 'GPP-Brand.xlsx',
            "Master_Product": PROCESSED_DATAFLOW_DIR / 'Master_Products' / 'Master_Product.xlsx',
            "Proyects": PROCESSED_DATAFLOW_DIR / 'Master_Products' / 'Proyects.xlsx'
            
        }
    },
    "Sodimac": {
        "Processed":{
            "OUTPUT_DIR_PROCESSED_PARQUETS_EDAS": PROCESSED_DATAFLOW_DIR / 'Sodimac' / 'EDAS'
           
        },
       },

    "Datamind":{
        "Raw":{
            "Week_update_Argentina":RAW_DATA_DIR/'Datamind'/'Update_Week_Datamind'/'Actualizacion Argentina Semanal.xlsx',
            "Week_update_Chile":RAW_DATA_DIR/'Datamind'/'Update_Week_Datamind'/'Actualizacion Chile Semanal.xlsx',
            "Week_update_Mexico":RAW_DATA_DIR/'Datamind'/'Update_Week_Datamind'/'Actualizacion Mexico Semanal.xlsx',
            "Precios_Coppel":RAW_DATA_DIR/'Datamind'/'Coppel'/'Precios_Coppel.xlsx',
            "Mercado_Libre_Sales":r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Mercado Libre\Analytics LAG ONE TEAM - Meli_No_Seller_Data_V2/meli_sbd_202601_lastweek.xlsx',
            "Mercado_Libre_Sku":r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Mercado Libre\Analytics LAG ONE TEAM - Meli_SKU_Clean/Meli_SKU_Clean_V2.xlsx',
            "EDAS":RAW_DATA_DIR/'Datamind'/'EDAS'/'QueryEDAS.parquet'
        },
        "Processed":{
            "OUTPUT_DIR_PROCESSED_PARQUETS_DATAMIND": PROCESSED_DATAFLOW_DIR / 'Datamind'

        }
    },

    "Campaigns": {
        "Processed": {
            "OUTPUT_FILE_PROCESSED_EXCEL_CAMPAIGNS": PROCESSED_DATAFLOW_DIR / 'Campaigns'/'Campaigns SBD.xlsx',
            "OUTPUT_FILE_PROCESSED_EXCEL_SOV": PROCESSED_DATAFLOW_DIR / 'Campaigns'/'SOV.xlsx',
            "OUTPUT_FILE_PROCESSED_EXCEL_ANALYSIS_CAMPAIGNS": PROCESSED_DATAFLOW_DIR / 'Campaigns'/'Analysis Campaigns.xlsx'
        }
    }
}

# =========================================================================
#  CONSTRUCCIÓN DE OBJETOS DE ACCESO RÁPIDO (DATACLASSES)
# Se reconstruyen usando las nuevas claves del PATHS_CONFIG.
# =========================================================================

# --- DEMAND ---
@dataclass(frozen=True)
class DemandPaths:
    #___________________
    # --- INPUTS -------
    #___________________
    """Rutas de acceso rápido para el proceso ETL Demand."""
    INPUT_RAW_HISTORIC_DIR: Path = PATHS_CONFIG['Demand']['Raw']['Historic']
    INPUT_RAW_UPDATE_DIR: Path = PATHS_CONFIG['Demand']['Raw']['Mothly_Update']
    INPUT_PROCESSED_COUNTRY_CODES_FILE: Path = PATHS_CONFIG['Shared']['Country_Codes']
    INPUT_PROCESSED_GROSS_TO_NET_FILE: Path = PATHS_CONFIG['Shared']['Gross_to_Net']
    INPUT_PROCESSED_NPI_FILE: Path = PATHS_CONFIG['Shared']['NPI']
    INPUT_PROCESSED_MASTER_PRODUCTS_FILE: Path = PATHS_CONFIG['Master_Products']['Processed']['Master_Product']
    INPUT_PROCESSED_FX_RATE_FILE: Path = PATHS_CONFIG['Shared']['fx_rate']

    #___________________
    # --- OUTPUTS ------
    #___________________
    OUTPUT_PROCESSED_PARQUETS_DIR: Path = PATHS_CONFIG['Demand']['Processed']['OUTPUT_DIR_PROCESSED_PARQUETS']
    
DEMAND_PATHS = DemandPaths()

# --- FILL RATE ---
@dataclass(frozen=True)
class FillRatePaths:
    #___________________
    # --- INPUTS -------
    #___________________
    """Rutas de acceso rápido para el proceso ETL Fill Rate."""
    INPUT_RAW_HISTORIC_DIR: Path = PATHS_CONFIG['FillRate']['Raw']['Historic']
    INPUT_RAW_UPDATE_DIR: Path = PATHS_CONFIG['FillRate']['Raw']['Mothly_Update']
    INPUT_PROCESSED_COUNTRY_CODES_FILE: Path = PATHS_CONFIG['Shared']['Country_Codes']
    #___________________
    # --- OUTPUTS ------
    #___________________
    OUTPUT_PROCESSED_PARQUETS_DIR: Path = PATHS_CONFIG['FillRate']['Processed']['OUTPUT_DIR_PROCESSED_PARQUETS']
    
FILLRATE_PATHS = FillRatePaths()

# --- SALES ---
@dataclass(frozen=True)
class SalesPaths:
    #___________________
    # --- INPUTS -------
    #___________________
    """Rutas de acceso rápido para el proceso ETL Sales."""
    INPUT_RAW_HISTORIC_DIR: Path = PATHS_CONFIG['Sales']['Raw']['Historic']
    INPUT_RAW_UPDATE_DIR: Path = PATHS_CONFIG['Sales']['Raw']['Mothly_Update']
    INPUT_PROCESSED_COUNTRY_CODES_FILE: Path = PATHS_CONFIG['Shared']['Country_Codes']
    INPUT_PROCESSED_GROSS_TO_NET_FILE: Path = PATHS_CONFIG['Shared']['Gross_to_Net']
    INPUT_PROCESSED_NPI_FILE: Path = PATHS_CONFIG['Shared']['NPI']
    INPUT_PROCESSED_MASTER_PRODUCTS_FILE: Path = PATHS_CONFIG['Master_Products']['Processed']['Master_Product']
    INPUT_RAW_HISTORICAL_CHANGES_FILE: Path = PATHS_CONFIG['Sales']['Raw']['HistoricalChanges']


    #___________________
    # --- OUTPUTS -------
    #___________________
    OUTPUT_PROCESSED_PARQUETS_DIR: Path = PATHS_CONFIG['Sales']['Processed']['OUTPUT_DIR_PROCESSED_PARQUETS']
    
SALES_PATHS = SalesPaths()


# --- MASTER CUSTOMERS ---
@dataclass(frozen=True) 
class MasterCustomersPaths:
    #___________________
    # --- INPUTS -------
    #___________________
    """Rutas de acceso rápido para el proceso Master Customers."""
    INPUT_RAW_UPDATE_FILL_RATE_DIR: Path = PATHS_CONFIG['FillRate']['Raw']['Mothly_Update']
    INPUT_RAW_UPDATE_SALES_DIR: Path = PATHS_CONFIG['Sales']['Raw']['Mothly_Update']
    INPUT_RAW_UPDATE_SALES_DIR_sharepoint: Path = PATHS_CONFIG['Sales']['Raw']['Historic_Sharepoint']
    
    INPUT_RAW_Customers_Shared_by_Country_FILE: Path = PATHS_CONFIG['Master_Customers']['Raw']['Customers_Shared_by_Country']
    INPUT_RAW_NOTATION_NAMES_FILE: Path = PATHS_CONFIG['Master_Customers']['Raw']['Notation_Name_Customers']
    INPUT_RAW_QUERY_CUSTOMERS_FILE: Path = PATHS_CONFIG['Master_Customers']['Raw']['QueryCustomers']
    INPUT_RAW_NOTATION_SUBCHANNEL_DATALAKE_W_ERROR_FILE: Path = PATHS_CONFIG['Master_Customers']['Raw']['Notation_subchannel_datalake_w_error']

    
    INPUT_PROCESSED_COUNTRY_CODES_FILE: Path = PATHS_CONFIG['Shared']['Country_Codes']
    #___________________
    # --- OUTPUTS -------
    #___________________
    OUTPUT_FILE_PROCESSED_MASTER_CUSTOMERS_FILE: Path = PATHS_CONFIG['Master_Customers']['Processed']['Master_Customers']
    
MASTER_CUSTOMERS_PATHS = MasterCustomersPaths()

# --- MASTER PRODUCTS ---
@dataclass(frozen=True)
class MasterProductsPaths:
    #___________________
    # --- INPUTS -------
    #___________________
    """Rutas de acceso rápido para el proceso Master Products."""
    OUTPUT_PROCESSED_PARQUETS_DIR: Path = PATHS_CONFIG['Demand']['Processed']['OUTPUT_DIR_PROCESSED_PARQUETS']
    INPUT_RAW_UPDATE_FILL_RATE_DIR: Path = PATHS_CONFIG['FillRate']['Raw']['Mothly_Update']
    INPUT_RAW_UPDATE_SALES_DIR: Path = PATHS_CONFIG['Sales']['Raw']['Mothly_Update'] 
    INPUT_RAW_UPDATE_DEMAND_DIR: Path =  PATHS_CONFIG['Demand']['Raw']['Mothly_Update']

    INPUT_RAW_SHARED_PSD_FILE: Path = PATHS_CONFIG['Master_Products']['Raw']['sku_shared_of_PSD']
    INPUT_RAW_SkuName_FILE: Path = PATHS_CONFIG['Master_Products']['Raw']['SkuName']
    INPUT_RAW_ConsultaSKU_FILE: Path = PATHS_CONFIG['Master_Products']['Raw']['ConsultaSKU']
    INPUT_RAW_Result_ConsultaSKU_FILE: Path = PATHS_CONFIG['Master_Products']['Raw']['Result_ConsultaSku']
    
    INPUT_PROCESSED_GPP_BRAND_FILE: Path = PATHS_CONFIG['Master_Products']['Processed']['GPP_Brand']
    INPUT_PROCESSED_PROYECTS_FILE: Path = PATHS_CONFIG['Master_Products']['Processed']['Proyects']

    #___________________
    # --- OUTPUTS -------
    #___________________
    WORKFILE_NEW_PRODUCTS_REVIEW_FILE: Path = PATHS_CONFIG['Master_Products']['Raw']['Sku_for_Review']
    
    WORKFILE_HTS_FILE:Path=PATHS_CONFIG['Master_Products']['Raw']['hts_products']
    WORKFILE_PWT_FILE:Path=PATHS_CONFIG['Master_Products']['Raw']['pwt_products']

    OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE: Path = PATHS_CONFIG['Master_Products']['Processed']['Master_Product']
    
    
MASTER_PRODUCTS_PATHS = MasterProductsPaths()


class SodimacPaths:

    #___________________
    # --- OUTPUTS ------
    #___________________
    OUTPUT_PROCESSED_PARQUETS_DIR_EDAS: Path = PATHS_CONFIG['Sodimac']['Processed']['OUTPUT_DIR_PROCESSED_PARQUETS_EDAS']


Sodimav_PATHS = SodimacPaths()


class DatamindPaths:
    #____________________
    #--- INPUTS ---------
    #____________________
    INPUT_RAW_ARGENTINA_FILE: Path = PATHS_CONFIG['Datamind']['Raw']['Week_update_Argentina']
    INPUT_RAW_MEXICO_FILE: Path = PATHS_CONFIG['Datamind']['Raw']['Week_update_Mexico']
    INPUT_RAW_CHILE_FILE: Path = PATHS_CONFIG['Datamind']['Raw']['Week_update_Chile']
    INPUT_RAW_COPPEL_FILE: Path = PATHS_CONFIG['Datamind']['Raw']['Precios_Coppel']
    INPUT_RAW_MELI_SALES_FILE: Path = PATHS_CONFIG['Datamind']['Raw']['Mercado_Libre_Sales']
    INPUT_RAW_MELI_SKU_FILE: Path = PATHS_CONFIG['Datamind']['Raw']['Mercado_Libre_Sku']
    INPUT_PROCESSED_GROSS_TO_NET_FILE: Path = PATHS_CONFIG['Shared']['Gross_to_Net']
    INPUT_RAW_SkuName_FILE: Path = PATHS_CONFIG['Master_Products']['Raw']['SkuName']
    INPUT_RAW_EDAS_FILE: Path = PATHS_CONFIG['Datamind']['Raw']['EDAS']

    #_________________
    #--- OUTPUTS -------
    #__________________
    OUTPUT_PROCESSED_PARQUETS_DIR: Path = PATHS_CONFIG['Datamind']['Processed']['OUTPUT_DIR_PROCESSED_PARQUETS_DATAMIND']

Datamind_PATHS = DatamindPaths()

class CampaignsPaths:
    #_________________
    #--- OUTPUTS --
    #________________
    OUTPUT_FILE_PROCESSED_EXCEL_CAMPAIGNS: Path = PATHS_CONFIG['Campaigns']['Processed']['OUTPUT_FILE_PROCESSED_EXCEL_CAMPAIGNS']
    OUTPUT_FILE_PROCESSED_EXCEL_SOV: Path = PATHS_CONFIG['Campaigns']['Processed']['OUTPUT_FILE_PROCESSED_EXCEL_SOV']
    OUTPUT_FILE_PROCESSED_EXCEL_ANALYSIS_CAMPAIGNS: Path = PATHS_CONFIG['Campaigns']['Processed']['OUTPUT_FILE_PROCESSED_EXCEL_ANALYSIS_CAMPAIGNS']

Campaigns_PATHS = CampaignsPaths()