# config_paths_v3.py

from pathlib import Path
from dataclasses import dataclass

# =========================================================================
# 1. DEFINICIÓN DE LA RUTA BASE
# =========================================================================

BASE_PATH = Path(
    #r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics'
    r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics'
    
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
        "Table_for_filter_NPI":PROCESSED_DATAFLOW_DIR / 'Shared_Information_for_Projects' / 'NPI'/'Table_for_filter_NPI.xlsx'
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
            "Mothly_Update": RAW_DATA_DIR / 'Sales' / 'Mothly_Update'            
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
            "SkuName": RAW_DATA_DIR / 'Products' / 'other' /'QuerySkuName.parquet'
                
        },
        "Processed": {
            "GPP_Brand": PROCESSED_DATAFLOW_DIR / 'Master_Products' / 'GPP-Brand.xlsx',
            "Master_Product": PROCESSED_DATAFLOW_DIR / 'Master_Products' / 'Master_Product.xlsx',
            "Master_Product_Prueba": PROCESSED_DATAFLOW_DIR / 'Master_Products' / 'Master_Product_Prueba.xlsx',
            
        }
    },
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
    INPUT_PROCESSED_FILTER_NPI_FILE: Path = PATHS_CONFIG['Shared']['Table_for_filter_NPI']
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
    INPUT_PROCESSED_FILTER_NPI_FILE: Path = PATHS_CONFIG['Shared']['Table_for_filter_NPI']
    INPUT_PROCESSED_MASTER_PRODUCTS_FILE: Path = PATHS_CONFIG['Master_Products']['Processed']['Master_Product']

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
    INPUT_RAW_Customers_Shared_by_Country_FILE: Path = PATHS_CONFIG['Master_Customers']['Raw']['Customers_Shared_by_Country']
    INPUT_RAW_NOTATION_NAMES_FILE: Path = PATHS_CONFIG['Master_Customers']['Raw']['Notation_Name_Customers']
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
    INPUT_RAW_SHARED_PSD_FILE: Path = PATHS_CONFIG['Master_Products']['Raw']['sku_shared_of_PSD']
    INPUT_RAW_SkuName_FILE: Path = PATHS_CONFIG['Master_Products']['Raw']['SkuName']
    
    INPUT_PROCESSED_GPP_BRAND_FILE: Path = PATHS_CONFIG['Master_Products']['Processed']['GPP_Brand']
    #___________________
    # --- OUTPUTS -------
    #___________________
    WORKFILE_NEW_PRODUCTS_REVIEW_FILE: Path = PATHS_CONFIG['Master_Products']['Raw']['Sku_for_Review']
    
    WORKFILE_HTS_FILE:Path=PATHS_CONFIG['Master_Products']['Raw']['hts_products']
    WORKFILE_PWT_FILE:Path=PATHS_CONFIG['Master_Products']['Raw']['pwt_products']

    OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE: Path = PATHS_CONFIG['Master_Products']['Processed']['Master_Product']
    
    
MASTER_PRODUCTS_PATHS = MasterProductsPaths()