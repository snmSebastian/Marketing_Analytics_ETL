# config_paths_v3.py

from pathlib import Path
from dataclasses import dataclass

# =========================================================================
# 1. DEFINICIÓN DE LA RUTA BASE
# =========================================================================

BASE_PATH = Path(
    r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics'
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
        "Country_Codes": PROCESSED_DATAFLOW_DIR / 'Shared_Information_for_Projects' / 'Country' / 'Region_Country_codes.xlsx'
    },
    
    # --- PROCESO ETL: DEMAND ---
    "Demand": {
        "Raw": {
            "Historic": RAW_DATA_DIR / 'Demand' / 'Historic',
            "Mothly_Update": RAW_DATA_DIR / 'Demand' / 'Mothly_Update'
        },
        "Processed": {
            "OUTPUT_DIR_PROCESSED_PARQUETS": PROCESSED_DATAFLOW_DIR / 'Demand',
            "OUTPUT_DIR_PROCESSED_PARQUETS_Prueba": PROCESSED_DATAFLOW_DIR / 'Demand' / 'prueba'
            }
    },
    
    # --- PROCESO ETL: FILL RATE ---
    "FillRate": {
        "Raw": {
            "Historic": RAW_DATA_DIR / 'Fill Rate' / 'Historic',
            "Mothly_Update": RAW_DATA_DIR / 'Fill Rate' / 'Mothly_Update'
        },
        "Processed": {
            "OUTPUT_DIR_PROCESSED_PARQUETS": PROCESSED_DATAFLOW_DIR / 'Fill_Rate',
            "OUTPUT_DIR_PROCESSED_PARQUETS_Prueba": PROCESSED_DATAFLOW_DIR / 'Fill_Rate' / 'prueba'
        }
    },
    
    # --- PROCESO ETL: SALES ---
    "Sales": {
        "Raw": {
            "Historic": RAW_DATA_DIR / 'Sales' / 'Historic',
            "Mothly_Update": RAW_DATA_DIR / 'Sales' / 'Mothly_Update'
        },
        "Processed": {
            "OUTPUT_DIR_PROCESSED_PARQUETS": PROCESSED_DATAFLOW_DIR / 'Sales',
            "OUTPUT_DIR_PROCESSED_PARQUETS_Prueba": PROCESSED_DATAFLOW_DIR / 'Sales' / 'prueba'
            
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
            "Master_Customers": PROCESSED_DATAFLOW_DIR / 'Master_Customers' / 'Master_Customers.xlsx',
            "Master_Customers_Prueba": PROCESSED_DATAFLOW_DIR / 'Master_Customers' / 'Master_Customers_Prueba.xlsx'
        }
    },
    
    # --- MASTER DATA: PRODUCTS ---
    "Master_Products": {
        "Raw": {
            "Sku_for_Review": RAW_DATA_DIR / 'Products' / 'Sku_for_Review.xlsx',
            "sku_shared_of_PSD": RAW_DATA_DIR / 'Products' / 'sku_shared_of_PSD.xlsx',
            "Sku_for_Review": RAW_DATA_DIR / 'Products' / 'Sku_for_Review.xlsx',
            "hts_products": RAW_DATA_DIR / 'Products' / 'working_files_for_pwt_hts_products'/'HTS_Classification_Workfile.xlsx',
            "pwt_products": RAW_DATA_DIR / 'Products' / 'working_files_for_pwt_hts_products'/'PWT_Classification_Workfile.xlsx'
                
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
    #___________________
    # --- OUTPUTS ------
    #___________________
    OUTPUT_PROCESSED_PARQUETS_DIR: Path = PATHS_CONFIG['Demand']['Processed']['OUTPUT_DIR_PROCESSED_PARQUETS']
    OUTPUT_PROCESSED_PARQUETS_DIR_PRUEBA: Path = PATHS_CONFIG['Demand']['Processed']['OUTPUT_DIR_PROCESSED_PARQUETS_Prueba']
    
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
    OUTPUT_PROCESSED_PARQUETS_DIR_PRUEBA: Path = PATHS_CONFIG['FillRate']['Processed']['OUTPUT_DIR_PROCESSED_PARQUETS_Prueba']
    
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
    #___________________
    # --- OUTPUTS -------
    #___________________
    OUTPUT_PROCESSED_PARQUETS_DIR: Path = PATHS_CONFIG['Sales']['Processed']['OUTPUT_DIR_PROCESSED_PARQUETS']
    OUTPUT_PROCESSED_PARQUETS_DIR_PRUEBA: Path = PATHS_CONFIG['Sales']['Processed']['OUTPUT_DIR_PROCESSED_PARQUETS_Prueba']
    
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
    OUTPUT_FILE_PROCESSED_MASTER_CUSTOMERS_FILE_PRUEBA: Path = PATHS_CONFIG['Master_Customers']['Processed']['Master_Customers_Prueba']
    
MASTER_CUSTOMERS_PATHS = MasterCustomersPaths()

# --- MASTER PRODUCTS ---
@dataclass(frozen=True)
class MasterProductsPaths:
    #___________________
    # --- INPUTS -------
    #___________________
    """Rutas de acceso rápido para el proceso Master Products."""
    INPUT_RAW_UPDATE_DEMAND_DIR: Path = PATHS_CONFIG['Demand']['Raw']['Mothly_Update']
    INPUT_RAW_UPDATE_FILL_RATE_DIR: Path = PATHS_CONFIG['FillRate']['Raw']['Mothly_Update']
    INPUT_RAW_UPDATE_SALES_DIR: Path = PATHS_CONFIG['Sales']['Raw']['Mothly_Update'] 
    INPUT_RAW_SHARED_PSD_FILE: Path = PATHS_CONFIG['Master_Products']['Raw']['sku_shared_of_PSD']
    INPUT_PROCESSED_GPP_BRAND_FILE: Path = PATHS_CONFIG['Master_Products']['Processed']['GPP_Brand']
    #___________________
    # --- OUTPUTS -------
    #___________________
    WORKFILE_NEW_PRODUCTS_REVIEW_FILE: Path = PATHS_CONFIG['Master_Products']['Raw']['Sku_for_Review']
    
    WORKFILE_HTS_FILE:Path=PATHS_CONFIG['Master_Products']['Raw']['hts_products']
    WORKFILE_PWT_FILE:Path=PATHS_CONFIG['Master_Products']['Raw']['pwt_products']

    OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE: Path = PATHS_CONFIG['Master_Products']['Processed']['Master_Product']
    OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE_PRUEBA: Path = PATHS_CONFIG['Master_Products']['Processed']['Master_Product_Prueba']
    
    
MASTER_PRODUCTS_PATHS = MasterProductsPaths()