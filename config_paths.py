# config_paths.py
from pathlib import Path

# =========================================================================
# 1. DEFINICIÓN DE LA RUTA BASE
#
# Se asume que todas las rutas tienen un prefijo común (la carpeta de OneDrive).
# Esto facilita el cambio de entorno (ej. de su máquina a un servidor o Docker)
# cambiando solo esta línea o inyectándola vía Variable de Entorno.
#
# Base: C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\
# =========================================================================

# NOTA: Aunque se usan Path, las rutas absolutas de Windows se pasan como strings.
# Path(r'...') las convierte internamente a un objeto Path robusto.

BASE_PATH = Path(
    r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics'
)

# =========================================================================
# 2. RUTAS COMPARTIDAS (SHARED)
# Información usada por múltiples procesos ETL o Masters.
# =========================================================================

# Directorio base para datos procesados y compartidos
PROCESSED_DATAFLOW_DIR = BASE_PATH / 'Data' / 'Processed-Dataflow'
RAW_DATA_DIR = BASE_PATH / 'Data' / 'Raw'

# Información de Códigos de País/Región
SHARED_COUNTRY_CODES_PATH = PROCESSED_DATAFLOW_DIR / 'Shared_Information_for_Projects' / 'Country' / 'Region_Country_codes.xlsx'

# =========================================================================
# 3. RUTAS DEL PROCESO ETL - DEMAND
# =========================================================================

# Directorios
DEMAND_RAW_DIR = RAW_DATA_DIR / 'Demand'
DEMAND_PROCESSED_DIR = PROCESSED_DATAFLOW_DIR / 'Demand'

# Demand - process_files (Históricos)
DEMAND_INPUT_HISTORIC_PATH = DEMAND_RAW_DIR / 'Historic'
DEMAND_OUTPUT_PROCESSED_PATH = DEMAND_PROCESSED_DIR

# Demand - update
DEMAND_INPUT_MONTHLY_UPDATE_PATH = DEMAND_RAW_DIR / 'Mothly_Update'
DEMAND_PROCESSED_PARQUETS_HISTORICS = DEMAND_PROCESSED_DIR / 'prueba' # Ajustar nombre si 'prueba' es temporal

# =========================================================================
# 4. RUTAS DEL PROCESO ETL - FILL RATE
# =========================================================================

# Directorios
FILLRATE_RAW_DIR = RAW_DATA_DIR / 'Fill Rate'
FILLRATE_PROCESSED_DIR = PROCESSED_DATAFLOW_DIR / 'Fill_Rate'

# Fill Rate - process_files (Históricos)
FILLRATE_INPUT_HISTORIC_PATH = FILLRATE_RAW_DIR / 'Historic'
FILLRATE_OUTPUT_PROCESSED_PATH = FILLRATE_PROCESSED_DIR

# Fill Rate - update
FILLRATE_INPUT_MONTHLY_UPDATE_PATH = FILLRATE_RAW_DIR / 'Mothly_Update'
FILLRATE_PROCESSED_PARQUETS_HISTORICS = FILLRATE_PROCESSED_DIR / 'prueba' # Ajustar nombre si 'prueba' es temporal
FILLRATE_OUTPUT_UPDATE_PATH = FILLRATE_PROCESSED_DIR / 'prueba'

# =========================================================================
# 5. RUTAS DEL PROCESO ETL - SALES
# =========================================================================

# Directorios
SALES_RAW_DIR = RAW_DATA_DIR / 'Sales'
SALES_PROCESSED_DIR = PROCESSED_DATAFLOW_DIR / 'Sales'

# Sales - process_files (Históricos)
SALES_INPUT_HISTORIC_PATH = SALES_RAW_DIR / 'Historic'
SALES_OUTPUT_PROCESSED_PATH = SALES_PROCESSED_DIR

# Sales - update
SALES_INPUT_MONTHLY_UPDATE_PATH = SALES_RAW_DIR / 'Mothly_Update'
SALES_PROCESSED_PARQUETS_HISTORICS = SALES_PROCESSED_DIR / 'prueba' # Ajustar nombre si 'prueba' es temporal

# =========================================================================
# 6. RUTAS DE PROCESOS MASTER DATA (Clientes y Productos)
# =========================================================================

# Master Customers - update
MC_RAW_CUSTOMER_CLASSIFICATIONS = RAW_DATA_DIR / 'Customers' / 'Clasifications_Customers.xlsx'
MC_RAW_CUSTOMER_NOTATION = RAW_DATA_DIR / 'Customers' / 'Notation_Name_Customers.xlsx'

MC_PROCESSED_MASTER_CUSTOMERS = PROCESSED_DATAFLOW_DIR / 'Master_Customers' / 'Master_Customers.xlsx'
MC_PROCESSED_PRUEBA = PROCESSED_DATAFLOW_DIR / 'Master_Customers' / 'prueba.xlsx'

# Master Products - update
MP_PROCESSED_MASTER_PRODUCTS = PROCESSED_DATAFLOW_DIR / 'Master_Products' / 'Master_Product-prueba.xlsx'
MP_PROCESSED_GPP_BRAND = PROCESSED_DATAFLOW_DIR / 'Master_Products' / 'GPP-Brand.xlsx'
MP_RAW_NEW_PRODUCTS_REVIEW = RAW_DATA_DIR / 'Products' / 'Sku_for_Review_prueba.xlsx'
MP_RAW_SHARED_PSD = RAW_DATA_DIR / 'Products' / 'sku_shared_of_PSD.xlsx'

# Rutas de datos RAW para buscar nuevos SKUs (usadas en ambos masters)
MP_DEMAND_MONTHLY_UPDATE = DEMAND_INPUT_MONTHLY_UPDATE_PATH
MP_FILLRATE_MONTHLY_UPDATE = FILLRATE_INPUT_MONTHLY_UPDATE_PATH
MP_SALES_MONTHLY_UPDATE = SALES_INPUT_MONTHLY_UPDATE_PATH