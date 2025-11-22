import pandas as pd
import adlfs
from azure.identity import ClientSecretCredential

# --- 1. CONFIGURACIÓN DE SEGURIDAD (OBTENIDA DEL PASO 2: SERVICE PRINCIPAL) ---
# 🚨 DEBES REEMPLAZAR ESTOS VALORES
TENANT_ID = "TU_TENANT_ID_DE_AZURE_AD" 
CLIENT_ID = "TU_CLIENT_ID_DEL_SERVICE_PRINCIPAL" 
CLIENT_SECRET = "TU_CLIENT_SECRET_O_CERTIFICADO" 

# --- 2. RUTA ONELAKE CONFIRMADA (Paso 1) ---
# Esta URL apunta a la carpeta de la entidad dentro de OneLake
ONELAKE_PATH = "abfss://GTS - Americas - PRD@onelake.dfs.fabric.microsoft.com/dataflows/RMA_Querys_Snowflake/VW_BRZ_DEMAND_HISTORY_FORECAST_TOOLS/"

try:
    # 3. Autenticación
    cred = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )

    # 4. Inicializar el sistema de archivos de Azure Data Lake (adlfs)
    # Se conecta al dominio de Fabric
    fs = adlfs.AzureBlobFileSystem(
        account_name='onelake.dfs.fabric.microsoft.com', 
        credential=cred
    )
    
    # 5. Leer los archivos Parquet de la entidad del Dataflow en Pandas
    # adlfs lee todos los archivos Parquet en el directorio y los concatena
    df = pd.read_parquet(
        ONELAKE_PATH,
        filesystem=fs
    )
    
    print(f"Dataframe cargado con éxito desde OneLake. Filas: {len(df)}")
    print("\nColumnas cargadas (incluyendo DATE_KEY):")
    print(df.info())
    print(df.head())

except Exception as e:
    print(f"Error: No se pudo establecer la conexión o leer los datos.")
    print(f"Verifique las credenciales (SPN) y los permisos de Lector en el workspace 'GTS - Americas - PRD'.")
    print(e)