"""
EL DNI DE PRODUCTOS: EXTRACTOR MAESTRO DE SKUS
---------------------------------------------
Este script es el "fotógrafo oficial" de nuestros productos en Snowflake. Su chamba 
es traernos la información más actualizada y limpia de cada SKU, incluyendo su marca, 
cómo se clasifica (GPP) y si usa cable o batería. Es la fuente de verdad para que 
todos los demás procesos (ventas, demanda, maestro de productos) hablen el mismo 
idioma sobre qué es cada producto. Sin esto, la clasificación de productos sería 
un desastre.

FLUJO DE TRABAJO:
1. Abre el portal a Snowflake: Se conecta al Data Warehouse para acceder a la tabla 
   maestra de productos (`DIM_FINAL_PRODUCT`).
2. Pide el "Acta de Nacimiento": Ejecuta una consulta SQL para extraer todos los 
   detalles relevantes de cada SKU (descripción, marca, jerarquía GPP, tipo de energía).
3. Filtro de Nacionalidad: Solo trae los productos de los sistemas ERP que nos 
   interesan (SAPC11, SAPE03, QADAR, etc.), dejando fuera la basura.
4. Guarda la foto en alta resolución: Almacena la data en un archivo Parquet, 
   listo para ser consumido por el "Cerebro de Productos" y otros módulos.

💡 NOTA DE SENIOR:
¡Ojo con el SQL! La lista de `FINAL_GPP_DESC_SYS` es un filtro crítico. Si el 
negocio decide incluir productos de un nuevo sistema ERP, hay que actualizar ese 
`IN (...)` en el query. Si no, esos SKUs simplemente no aparecerán en ningún reporte. 
Este script es una LIBRERÍA CORE para la consistencia de datos; cualquier cambio 
aquí impacta directamente la definición de producto en todo el pipeline.
"""

import snowflake.connector

from .Conection import conectar_snowflake_sso, query, conectar_snowflake_password
from Fill_Rate.Process_ETL.Process_Files import clean_sku
    
def main():
    """
    Orquesta el flujo de extracción de datos de Demanda.
    El proceso incluye:
     1) Conexión a Snowflake vía SSO.
     2) Ejecución de query con filtros de periodo fiscal actual.
     3) Carga de resultados en DataFrame.
    Returns: None: La función orquesta el proceso y no devuelve un valor.
    """
    from config_paths import MasterProductsPaths
    SkuName_update_raw_dir = MasterProductsPaths.INPUT_RAW_SkuName_FILE
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: MASTER PRODUCTS DATA EXTRACTION ---")
    print("=" * 55)
    conexion=conectar_snowflake_sso(Database="PROD_EDW",Schema="DIMENSIONS")
    #conexion=conectar_snowflake_password(Database="PROD_EDW",Schema="DIMENSIONS")
    sql="""
              SELECT 
            prod_id_hrmz as SKU,
            prod_desc_hrmz  AS "SKU Description",

            final_brand as "Brand",

            final_gpp AS "GPP Code",

            final_sbu_id AS "GPP SBU",
            final_sbu_desc AS "GPP SBU Description",

            final_gpp_division_id as "GPP Division Code",
            final_gpp_division_desc as "GPP Division Description",

            final_gpp_category_id as "GPP Category Code",
            final_gpp_category_desc as "GPP Category Description",

            final_gpp_portfolio_id as "GPP Portfolio Code",
            final_gpp_portfolio_desc as "GPP Portfolio Description",

            FINAL_POWER_SOURCE as "Corded / Cordless",

            FINAL_GPP_DESC_SYS as "System"

        FROM PROD_EDW.DIMENSIONS.DIM_FINAL_PRODUCT
       

    """
   # WHERE FINAL_GPP_DESC_SYS IN ('SAPC11','SAPE03','QADAR','SAPBYD','QADCH','QADPE','QADBR')

    df_queryNameSku=query(conexion,sql)
    df_queryNameSku=clean_sku(df_queryNameSku,'SKU')
    df_queryNameSku.to_parquet(SkuName_update_raw_dir, index=False)
    #print(df_queryDemand.head())
    print("--- 🔄 PROCESO FINALIZADO: MASTER PRODUCTS DATA EXTRACTION ---")


if __name__ == "__main__":
    main()
    