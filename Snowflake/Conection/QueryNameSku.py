"""
Módulo de orquestación para la extracción de datos de DIM PRODUCT desde Snowflake.
Utiliza los componentes de conexión (Conection) para realizar consultas 
al Data Warehouse y obtener el histórico de Forecast actualizado.
"""

import snowflake.connector

from .Conection import conectar_snowflake_sso, query
    
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
    sql="""
        SELECT 
            prod_id_hrmz as SKU,
            prod_name AS "SKU Description",

            final_brand as Brand,

            final_gpp AS "GPP Code",

            gpp_sbu_id AS "SBU Code",
            gpp_sbu_desc AS "SBU Description",

            gpp_division_id as "Division Code",
            gpp_division_desc as "Division Description",

            gpp_category_id as "Category Code",
            gpp_category_desc as "Category  Description",

            gpp_portfolio_id as "Portafolio Code",
            gpp_portfolio_desc as "Portafolio Description",

            FINAL_GPP_DESC_SYS as "System"

        FROM PROD_EDW.DIMENSIONS.DIM_PRODUCT
        WHERE FINAL_GPP_DESC_SYS IN ('SAPC11','SAPE03','QADAR','SAPBYD','QADCH','QADPE','QADBR')
    """

    df_queryDemand=query(conexion,sql)
    df_queryDemand.to_parquet(SkuName_update_raw_dir, index=False)
    #print(df_queryDemand.head())
    print("--- 🔄 PROCESO FINALIZADO: MASTER PRODUCTS DATA EXTRACTION ---")


if __name__ == "__main__":
    main()
    