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
        WHERE FINAL_GPP_DESC_SYS IN ('SAPC11','SAPE03','QADAR','SAPBYD','QADCH','QADPE','QADBR')


    """

    df_queryDemand=query(conexion,sql)
    df_queryDemand.to_parquet(SkuName_update_raw_dir, index=False)
    #print(df_queryDemand.head())
    print("--- 🔄 PROCESO FINALIZADO: MASTER PRODUCTS DATA EXTRACTION ---")


if __name__ == "__main__":
    main()
    