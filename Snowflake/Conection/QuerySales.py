"""
Módulo de orquestación para la extracción de datos de VENTAS desde Snowflake.
Utiliza los componentes de conexión (Conection) para realizar consultas 
al Data Warehouse y obtener el histórico de facturación.
"""

import snowflake.connector

from .Conection import conectar_snowflake_sso, query, conectar_snowflake_password
from Fill_Rate.Process_ETL.Process_Files import clean_sku
    
def main():
    """
    Orquesta el flujo de extracción de datos de Ventas.
    El proceso incluye:
     1) Conexión a Snowflake vía Password (Service Account) o SSO.
     2) Ejecución de query de facturación histórica.
     3) Carga de resultados en DataFrame.
    Returns: None: La función orquesta el proceso y no devuelve un valor.
    """
    from config_paths import SalesPaths      
    sales_update_raw_dir = SalesPaths.INPUT_RAW_UPDATE_DIR
    print("-" * 55)
    print("--- INICIANDO PROCESO: SALES DATA EXTRACTION ---")
    print("-" * 55)
    conexion=conectar_snowflake_sso(Database="PROD_MARTS",Schema="LAGBI")
    #conexion=conectar_snowflake_password(Database="PROD_MARTS",Schema="LAGBI")
    sql="""
        WITH base AS (
            SELECT 
                SRC_SYS_KEY,
                DOCUMENT_TYPE,
                FMTH_ID_FINANCIAL,
                FWK_ID,
                ENTITY_LEVE3_LNDESC,
                ENTITY_LEVE4_LNDESC,
                ENTITY_LEVE6_LNDESC,
                ENTITY_LEVE7_LNDESC,
                ENTITY_LEVE8_LNDESC,
                ENTITY_LEVE9_LNDESC,
                SOLDTO_CUST_KEY,
                SOLDTO_CUST_NAME,
                PROD_ID_HRMZ,
                PROD_ID,
                PROD_KEY,
                PROD_NAME,
                BRAND_DESC,
                GPP_DIVISION_DESC,
                GPP_DIVISION_ID,
                GPP_CATEGORY_DESC,
                GPP_CATEGORY_ID,
                GPP_PORTFOLIO_DESC,
                GPP_PORTFOLIO_ID,
                DISPATCH_BCRNCY_AMT_PLRATE,
                COST_DCRNCY_AMT_USD_PLRATE,
                DISPATCH_INVC_QTY,
                RETURN_QTY,
                NSV_BCRNCY_AMT_PLRATE,
                FX_NSV_BCRNCY_AMT_PLRATE,
                FX_NSV_FINANCIAL_PLRATE,
                OUTBOUND_BCRNCY_USD_PLRATE,
                DISPATCH_BCRNCY_AMT,
                CASE
                    WHEN TRIM(ENTITY_LEVE5_LNDESC) = 'Trinidad And Tobago' THEN 'Trinidad y Tobago'
                    WHEN TRIM(ENTITY_LEVE5_LNDESC) IN ('Dom Rep', 'Republica Dominicana') THEN 'Dominican Republic'
                    ELSE ENTITY_LEVE5_LNDESC
                END AS country_normalized
            FROM PROD_MARTS.LAGBI.VW_BRZ_SALES_BILLING_LAGBI
        )

        SELECT 
            SRC_SYS_KEY AS "Source System",
            DOCUMENT_TYPE AS "Document Type",

            CONCAT(RIGHT(FMTH_ID_FINANCIAL, 2), '/01/', LEFT(FMTH_ID_FINANCIAL, 4)) AS "fk_Date", 
            CONCAT(LEFT(FMTH_ID_FINANCIAL, 4), '-', RIGHT(FMTH_ID_FINANCIAL, 2)) AS "fk_year_month",
            
            FWK_ID AS "Week",

            ENTITY_LEVE3_LNDESC AS "Region Cluster",
            ENTITY_LEVE4_LNDESC AS "Region Consolidated",

            CASE 
                WHEN UPPER(ENTITY_LEVE4_LNDESC) = 'MTD' THEN CONCAT(country_normalized, ' MTD')
                ELSE country_normalized
            END AS "fk_Country",

            ENTITY_LEVE6_LNDESC AS "Country Detail",
            ENTITY_LEVE7_LNDESC AS "Sales Type",
            ENTITY_LEVE8_LNDESC AS "Sales Type Detail",
            ENTITY_LEVE9_LNDESC AS "Sales Type Invoince Country",

            SOLDTO_CUST_KEY AS "fk_Sold_To_Customer_Code",
            SOLDTO_CUST_NAME AS "Customer Name",
            
            COALESCE(PROD_ID_HRMZ, PROD_ID, PROD_KEY) AS "fk_SKU",
            PROD_ID,
            PROD_KEY,
            PROD_NAME AS "SKU Description",
            BRAND_DESC AS "Brand",

            GPP_DIVISION_DESC AS "GPP Division",
            GPP_DIVISION_ID AS "GPP Division Code",
            GPP_CATEGORY_DESC AS "GPP Category",
            GPP_CATEGORY_ID AS "GPP Category Code",
            GPP_PORTFOLIO_DESC AS "GPP Portafolio",
            GPP_PORTFOLIO_ID AS "GPP Portafolio Code",

            DISPATCH_BCRNCY_AMT_PLRATE AS "Total Sales",
            COST_DCRNCY_AMT_USD_PLRATE AS "Total Cost",
            DISPATCH_INVC_QTY AS "Units Sold",
            RETURN_QTY AS "Units Return",
            NSV_BCRNCY_AMT_PLRATE AS "NSV",
            FX_NSV_BCRNCY_AMT_PLRATE AS "FX Rate NSV",
            FX_NSV_FINANCIAL_PLRATE AS "fx_nsv_financial",
            OUTBOUND_BCRNCY_USD_PLRATE AS "Outbound",
            DISPATCH_BCRNCY_AMT AS "GSV Local"
        FROM base
        """
#        WHERE FMTH_ID_FINANCIAL >= TO_CHAR(ADD_MONTHS(CURRENT_DATE(), -8), 'YYYYMM');
    df_querySales=query(conexion,sql)
    
    if df_querySales is None:
        print("Error: No se pudieron obtener datos de ventas de Snowflake. Terminando el proceso.")
        return

    df_querySales = df_querySales.drop(columns=['PROD_ID','PROD_KEY'])
    df_querySales=clean_sku(df_querySales,'fk_SKU')
    
    df_querySales.to_parquet(sales_update_raw_dir /'QuerySales.parquet', index=False)
    #print(df_queryDemand.head())
    print("---PROCESO FINALIZADO: SALES DATA EXTRACTION ---")


if __name__ == "__main__":
    main()
    
