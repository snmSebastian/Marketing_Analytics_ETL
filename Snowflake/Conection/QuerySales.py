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
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: SALES DATA EXTRACTION ---")
    print("=" * 55)
    conexion=conectar_snowflake_sso(Database="PROD_MARTS",Schema="LAGBI")
    #conexion=conectar_snowflake_password(Database="PROD_MARTS",Schema="LAGBI")
    sql="""
    SELECT 
                SRC_SYS_KEY AS "Source System",
                DOCUMENT_TYPE AS "Document Type",

    
                concat( right(FMTH_ID_FINANCIAL,2),'/01/',left( FMTH_ID_FINANCIAL,4))  AS "fk_Date",
                concat( left( FMTH_ID_FINANCIAL,4),'-',right(FMTH_ID_FINANCIAL,2)) AS "fk_year_month",
                FWK_ID AS "Week",

                ENTITY_LEVE3_LNDESC AS "Region Cluster",
                ENTITY_LEVE4_LNDESC AS "Region Consolidated",
                ENTITY_LEVE5_LNDESC AS "fk_Country",
                ENTITY_LEVE6_LNDESC AS "Country Detail",
                
                ENTITY_LEVE7_LNDESC AS "Sales Type",
                ENTITY_LEVE8_LNDESC AS "Sales Type Detail",
                ENTITY_LEVE9_LNDESC AS "Sales Type Invoince Country",

                SOLDTO_CUST_KEY AS "fk_Sold_To_Customer_Code",
                soldto_cust_name as "Customer Name",
                PROD_ID_HRMZ AS "fk_SKU",
                prod_name as "SKU Description",
                brand_desc as "Brand",

                gpp_division_desc as "GPP Division",
                gpp_division_id as "GPP Division Code",
                gpp_category_desc as "GPP Category",
                gpp_category_id as "GPP Category Code",
                gpp_portfolio_desc as "GPP Portafolio",
                gpp_portfolio_id as "GPP Portafolio Code",
                    
                DISPATCH_BCRNCY_AMT_PLRATE AS "Total Sales",            
                COST_DCRNCY_AMT_USD_PLRATE AS "Total Cost",
                DISPATCH_INVC_QTY AS "Units Sold",
                return_qty as "Units Return",
                NSV_BCRNCY_AMT_PLRATE AS "NSV",
                fx_nsv_bcrncy_amt_plrate as "FX Rate NSV",
                fx_nsv_financial_plrate as "fx_nsv_financial",
                outbound_bcrncy_usd_plrate as "Outbound"




    FROM PROD_MARTS.LAGBI.VW_BRZ_SALES_BILLING_LAGBI  
    WHERE DOCUMENT_TYPE <>'CREDIT'      
  
    """
#    AND FMTH_ID_FINANCIAL >= TO_CHAR(ADD_MONTHS(CURRENT_DATE(), -6), 'YYYYMM')

   # AND FMTH_ID_FINANCIAL = TO_CHAR(CURRENT_DATE(), 'YYYYMM')

    # TRAE INFO DEL MES ANTERIOR HASTA EL DIA DE HOY
    #-- Desde el primer día del mes pasado
    #  AND FMTH_ID_FINANCIAL >= TO_CHAR(ADD_MONTHS(CURRENT_DATE(), -1), 'YYYYMM')
    #  -- Hasta el mes en curso actual
    #  AND FMTH_ID_FINANCIAL <= TO_CHAR(CURRENT_DATE(), 'YYYYMM')

    #AND FMTH_ID_FINANCIAL = TO_CHAR(ADD_MONTHS(CURRENT_DATE(), -1), 'YYYYMM')

    #AND FMTH_ID_FINANCIAL BETWEEN '202301' AND '202501'
    #AND SRC_SYS_KEY <>'QADBR'
    #    AND FMTH_ID_FINANCIAL = TO_CHAR(ADD_MONTHS(CURRENT_DATE(), -1), 'YYYYMM')

    df_querySales=query(conexion,sql)
    
    if df_querySales is None:
        print("Error: No se pudieron obtener datos de ventas de Snowflake. Terminando el proceso.")
        return

    df_querySales=clean_sku(df_querySales,'fk_SKU')
    df_querySales.to_parquet(sales_update_raw_dir /'QuerySales.parquet', index=False)
    #print(df_queryDemand.head())
    print("--- 🔄 PROCESO FINALIZADO: SALES DATA EXTRACTION ---")


if __name__ == "__main__":
    main()
    
