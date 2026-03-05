"""
Módulo de orquestación para la extracción de datos de Demanda desde Snowflake.
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
    from config_paths import SalesPaths      
    sales_historic_processed_dir =SalesPaths.OUTPUT_PROCESSED_PARQUETS_DIR
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: DEMAND DATA EXTRACTION ---")
    print("=" * 55)
    conexion=conectar_snowflake_sso(Database="PROD_MARTS",Schema="LAGBI")
    sql="""
       SELECT 
            SRC_SYS_KEY AS "Source System",
            DOCUMENT_TYPE AS "Document Type",


            concat( right(FMTH_ID,2),'/01/',left( FMTH_ID,4))  AS "fk_Date",
            concat( left( FMTH_ID,4),'-',right(FMTH_ID,2)) AS "fk_year_month",
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
                
            DISPATCH_DCRNCY_AMT_USD AS "Total Sales", 
            COST_DCRNCY_AMT_USD AS "Total Cost",
            DISPATCH_INVC_QTY AS "Units Sold",
            return_qty as "Units Return",
            NSV_BCRNCY_AMT_PLRATE AS "NSV",
            fx_nsv_bcrncy_amt_plrate as "FX Rate NSV"

       FROM PROD_MARTS.LAGBI.VW_BRZ_SALES_BILLING_LAGBI  
       WHERE DOCUMENT_TYPE <>'CREDIT' 
       AND FMTH_ID = TO_CHAR(ADD_MONTHS(CURRENT_DATE(), -1), 'YYYYMM')
        """

    df_querySales=query(conexion,sql)
    df_querySales.to_parquet(r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Sales\Mothly_Update\QuerySales.parquet', index=False)
    #print(df_queryDemand.head())
    print("--- 🔄 PROCESO FINALIZADO: SALES DATA EXTRACTION ---")


if __name__ == "__main__":
    main()
    
