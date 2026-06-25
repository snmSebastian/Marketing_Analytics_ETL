"""
Módulo de orquestación para la extracción de datos de Demanda desde Snowflake.
Utiliza los componentes de conexión (Conection) para realizar consultas 
al Data Warehouse y obtener el histórico de Forecast actualizado.
"""

import snowflake.connector

from .Conection import conectar_snowflake_sso, query
from Fill_Rate.Process_ETL.Process_Files import group_parquet,clean_sku
    

def main():
    """
    Orquesta el flujo de extracción de datos de Demanda.
    El proceso incluye:
     1) Conexión a Snowflake vía SSO.
     2) Ejecución de query con filtros de periodo fiscal actual.
     3) Carga de resultados en DataFrame.
    Returns: None: La función orquesta el proceso y no devuelve un valor.
    """
    from config_paths import DatamindPaths

    edas_update_output_file = DatamindPaths.INPUT_RAW_EDAS_FILE
    
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: EDAS DATA EXTRACTION ---")
    print("=" * 55)
    conexion=conectar_snowflake_sso(Database="PROD_MARTS",Schema="EDAS_POS")
    sql="""
        SELECT
            sbd_fiscal_week_end_date AS "Date",
            TO_VARCHAR(sbd_fiscal_year) || '-' || TO_VARCHAR(sbd_fiscal_month) AS "Year-Month",
            TO_VARCHAR(sbd_fiscal_year) || '-' || TO_VARCHAR(sbd_fiscal_week) AS "Year-Week",
            
            country AS "Country",
            parent_customer AS "Retailer",
            brand AS "Brand",

            customer_material AS "SKU",
            customer_material_description AS "Sku Description",
        
            sell_through_pos_bcrncy_amt_usd AS "Venta neta",
            sell_in_pos_bcrncy_amt_usd AS "Venta bruta",
            POS_QUANTITY AS "Unidades vendidas",
            sell_through_pos_value AS "Precio Publico Estimado",
            sbd_fiscal_year AS "year"
        FROM 
            PROD_MARTS.EDAS_POS.VW_BRZ_EDAS_POS
        WHERE 
            sbd_fiscal_year >= 2023
             AND LOWER(parent_customer) || '-' || LOWER(country) IN (
                            'dyna & cia sa-colombia',
                            'amazon.com services inc-brazil',
                            'casa ferretera sa-colombia',
                            'meico-colombia',
                            'jen-colombia',
                            'sodimac-colombia',
                            'sabesa-costa rica',
                            'novex-guatemala',
                            'grupo arena-peru',
                            'gw yichang-peru',
                            'easy-colombia'
                            )
            

        """
    #             AND DATE_TRUNC('MONTH', sbd_fiscal_week_end_date) >=  DATEADD('MONTH', -1, DATE_TRUNC('MONTH', CURRENT_DATE()))

     #  AND sbd_fiscal_month = MONTH(ADD_MONTHS(CURRENT_DATE(), -1))
    
    df_queryEDAS=query(conexion,sql)
    df_queryEDAS=clean_sku(df_queryEDAS,'SKU')
    df_queryEDAS.to_parquet(edas_update_output_file,index=False)
    

    print("--- 🔄 PROCESO FINALIZADO:  EDAS DATA EXTRACTION ---")


if __name__ == "__main__":
    main()
    