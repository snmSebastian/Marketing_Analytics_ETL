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
    from config_paths import SodimacPaths

    sodimac_edas_update_output_dir = SodimacPaths.OUTPUT_PROCESSED_PARQUETS_DIR_EDAS
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: SODIMAC EDAS DATA EXTRACTION ---")
    print("=" * 55)
    conexion=conectar_snowflake_sso(Database="PROD_MARTS",Schema="EDAS_POS")
    sql="""
        SELECT
            source_customer_indicator,
            country,
            prod_key,
            prod_desc,
            customer_material,
            customer_material_description,
            brand,
            sell_in_currency,
            sell_through_pos_value ,
            sell_in_pos_bcrncy_amt_usd,
            sell_through_pos_bcrncy_amt_usd,

            POS_QUANTITY,
            sbd_fiscal_year,
            sbd_fiscal_quarter,
            sbd_fiscal_month,
            sbd_fiscal_week,
            sbd_fiscal_week_start_date,
            sbd_fiscal_week_end_date,
            TO_VARCHAR(sbd_fiscal_year) || '-' || TO_VARCHAR(sbd_fiscal_month) AS fk_year_month

       FROM PROD_MARTS.EDAS_POS.VW_BRZ_EDAS_POS
       WHERE source_customer_indicator in ('SODIMAC_CL_POS_M','SODIMAC_CO_D','SODIMAC_MX_POS_W','SODIMAC_AR_POS_W','SODIMAC_PE_POS_W') 

       AND sbd_fiscal_year = YEAR(ADD_MONTHS(CURRENT_DATE(), -1))
       AND sbd_fiscal_month = MONTH(ADD_MONTHS(CURRENT_DATE(), -1))
        """

    df_querySodimac=query(conexion,sql)
    try:
        df_querySodimac.rename(columns={
        'FK_YEAR_MONTH': 'fk_year_month'},inplace=True)
    except:
        pass
    df_querySodimac=clean_sku(df_querySodimac,'prod_key')

    group_parquet(df_querySodimac,sodimac_edas_update_output_dir , name='Sodimac_EDAS')

    print("--- 🔄 PROCESO FINALIZADO: SODIMAC EDAS DATA EXTRACTION ---")


if __name__ == "__main__":
    main()
    