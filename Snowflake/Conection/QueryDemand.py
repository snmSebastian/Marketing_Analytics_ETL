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
    from config_paths import DemandPaths
    demand_update_raw_dir = DemandPaths.INPUT_RAW_UPDATE_DIR
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: DEMAND DATA EXTRACTION ---")
    print("=" * 55)
    conexion=conectar_snowflake_sso(Database="PROD_MARTS",Schema="DEMAND")
    sql="""
        SELECT
            FISCAL_PERIOD as "Fiscal Period",
            FYR_ID as "Fiscal Year",
            PROD_KEY as "Global Material",
            DMD_GRP_KEY as "Demand Group",
            LOC_KEY as "Plant Code",
            GPP_BASIC_SBU_NAME as "SBU",
            DIV as "GPP Division Code",
            DMD_GPP_CTGY_CD as "GPP Category Code",
            DMD_GPP_BASIC_PORTFOLIO as "GPP Portfolio Code",
            FCST_QTY,
            FORECAST_VALUE_GSV,
            CURRENT_STANDARD_COST
                                
      FROM  PROD_MARTS.DEMAND.VW_BRZ_GLOBAL_FORECAST_SNAPSHOT
      WHERE FISCAL_PERIOD >= MONTH(CURRENT_DATE()) 
      AND FYR_ID >= YEAR(CURRENT_DATE()) 
     AND FYR_ID <=YEAR(CURRENT_DATE())+1
     AND DMD_GRP_KEY IN ('ARDIST','AREASY','ARECOMM','ARFZ','ARHYPER','ARINTERCO','AROTHER',
                        'ARSODIMAC','CHARDISTFZ','MRARAFIL','MRAROTH','MRUROTH','BRARDIST',
                        'BRATA','BRATASP','BRCON','BRCONSP','BRECO','BRECOSP','BRHC','BRHCSP',
                        'BRMDRSP','BRMRO','BRMROSP','BROTHER','BROTHSP','BRVAR','BRVARSP','BRB2CSP',
                        'MRCA','MRGC','MROTHER','MRPAN','MRCCFNL','CHECOMM','CHINTERCO','CHMDR',
                        'CHOTHER','CHSODIMAC','MRCHMDR','MRCHOTH','CHTRAD','CHIND','COECOMM',
                        'COMDR','COOTHER','COSODIMAC','MRCOAFIL','MRCOOTH','MRECOTH','BRECEXP',
                        'MRECFNL','BNDSAWSEG','MRMXOTH','MXFNL','MXHD','MXINTERCO','MXMDR',
                        'MXOD','MXOM','MXOTHER','MXTRAD','MXWM','BRINTERCO','MRPEMDR','MRPEOTH',
                        'PEIND','PEINTERCO','PEMDR','PEOTHER','PESODIMAC','BRMDR','CHARPUBFZ','BRARPUB','COFNL','PEECOMM')

        """

    df_queryDemand=query(conexion,sql)
    df_queryDemand.to_parquet(demand_update_raw_dir / 'QueryDemand.parquet', index=False)
    #print(df_queryDemand.head())
    print("--- 🔄 PROCESO FINALIZADO: DEMAND DATA EXTRACTION ---")


if __name__ == "__main__":
    main()
    