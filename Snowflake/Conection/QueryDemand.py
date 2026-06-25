"""
EL RADAR DE DEMANDA: EXTRACCIÓN DE FORECAST GLOBAL
-------------------------------------------------
Este script es nuestro "ojo en el futuro". Se conecta a Snowflake para traerse la foto 
oficial del Forecast (Demanda). Sin esta data, estaríamos operando a ciegas, ya que 
es la base para planear el inventario y entender qué espera vender la región este 
año y el que viene.

Es el primer paso para que Supply y Finance sepan si estamos alineados con el mercado.

FLUJO DE TRABAJO:
1. Túnel a la Nube: Establece la conexión segura con el esquema de DEMAND en Snowflake 
   usando el conector core de la librería.
2. Ventana de Tiempo Inteligente: Ejecuta un query que solo trae lo relevante: desde 
   el mes actual hasta el cierre del próximo año (evitamos basura histórica pesada).
3. El Filtro de Seguridad: Aplica una lista estricta de "Demand Groups" (DMD_GRP_KEY) 
   para asegurar que solo procesamos los clusters autorizados por el negocio.
4. Persistencia en Parquet: Guarda la extracción en un archivo Parquet ultra-comprimido, 
   dejando la data lista para que el resto del pipeline la procese sin lag.

💡 NOTA DE SENIOR:
¡Mucho ojo con la lista de `DMD_GRP_KEY` en el SQL! Si el equipo de planeación crea un 
nuevo grupo en SAP y no lo agregamos a este "hardcode", esa demanda simplemente 
no aparecerá en los reportes regionales. Si notas que faltan números de un país 
específico, revisa primero si su llave está en ese `IN (...)`.
"""

import snowflake.connector

from .Conection import conectar_snowflake_sso, query
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
            GPP_BASIC_DIV_NAME as "GPP Division Code",
            DMD_GPP_CTGY_CD as "GPP Category Code",
            DMD_GPP_BASIC_PORTFOLIO as "GPP Portfolio Code",
            FCST_QTY,
            FORECAST_VALUE_GSV AS "FORECAST_VALUE_GSV",
            CURRENT_STANDARD_COST
                                
      FROM PROD_MARTS.DEMAND.vW_BRZ_DEMAND_HISTORY_FORECAST_TOOLS
      
      WHERE 
      FYR_ID >= YEAR(CURRENT_DATE()) 
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
#WHERE FISCAL_PERIOD >= MONTH(CURRENT_DATE()) 
      
    df_queryDemand=query(conexion,sql)
    print(df_queryDemand.head())
    df_queryDemand=clean_sku(df_queryDemand,'Global Material')
    df_queryDemand.to_parquet(demand_update_raw_dir / 'QueryDemand.parquet', index=False)
    print(df_queryDemand.head())
    print("--- 🔄 PROCESO FINALIZADO: DEMAND DATA EXTRACTION ---")

if __name__ == "__main__":
    main()
    