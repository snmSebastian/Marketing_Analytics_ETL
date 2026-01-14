"""
Módulo de conexión y extracción de datos desde Snowflake.
Proporciona las herramientas necesarias para la autenticación mediante SSO,
ejecución de consultas SQL y retorno de resultados en formato DataFrame,
asegurando una integración fluida con el ecosistema de datos.
"""

#--------------------------------------------------
#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
import snowflake.connector

def conectar_snowflake_sso(Database:str, Schema:str):
    """
    Establece una conexión con Snowflake utilizando el navegador 
    para autenticación (SSO / External Browser).

    Args:
        Database (str): Nombre de la base de datos a conectar.
        Schema (str): Nombre del esquema específico dentro de la base de datos.

    Returns:
        snowflake.connector.connection: Objeto de conexión si es exitoso, None en caso contrario.
    """

    # --- CONFIGURACIÓN DE PARÁMETROS DE CONEXIÓN --
    conn_params = {
        "account": "PAA12529-SBD_CASPIAN",
        "user": "SEBASTIAN.NUNEZ@SBDINC.COM",
        "authenticator": "externalbrowser",
        "role": "SSN0609_ROLE",
        "warehouse": "DEV_AIDA_WH",
        "database": Database,
        "schema": Schema
    }

    try:
        # Establecer la conexión
        print("Iniciando autenticación... Por favor, revisa tu navegador.")
        ctx = snowflake.connector.connect(**conn_params)
        
        print("Conexión exitosa.")
        return ctx

    except Exception as e:
        print(f"Error al conectar: {e}")
        return None

def query(conexion,sql:str):
    """
    Ejecuta una consulta SQL en la conexión proporcionada y retorna los resultados.

    Args:
        conexion: Objeto de conexión activo de Snowflake.
        sql (str): Cadena de texto con la consulta SQL a ejecutar.

    Returns:
        pd.DataFrame: Resultados de la consulta en un DataFrame de Pandas.
    """
    try:
        # --- EJECUCIÓN DE CONSULTA ---
        cs = conexion.cursor()
        result=cs.execute(sql).fetch_pandas_all()
        return result
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        return None


#--------------------------------------------------
# Prueba de ejecucion con query demand
#--------------------------------------------------

def main():
    """
    Orquesta el flujo de extracción de demanda desde Snowflake.
    El proceso incluye:
    1) Establecimiento de conexión SSO. 
    2) Definición y ejecución de la consulta de Forecast. 
    """
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: SNOWFLAKE DATA EXTRACTION ---")
    print("=" * 55)
 
    conexion=conectar_snowflake_sso(Database="PROD_MARTS",Schema="DEMAND")
    sql="""
        SELECT
                    FISCAL_PERIOD,
                    FYR_ID,
                    PROD_KEY,
                    DMD_GRP_KEY,
                    LOC_KEY,
                    gpp_basic_sbu_name,
                    gpp_basic_div_name,
                    dmd_gpp_ctgy_cd,
                    dmd_gpp_basic_portfolio,
                    FCST_QTY,
                    FORECAST_VALUE_GSV,
                    CURRENT_STANDARD_COST
                    
                FROM PROD_MARTS.DEMAND.VW_BRZ_DEMAND_HISTORY_FORECAST_TOOLS
                WHERE FISCAL_PERIOD >= MONTH(DATEADD(month, -1, CURRENT_DATE())) 
                AND FYR_ID >= YEAR(CURRENT_DATE())
        """

    df_queryDemand=query(conexion,sql)
    df_queryDemand.to
    #print(df_queryDemand.head())


if __name__ == "__main__":
    main()