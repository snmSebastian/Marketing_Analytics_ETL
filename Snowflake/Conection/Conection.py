"""
LA LLAVE MAESTRA DE LA NUBE: CONECTOR CORE SNOWFLAKE
---------------------------------------------------
Este script es el cordón umbilical entre nuestros procesos locales y el Data Warehouse 
global (Caspian/Snowflake). Su misión es abrir la puerta de forma segura para que 
podamos traer la data de ventas, demanda y maestros sin tener que andar descargando 
reportes manuales que pesan toneladas.

Es el "Traductor Oficial" que convierte consultas SQL en DataFrames de Pandas listos 
para procesar.

FLUJO DE TRABAJO:
1. Apertura del Portal: Lanza la autenticación vía SSO (Single Sign-On), abriendo 
   tu navegador para validar que eres tú.
2. Negociación de Acceso: Configura el Warehouse, Rol y Esquema específicos para 
   que Snowflake sepa qué recursos usar y no nos bloquee.
3. Extracción de Oro: Ejecuta el query y hace el fetch de los datos directamente 
   a memoria, saltándose el paso de generar archivos intermedios.

💡 NOTA DE SENIOR:
¡Mucho cuidado! Este módulo es una LIBRERÍA CORE. Prácticamente todos los scripts 
de extracción (`QuerySales`, `QueryDemand`, `QueryNameSku`) dependen de este archivo. 
Si cambias algo en `conn_params` (como el warehouse o el rol), podrías romper el 
acceso de todo el equipo de LAG. 
Ojo: Como usa `externalbrowser`, siempre va a pedirte interacción humana. No sirve 
para procesos 100% automáticos en servidores "ciegos" sin antes cambiar el método a 
Service Account.
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


def conectar_snowflake_password(Database:str, Schema:str):
    """
    Establece una conexión con Snowflake utilizando usuario y contraseña directa.

    Args:
        Database (str): Nombre de la base de datos.
        Schema (str): Nombre del esquema.
        Password (str): Tu contraseña de Snowflake.

    Returns:
        snowflake.connector.connection: Objeto de conexión.
    """

    # --- CONFIGURACIÓN DE PARÁMETROS DE CONEXIÓN --
    conn_params = {
    "account": "PAA12529-SBD_CASPIAN",
    "user": "SVC-LAGBI@sbdinc.com", 
    "authenticator": "externalbrowser",
    "role": "SVC_LAGBI_ROLE",
    "warehouse": "DEV_AIDA_WH",
    "database": Database,
    "schema": Schema
    }



    try:
        # Establecer la conexión
        print("Conectando a Snowflake...")
        ctx = snowflake.connector.connect(**conn_params)
        if ctx is None:
            print("🛑 Proceso detenido: Error en la conexión a Snowflake.")
            return
        print("Conexión exitosa(via password).")
        return ctx
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"❌ Error de credenciales o permisos: {e}")
        return None
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

