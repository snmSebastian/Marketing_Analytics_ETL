"""
EL BUSCADOR DE CLIENTES: EXTRACCIÓN Y TRADUCTOR DE CATEGORÍAS
-----------------------------------------------------------
Este script es el puente hacia la base de datos de clientes en Snowflake. Su misión es 
crítica: no solo baja la lista de clientes, sino que arregla el "caos" de canales y 
subcanales que vienen del sistema. Es el que se encarga de que un cliente no aparezca 
como "Unknown" solo porque en el ERP alguien escribió mal el nombre del canal.

FLUJO DE TRABAJO:
1. Conexión a la Nube: Abre el portal a Snowflake para traerse la foto actual de 
   clientes, regiones y sus clasificaciones originales.
2. Filtro de Unicidad: Limpia duplicados para que tengamos una sola "verdad" por 
   cada combinación de Código de Cliente y País.
3. El "Traductor" (Notation Fixer): Cruza la data contra una tabla de errores comunes 
   para traducir nombres técnicos o mal escritos a nuestras categorías oficiales.
4. Mapeo por Prioridad: Intenta clasificar al cliente usando un orden lógico: primero 
   por la combinación más específica (Subcanal+Canal) y, si no encuentra match, va 
   bajando hasta encontrar una identidad válida.
5. Entrega Limpia: Genera el archivo Excel que sirve de insumo para el Maestro de Clientes.

💡 NOTA DE SENIOR:
Mucho ojo con la función `notation_classification_customers_datalake`. Usa un mapeo por 
diccionario (Hash Map) para que sea ultra rápido, pero depende totalmente del archivo 
de "Notation Error". Si ese Excel tiene llaves duplicadas o vacías, el `.map()` va 
a escupir "NOTFOUND" y se nos van a desclasificar los clientes en los reportes.
"""

import snowflake.connector
import pandas as pd

from .Conection import conectar_snowflake_sso, query

def notation_classification_customers_datalake(df_customers, df_notation):
    # 1. Crear una copia para evitar SettingWithCopyWarning y no mutar originales
    df_c = df_customers.copy()
    df_n = df_notation.copy()

    # 2. Pre-procesar el catálogo de referencia (df_notation)
    # Evitamos múltiples llamadas a str; creamos una función auxiliar si es necesario
    def clean_str_series(series):
        return series.fillna("-").astype(str).str.upper().str.replace(r'\s+', '', regex=True)

    # Crear la llave en el catálogo
    df_n['SOLDTO_DIST_TYPE_DESC_HMZ'] = clean_str_series(df_n['SOLDTO_DIST_TYPE_DESC_HMZ'])
    df_n['SOLDTO_DIST_CHNL_DESC_HMZ'] = clean_str_series(df_n['SOLDTO_DIST_CHNL_DESC_HMZ'])
    
    df_n['fk_key'] = df_n['SOLDTO_DIST_TYPE_DESC_HMZ'] + '-' + df_n['SOLDTO_DIST_CHNL_DESC_HMZ']
    
    #Creamos diccionario para mapear a traves de la llave
    mapping_fk = dict(zip(df_n['fk_key'], df_n['Final']))
    mapping_subcanal = dict(zip(df_n['SOLDTO_DIST_TYPE_DESC_HMZ'], df_n['Final']))
    
    # 3. Pre-procesar llaves en el dataframe principal
    cols_to_clean = ['SubCanalopc1', 'Canalopc1', 'SubCanalopc2', 'Canalopc2', 'fk_Sold-To Customer Code']
    for col in cols_to_clean:
        if col in df_c.columns:
            df_c[col] = clean_str_series(df_c[col])

    # Crear las llaves compuestas ya limpias
    df_c['fk_key1'] = df_c['SubCanalopc1'] + '-' + df_c['Canalopc1']
    df_c['fk_key2'] = df_c['SubCanalopc2'] + '-' + df_c['Canalopc2']

    # 4. Lógica de asignación optimizada
    # Intentamos encontrar match en orden de prioridad: key1, key2, y luego individuales
    # Usamos .map() que es extremadamente eficiente en Pandas
    df_c['fk_Dist_Channel'] =(
        df_c['SubCanalopc1'].map(mapping_subcanal)
        .fillna(df_c['SubCanalopc2'].map(mapping_subcanal))
        .fillna(df_c['fk_key1'].map(mapping_fk))
        .fillna(df_c['fk_key2'].map(mapping_fk))
        .fillna("NOTFOUND")
        )    

    return df_c

def main():
    """
    Orquesta el flujo de extracción de datos de Demanda.
    El proceso incluye:
     1) Conexión a Snowflake vía SSO.
     2) Ejecución de query con filtros de periodo fiscal actual.
     3) Carga de resultados en DataFrame.
    Returns: None: La función orquesta el proceso y no devuelve un valor.
    """
    from config_paths import MASTER_CUSTOMERS_PATHS      
    query_customers_file = MASTER_CUSTOMERS_PATHS.INPUT_RAW_QUERY_CUSTOMERS_FILE
    Notation_subchannel_datalake_w_error=MASTER_CUSTOMERS_PATHS.INPUT_RAW_NOTATION_SUBCHANNEL_DATALAKE_W_ERROR_FILE

    df_notation_subchannel_datalake_w_error=pd.read_excel(Notation_subchannel_datalake_w_error, dtype=str, engine='openpyxl',sheet_name='List_SubChannel_w_error')
        
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: CUSTOMERS DATA EXTRACTION ---")
    print("=" * 55)
    conexion=conectar_snowflake_sso(Database="PROD_MARTS",Schema="LAGBI")
    sql="""
    SELECT 
                SRC_SYS_KEY AS "Source System",
                DOCUMENT_TYPE AS "Document Type",

                ENTITY_LEVE3_LNDESC AS "Region Cluster",
                ENTITY_LEVE4_LNDESC AS "Region Consolidated",
                ENTITY_LEVE5_LNDESC AS "fk_Country",
                ENTITY_LEVE6_LNDESC AS "Country Detail",
                

                SOLDTO_CUST_KEY AS "fk_Sold-To Customer Code",
                soldto_cust_name as "Customer Name",
                SOLDTO_DIST_TYPE_DESC_HMZ as "SubCanalopc1",
                SOLDTO_DIST_CHNL_DESC_HMZ as "Canalopc1",
                SOLDTO_DIST_CHNL_DESC as "SubCanalopc2",
                SOLDTO_DIST_TYPE_DESC as "Canalopc2"


    FROM PROD_MARTS.LAGBI.VW_BRZ_SALES_BILLING_LAGBI  
    WHERE DOCUMENT_TYPE <>'CREDIT'      
        """
#AND FMTH_ID = TO_CHAR(ADD_MONTHS(CURRENT_DATE(), -1), 'YYYYMM')
    df_queryCustomers=query(conexion,sql)
    df_queryCustomers.drop_duplicates(['fk_Sold-To Customer Code','fk_Country'],inplace=True)
    df_queryCustomers=notation_classification_customers_datalake(df_queryCustomers,df_notation_subchannel_datalake_w_error)
    df_queryCustomers.to_excel(query_customers_file,index=False)
    print(df_queryCustomers.columns)
    #print(df_queryDemand.head())
    print("--- 🔄 PROCESO FINALIZADO: QUERY CUSTOMERS DATA EXTRACTION ---")


if __name__ == "__main__":
    main()
    
