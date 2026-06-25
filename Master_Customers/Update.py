"""
MAESTRO DE CLIENTES: Orquestador de Dimensiones y Clasificación Regional.

Este módulo actúa como el "estandarizador" de la base de datos de clientes. Su misión 
es capturar nuevos registros que aparecen en Fill Rate y Sales para asignarles una 
identidad de negocio (Canal y Tipo de Distribución) antes de integrarlos al maestro.

¿Qué hace especial a este proceso?
 1. LIMPIEZA DE CÓDIGOS: Trata los códigos de cliente (slicing y limpieza de '/') 
    para asegurar que las llaves de cruce sean perfectas.
 2. LÓGICA DE CANALES (complete_clasification): No solo une tablas, aplica reglas 
    específicas por país. Por ejemplo, si un cliente es de Colombia y no tiene canal, 
    lo asigna a 'SHOWROOMS'; para otros países usa 'TRADITIONALHARDWARESTORES'.
 3. ENRIQUECIMIENTO: Cruza la data con tablas de "Clientes Compartidos" y 
    clasificaciones finales para normalizar nombres y categorías.
 4. ACTUALIZACIÓN INTELIGENTE (Upsert): Compara el maestro de Excel con la data nueva 
    usando la llave 'País-Cliente', reemplaza lo existente y añade lo nuevo.

💡 Nota Senior: Este módulo es el que garantiza que en Power BI los clientes no 
aparezcan como "Sin Clasificar", dándole orden al reporte regional.

 Primero asigna info proveniente de datalake, si no asigna segun reglas establecidas.
"""

#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
# Liberia
import pandas as pd
import numpy as np
# Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico.
import glob
import os
import sys
import re
from Fill_Rate.Process_ETL.Process_Files import asign_country_code, read_files
from Sales.Process_ETL.Update import read_files_parquets

def assing_clasification(df_consolidated, df_all_customers, df_customers_clasifications):
    """
    Aplica la lógica ETL completa para asignar el Canal y Tipo de Distribución (Dist Channel/Type) a los nuevos
    clientes. Incluye limpieza de código de cliente, mapeo de clasificación compartida, limpieza de
    notación y la asignación condicional (np.select) basada en el país y el estado del mapeo de canal.
    Args:
        df_consolidated (pd.DataFrame): DataFrame con la información de clientes nuevos (consolidado de Fill Rate y Sales).
        df_customers_shared (pd.DataFrame): Tabla de referencia de clientes compartidos.
        df_customers_clasifications (pd.DataFrame): Tabla de referencia con las clasificaciones finales de canales y tipos.
        df_country (pd.DataFrame): Tabla de referencia para el mapeo de códigos de país.
    Returns:
        pd.DataFrame: DataFrame final con el esquema de la tabla maestra de clientes, incluyendo las columnas 'fk_Dist_Channel' y 'fk_Dist_Type' completadas.
    """
    # Asignacion de pais   
    df_consolidated['code_customer'] = (
                                    # . Aplicamos el slicing a la columna original (con .str[2:] para cada elemento)
                                df_consolidated['fk_Sold-To Customer Code'].str[3:]
                                .where(
                                    # La condición: ¿El valor en esa celda contiene '/'?
                                    df_consolidated['fk_Sold-To Customer Code'].str.contains('/'),
                                    # El 'otro' valor: Mantener el valor original de la columna
                                    other=df_consolidated['fk_Sold-To Customer Code']
                                    )
                                    # se quitan los ceros iniciales
                                    #.str.lstrip('0')
                                )
    

    # Crea las fk para relacionar info de clientes compartidos con los nuevos clientes
    df_all_customers['fk_country_customer'] = df_all_customers['fk_Country'].astype(str) + '-' + df_all_customers['fk_Sold-To Customer Code'].astype(str) 
    df_all_customers['fk_country_customer'] = df_all_customers['fk_country_customer'].str.upper().str.strip().str.replace(' ', '')
   
   
    df_consolidated['fk_country_customer'] = df_consolidated['fk_Country'] + '-' + df_consolidated['code_customer'].astype(str)    
    df_consolidated['fk_country_customer'] = df_consolidated['fk_country_customer'].str.upper().str.strip().str.replace(' ', '')
   
    
    df_customers_clasifications['fk_channel']=df_customers_clasifications['pk_Sold-To Dist Channel']
    df_customers_clasifications['fk_channel'] = df_customers_clasifications['fk_channel'].astype(str).str.upper().str.strip().str.replace(' ', '')
   
    df_consolidated=pd.merge(df_consolidated,
             df_all_customers[['fk_country_customer', 'fk_Dist_Channel']],
             how='left',
             on='fk_country_customer')
    
    df_consolidated['fk_Dist_Channel']=df_consolidated['fk_Dist_Channel'].fillna('NOTFOUND')
    df_consolidated['fk_Dist_Channel'] = df_consolidated['fk_Dist_Channel'].str.upper().str.strip().str.replace(' ', '')
   
   
    diccionario_map = {'MESSMERCHANT':'MASSMERCHANT'}
    df_consolidated['fk_Dist_Channel']=df_consolidated['fk_Dist_Channel'].replace(diccionario_map)
   

    channel_map=list(set(df_customers_clasifications['fk_channel'].unique())) 
    # condiciones para completar la clasificación de los clientes
    es_colombia = (df_consolidated['fk_Country'] == 'COLOMBIA')
    canal_no_mapeado = (~df_consolidated['fk_Dist_Channel'].isin(channel_map))
    
    condiciones = [
     # Condición 1: Colombia Y canal no mapeado
    es_colombia & canal_no_mapeado, 
    # Condición 2: Colombia Y canal sí mapeado (el resto de Colombia)
    es_colombia & (~canal_no_mapeado),
    # Condición 3: NO Colombia Y canal no mapeado
    (~es_colombia) & canal_no_mapeado, 
    # Condición 4: NO Colombia Y canal sí mapeado (el resto de los canales)
    (~es_colombia) & (~canal_no_mapeado)
    ]
    
    #  Definir los valores a asignar para cada condición
    valores = [
    'SHOWROOMS',# Condición 1: COLOMBIA y NO en subcanal_map
    df_consolidated['fk_Dist_Channel'], # Condición 2: COLOMBIA y SÍ en subcanal_map (usa el valor actual)
    'TRADITIONALHARDWARESTORES', # Condición 3: OTRO país y NO en subcanal_map
    df_consolidated['fk_Dist_Channel'] # Condición 4: OTRO país y SÍ en subcanal_map (usa el valor actual)
    ]

    df_consolidated['fk_Dist_Channel'] = np.select(
    condiciones, 
    valores, 
    default=df_consolidated['fk_Dist_Channel'] # Si ninguna condición aplica (por seguridad)
    )

    df_consolidated=pd.merge(df_consolidated,
             df_customers_clasifications[['fk_channel','pk_Sold-To Dist Channel','fk_Sold-To Dist Type']],
             how='left',
             left_on='fk_Dist_Channel',
             right_on='fk_channel'
             )
   
    # delete duplicates
    df_consolidated = df_consolidated.drop_duplicates(subset=['fk_country_customer'])
   
    lst_columns=['fk_Country', 'fk_Sold-To Customer Code', 'Sold-To Customer Name',
    'pk_Sold-To Dist Channel', 'fk_Sold-To Dist Type','fk_country_customer']
    df_consolidated=df_consolidated[lst_columns]
   
    df_consolidated.rename(columns={
    'fk_Country': 'fk_Country',
    'fk_Sold-To Customer Code': 'fk_Sold-To Customer Code',
    'Sold-To Customer Name': 'Sold-To Customer Name',
    'pk_Sold-To Dist Channel': 'fk_Dist_Channel',
    'fk_Sold-To Dist Type': 'fk_Dist_Type'
    }, inplace=True)
   
    # Usar np.select para aplicar todas las condiciones de una vez 
    return df_consolidated

def update_excel_file(df_master, df_consolidated,name='master_customers'):
    """
    Implementa la lógica de Upsert (reemplazo) sobre el archivo maestro. Los registros se reemplazan si la
    clave compuesta fk_country_customer existe en el nuevo DataFrame consolidado. Filtra el maestro,
    excluye los registros a actualizar, y concatena los nuevos.
    
    Args:
        df_master (pd.DataFrame): DataFrame actual del archivo Excel maestro de clientes.
        df_consolidated (pd.DataFrame): DataFrame con los nuevos registros a insertar o actualizar.
        name (str, optional): Nombre del maestro, usado para adaptar la clave compuesta (si el código de cliente tiene un nombre diferente). Por defecto es 'master_customers'.
    Returns:
        pd.DataFrame: El DataFrame consolidado final, listo para sobrescribir el archivo maestro de
        clientes.    
    """
    # Obtener la lista de claves únicas a actualizar/reemplazar.
    #print(f'columnas df_update: {df_update.columns}')
    #print(f'columnas df_parquets_historic: {df_parquets_historic.columns}')
  
    
    #Creo la fk en el maste
    if name=='master_customers':
        df_master['fk_country_customer']=df_master['fk_Country']+'-'+df_master['fk_Sold-To Customer Code']
        df_master['fk_country_customer']=df_master['fk_country_customer'].str.upper().str.strip().str.replace(' ',  '')
        df_consolidated['fk_country_customer']=df_consolidated['fk_Country']+'-'+df_consolidated['fk_Sold-To Customer Code']
        df_consolidated['fk_country_customer']=df_consolidated['fk_country_customer'].str.upper().str.strip().str.replace(' ',  '')
    else:
        df_master['fk_country_customer']=df_master['fk_Country']+'-'+df_master['fk_Sold-To Customer Code']
        df_master['fk_country_customer']=df_master['fk_country_customer'].str.upper().str.strip().str.replace(' ',  '')
    
    keys_to_update = df_consolidated['fk_country_customer'].unique()
    # Filtrar el dataframe histórico para excluir los registros que NO serán actualizados.
    df_master_filtered = df_master[~df_master['fk_country_customer'].isin(keys_to_update)]
        
    # Combinar los datos históricos filtrados con los nuevos datos.
    if df_master_filtered.empty:
        df_final = df_consolidated
        print("Advertencia: No se encontraron registros históricos que mantener. df_final = df_update.")
    else:
        df_final = pd.concat([df_master_filtered, df_consolidated], ignore_index=True)
    if name=='master_customers':
        df_final=df_final.drop(columns=['fk_country_customer'])
    else:
        df_final=df_final.drop(columns=['fk_country_customer'])
    return df_final

def notation_name(df_update, df_notation_customers):
    """
    Aplica un mapeo de corrección para los nombres de clientes. Compara los nombres de los clientes en df_update contra
    una tabla de errores de notación. Si encuentra una coincidencia, reemplaza el nombre con la versión corregida;
    de lo contrario, mantiene el nombre original.
    
    Args:
        df_update (pd.DataFrame): DataFrame que contiene los nombres de clientes a ser revisados.
        df_notation_customers (pd.DataFrame): Tabla de referencia con las correcciones de notación
        ('Text Condition' -> 'Result').

    Returns:
        pd.DataFrame: El DataFrame df_update con los nombres de clientes corregidos.
    """
    # 1. Normalización (Mantenemos espacios para poder distinguir palabras)
    df_update['fk_customer'] = df_update['Sold-To Customer Name'].str.upper().str.strip()
    df_notation_customers['fk_key'] = df_notation_customers['Text Condition'].str.upper().str.strip()
    
    # 2. Diccionarios por condición
    df_c = df_notation_customers[df_notation_customers['Condition'] == 'Text Contains']
    df_s = df_notation_customers[df_notation_customers['Condition'] == 'Text Stars']
    df_i = df_notation_customers[df_notation_customers['Condition'] == 'Text Iqual']

    dic_contains = dict(zip(df_c['fk_key'], df_c['Result']))
    dic_starts = dict(zip(df_s['fk_key'], df_s['Result']))
    dic_equal = dict(zip(df_i['fk_key'], df_i['Result']))
    
    def find_match(row):
        name = row['fk_customer']
        original = row['Sold-To Customer Name']
        if pd.isna(name): return original
        
        # 1. IGUALDAD EXACTA
        if name in dic_equal:
            return dic_equal[name]

        # 2. EMPIEZA CON (Pero como palabra completa)
        for key, value in dic_starts.items():
            # r'^' indica inicio de cadena, r'\b' indica límite de palabra
            # Esto hace que AMAZON coincida con "AMAZON CORP" pero NO con "AMAZONAS"
            if re.search(r'^' + re.escape(key) + r'\b', name):
                return value

        # 3. CONTIENE (Como palabra completa en cualquier posición)
        for key, value in dic_contains.items():
            if re.search(r'\b' + re.escape(key) + r'\b', name):
                return value
        
        return original

    df_update['Sold-To Customer Name'] = df_update.apply(find_match, axis=1)
    df_update.drop(columns=['fk_customer'], inplace=True)
    return df_update


def main():
    """	
    Función principal que orquesta el proceso ETL para actualizar el Maestro de Clientes.	
    El proceso incluye:
        1) Consolidación de datos de actualización de Fill Rate y Sales.	
        2) Asignación de clasificaciones de cliente.
        3) Aplicación de la lógica de Upsert al maestro histórico.	
        4) Corrección de notación de nombres.
        5) Guardado final en el archivo Excel maestro.	
    
    Returns: None: La función orquesta el proceso y no devuelve un valor,
                   guardando el resultado en un archivo Excel
    
    """
    print("=" * 55)
    print("---  INICIANDO PROCESO: MD CUSTOMERS UPDATE ETL ---")
    print("=" * 55)
    #--------------------------------------------------
    #---------------- RUTAS ---------------------------
    try:
        from config_paths import MasterCustomersPaths
        country_code_file=MasterCustomersPaths.INPUT_PROCESSED_COUNTRY_CODES_FILE
        customers_shared=MasterCustomersPaths.INPUT_RAW_Customers_Shared_by_Country_FILE
        
        md_customers=MasterCustomersPaths.OUTPUT_FILE_PROCESSED_MASTER_CUSTOMERS_FILE
        
        notation_customers_file=MasterCustomersPaths.INPUT_RAW_NOTATION_NAMES_FILE
        customers_datalake=MasterCustomersPaths.INPUT_RAW_QUERY_CUSTOMERS_FILE


        fill_rate_update=MasterCustomersPaths.INPUT_RAW_UPDATE_FILL_RATE_DIR
        sales_update=MasterCustomersPaths.INPUT_RAW_UPDATE_SALES_DIR
        
        # --- LECTURA DE ARCHIVOS DE CONFIGURACIÓN ---
        df_customers_shared = pd.read_excel(customers_shared,sheet_name='Customers_Shared_by_Country', dtype=str, engine='openpyxl')
        print('read customers shared ')
        df_customers_clasifications = pd.read_excel(customers_shared,sheet_name='Clasifications', dtype=str, engine='openpyxl')
        print('read customers clasificario')
        df_country = pd.read_excel(country_code_file,
                                    sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
        print('read country')
        df_notation_customers=pd.read_excel(notation_customers_file, dtype=str, engine='openpyxl')
        print('notation ')
        df_customers_datalake=pd.read_excel(customers_datalake, dtype=str, engine='openpyxl')
        print('customers datalake')
        df_master=pd.read_excel(md_customers, dtype=str, engine='openpyxl')
        print('read md')
        df_fill_rate=read_files(fill_rate_update)
        print('read fr')
        df_sales=read_files_parquets(sales_update)
        print('read sales')
        # Unifico la informacion de clientes provenientes de datalake e informacion compartida por los paises
        df_customers_datalake.rename(columns={'Customer Name':'Sold-To Customer Name'},inplace=True)        
        df_customers_shared.rename(columns={'Country': 'fk_Country',
                                            'fk_Customer_Code': 'fk_Sold-To Customer Code',
                                            'Name_Customer':'Sold-To Customer Name',
                                            'Sold-To Dist Channel Shared':'fk_Dist_Channel'},inplace=True)
        
        df_all_customers=pd.concat([df_customers_shared, df_customers_datalake], ignore_index=True).drop_duplicates(['fk_Sold-To Customer Code','fk_Country'], keep='first')
        print('df_all_customers')
        
        # --- LECTURA Y CONSOLIDACIÓN DE DATOS DE ACTUALIZACIÓN --
        # consolido los clientes de fill rate y sales
        lst_columns_fr=['Country Code', 'Destination Country','Sold-To Customer Code','Sold-To Customer']
        lst_columns_sls=['fk_Country', 'fk_Sold_To_Customer_Code', 'Customer Name']

        df_fill_rate=df_fill_rate[lst_columns_fr]
        df_sales=df_sales[lst_columns_sls]

        df_fill_rate=asign_country_code(df_fill_rate, df_country)
        print('assign country')
        df_fill_rate.rename(columns={'Sold-To Customer Code': 'fk_Sold-To Customer Code',
                                    'Sold-To Customer':'Sold-To Customer Name'},inplace=True)
        print('rename col fr')
        df_sales.rename(columns={'Customer Name': 'Sold-To Customer Name',
                                 
                                 'fk_Sold_To_Customer_Code':'fk_Sold-To Customer Code'},inplace=True)
        print('rename col sales')
        
        lst_colums_md_customers=['fk_Country', 'fk_Sold-To Customer Code', 'Sold-To Customer Name']
        print('lst_colums_md_customers')
        df_fill_rate=df_fill_rate[lst_colums_md_customers]
        print('ok fill rate')
        print(df_fill_rate.columns)
        df_sales=df_sales[lst_colums_md_customers]
        print('ok sales')
        print(df_sales.columns)

        df_consolidated=pd.concat([df_fill_rate, df_sales], ignore_index=True).drop_duplicates(['fk_Sold-To Customer Code','fk_Country'], keep='first')
        print('df_consolidated')




        # Asigno clasificacion a todos los clientes, primero busco en datalake, si no asigno por regla establecida
        df_consolidated=assing_clasification(df_consolidated, df_all_customers, df_customers_clasifications)
        print('complete_clasification')
        
        df_update=update_excel_file(df_master,
                                    df_consolidated,
                                    name='master_customers')
        
        print('update_excel_file')

        
        df_update=notation_name(df_update,df_notation_customers)
        print('notation_name')
        
        df_update.to_excel(md_customers, index=False)
        print("Proceso de actualización de clientes completado exitosamente.")
        pass 
    except Exception as e:
        print(f"Error en procesamiento de datos de MD Customers: {e}")
        sys.exit(1)
if __name__ == "__main__":
    main()