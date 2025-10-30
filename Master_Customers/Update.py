"""
Módulo principal de actualización y consolidación para el Maestro de Clientes (Master Customers).
Su objetivo es generar una tabla de dimensión actualizada, integrando los nuevos códigos de cliente
de las actualizaciones de Fill Rate y Sales, asignando su clasificación de canal y tipo de distribución
mediante lógica de negocio compleja y tablas de referencia.
El resultado final es un archivo Excel maestro actualizado (Upsert).
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

from Fill_Rate.Process_ETL.Process_Files import asign_country_code, read_files

def complete_clasification(df_consolidated, df_customers_shared, df_customers_clasifications, df_country):
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
    df_consolidated = asign_country_code(df_consolidated, df_country)
    df_consolidated['code_customer'] = (
                                    # . Aplicamos el slicing a la columna original (con .str[2:] para cada elemento)
                                df_consolidated['Sold-To Customer Code'].str[3:]
                                .where(
                                    # La condición: ¿El valor en esa celda contiene '/'?
                                    df_consolidated['Sold-To Customer Code'].str.contains('/'),
                                    # El 'otro' valor: Mantener el valor original de la columna
                                    other=df_consolidated['Sold-To Customer Code']
                                    )
                                    # se quitan los ceros iniciales
                                    .str.lstrip('0')
                                )
    

    # Crea las fk para relacionar info de clientes compartidos con los nuevos clientes
    df_customers_shared['fk_country_customer'] = df_customers_shared['Country'].astype(str) + '-' + df_customers_shared['fk_Customer_Code'].astype(str) 
    df_customers_shared['fk_country_customer'] = df_customers_shared['fk_country_customer'].str.upper().str.strip().str.replace(' ', '')
    
   
    df_consolidated['fk_country_customer'] = df_consolidated['fk_Country'] + '-' + df_consolidated['code_customer'].astype(str)    
    df_consolidated['fk_country_customer'] = df_consolidated['fk_country_customer'].str.upper().str.strip().str.replace(' ', '')

    df_customers_clasifications['fk_channel']=df_customers_clasifications['pk_Sold-To Dist Channel']
    df_customers_clasifications['fk_channel'] = df_customers_clasifications['fk_channel'].astype(str).str.upper().str.strip().str.replace(' ', '')
   
    df_consolidated=pd.merge(df_consolidated,
             df_customers_shared[['fk_country_customer', 'Sold-To Dist Channel Shared']],
             how='left',
             on='fk_country_customer')
    df_consolidated['Sold-To Dist Channel Shared']=df_consolidated['Sold-To Dist Channel Shared'].fillna('NOTFOUND')
    df_consolidated['Sold-To Dist Channel Shared'] = df_consolidated['Sold-To Dist Channel Shared'].str.upper().str.strip().str.replace(' ', '')
   
  
    condicion_not_found = df_consolidated['Sold-To Dist Channel Shared'].str.contains('NOTFOUND|NOT', regex=True)
    df_consolidated['Sold-To Dist Channel Shared']=np.where(~condicion_not_found,
                                                             df_consolidated['Sold-To Dist Channel Shared'],
                                                             df_consolidated['Sold-To Dist Channel'])
    df_consolidated['Sold-To Dist Channel Shared'] = df_consolidated['Sold-To Dist Channel Shared'].str.upper().str.strip().str.replace(' ', '')

    diccionario_map = {'MESSMERCHANT':'MASSMERCHANT'}
    df_consolidated['Sold-To Dist Channel Shared']=df_consolidated['Sold-To Dist Channel Shared'].replace(diccionario_map)
   

    channel_map=list(set(df_customers_clasifications['fk_channel'].unique())) 
    # condiciones para completar la clasificación de los clientes
    es_colombia = (df_consolidated['fk_Country'] == 'COLOMBIA')
    canal_no_mapeado = (~df_consolidated['Sold-To Dist Channel Shared'].isin(channel_map))
    
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
    df_consolidated['Sold-To Dist Channel Shared'], # Condición 2: COLOMBIA y SÍ en subcanal_map (usa el valor actual)
    'TRADITIONALHARDWARESTORES', # Condición 3: OTRO país y NO en subcanal_map
    df_consolidated['Sold-To Dist Channel Shared'] # Condición 4: OTRO país y SÍ en subcanal_map (usa el valor actual)
    ]

    df_consolidated['fk_Dist_Channel'] = np.select(
    condiciones, 
    valores, 
    default=df_consolidated['Sold-To Dist Channel Shared'] # Si ninguna condición aplica (por seguridad)
    )

    
    df_consolidated=pd.merge(df_consolidated,
             df_customers_clasifications[['fk_channel','pk_Sold-To Dist Channel','fk_Sold-To Dist Type']],
             how='left',
             left_on='fk_Dist_Channel',
             right_on='fk_channel'
             )
   
    
    # delete duplicates
    df_consolidated = df_consolidated.drop_duplicates(subset=['fk_country_customer'])
   
    lst_columns=['fk_Country', 'Sold-To Customer Code', 'Sold-To Customer',
    'pk_Sold-To Dist Channel', 'fk_Sold-To Dist Type','fk_country_customer']
    df_consolidated=df_consolidated[lst_columns]
    
    df_consolidated.rename(columns={
    'fk_Country': 'fk_Country',
    'Sold-To Customer Code': 'fk_Sold-To Customer',
    'Sold-To Customer': 'Sold-To Customer Name',
    'pk_Sold-To Dist Channel': 'fk_Dist_Channel',
    'fk_Sold-To Dist Type': 'fk_Dist_Type'
    }, inplace=True)
    print('todo ok')

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
        df_master['fk_country_customer']=df_master['fk_Country']+'-'+df_master['fk_Sold-To Customer']
        df_master['fk_country_customer']=df_master['fk_country_customer'].str.upper().str.strip().str.replace(' ',  '')
        df_consolidated['fk_country_customer']=df_consolidated['fk_Country']+'-'+df_consolidated['fk_Sold-To Customer']
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

def notation_customers(df_update, df_notation_customers):
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
    #  Preparación de llaves 
    # Se crea la columna 'fk_customer' en ambos DataFrames, con la misma limpieza.
    df_notation_customers['fk_customer'] = df_notation_customers['Text Condition'].str.upper().str.strip().str.replace(' ', '')
    df_update['fk_customer'] = df_update['Sold-To Customer Name'].str.upper().str.strip().str.replace(' ', '')
    
    # Crear un diccionario de mapeo
    mapeo_clientes = df_notation_customers.set_index('fk_customer')['Result'].to_dict()
    
    # Aplicar el mapeo para obtener los NUEVOS NOMBRES
    # Creamos una columna temporal con los nuevos nombres si se encuentran en el mapeo.
    # Si un cliente no se encuentra en el diccionario 'mapeo_clientes', 
    # Pandas devuelve NaN (Not a Number).
    df_update['Nuevo Nombre'] = df_update['fk_customer'].map(mapeo_clientes)

    # Actualizar la columna 'Sold-To Customer Name'
    # Usamos .fillna() para reemplazar los valores NaN (clientes NO mapeados) 
    # con su nombre original.
    # Así, solo se actualizan los clientes que SÍ tenían un error de notación.
    df_update['Sold-To Customer Name'] = df_update['Nuevo Nombre'].fillna(
        df_update['Sold-To Customer Name']
    )
    
    # Opcional: Eliminar la columna temporal
    df_update.drop(columns=['Nuevo Nombre', 'fk_customer'], inplace=True)
    
    return df_update

def main():
    """	
    Función principal que orquesta el proceso ETL para actualizar el Maestro de Clientes.	
    El proceso incluye:
        1) Consolidación de datos de actualización de Fill Rate y Sales.	
        2) Asignación de clasificaciones de cliente. 3) Aplicación de la lógica de Upsert al maestro histórico.	
        4) Corrección de notación de nombres. 5) Guardado final en el archivo Excel maestro.	
    
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
        md_customers=MasterCustomersPaths.OUTPUT_FILE_PROCESSED_MASTER_CUSTOMERS_FILE_PRUEBA
        notation_customers_file=MasterCustomersPaths.INPUT_RAW_NOTATION_NAMES_FILE
        
        fill_rate_update=MasterCustomersPaths.INPUT_RAW_UPDATE_FILL_RATE_DIR
        sales_update=MasterCustomersPaths.INPUT_RAW_UPDATE_SALES_DIR
        
        # --- LECTURA DE ARCHIVOS DE CONFIGURACIÓN ---
        df_customers_shared = pd.read_excel(customers_shared,sheet_name='Customers_Shared_by_Country', dtype=str, engine='openpyxl')
        df_customers_clasifications = pd.read_excel(customers_shared,sheet_name='Clasifications', dtype=str, engine='openpyxl')
        df_country = pd.read_excel(country_code_file,
                                    sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
        df_notation_customers=pd.read_excel(notation_customers_file, dtype=str, engine='openpyxl')
        df_fill_rate=read_files(fill_rate_update)
        df_sales=read_files(sales_update)
        df_master=pd.read_excel(md_customers, dtype=str, engine='openpyxl')

        # --- LECTURA Y CONSOLIDACIÓN DE DATOS DE ACTUALIZACIÓN --
        lst_columns=['Country Code', 'Destination Country','Sold-To Customer Code','Sold-To Customer','Sold-To Dist Channel']
        
        df_fill_rate=df_fill_rate[lst_columns]
        df_sales=df_sales[lst_columns]
        
        df_consolidated=pd.concat([df_fill_rate, df_sales], ignore_index=True)
        df_consolidated=complete_clasification(df_consolidated,
                                            df_customers_shared,
                                            df_customers_clasifications,
                                            df_country)
        df_update=update_excel_file(df_master,
                                    df_consolidated,
                                    name='master_customers')
        df_update=notation_customers(df_update,df_notation_customers)
        df_update.to_excel(md_customers, index=False)
        print("Proceso de actualización de clientes completado exitosamente.")
        pass 
    except Exception as e:
        print(f"Error en procesamiento de datos de MD Customers: {e}")
        sys.exit(1)
if __name__ == "__main__":
    main()