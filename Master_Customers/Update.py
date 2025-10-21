#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
# Liberia
import pandas as pd
import numpy as np
# Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico.
import glob
import os

from Fill_Rate.Process_ETL.Process_Files import asign_country_code, read_files

def complete_clasification(df_consolidated, df_customers_shared, df_customers_clasifications, df_country):
    """
    Completa la clasificación de los clientes en el DataFrame df_consolidated.

    Args:
    df_consolidated (pd.DataFrame): DataFrame con la información de clientes nuevos.

    Returns:
    pd.DataFrame: DataFrame con la clasificación completada.
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
    Actualiza el master customer eliminando los registros viejos y
    concatenando los nuevos registros del dataframe de actualización.
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

import numpy as np
import pandas as pd # Asegúrate de importar pandas

def notation_customers(df_update, df_notation_customers):
    
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
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: MD CUSTOMERS UPDATE ETL ---")
    print("=" * 55)
    #--------------------------------------------------
    #---------------- RUTAS ---------------------------
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
if __name__ == "__main__":
    main()
    print("Proceso de actualización de clientes completado exitosamente ✅.")