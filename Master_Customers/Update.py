# leer los archivos update de demand fil rate y sales
# realide cada uno de estos qobtener la informaicon de custome
#crear un dataframe unificado 
# crear un archivo  donde esten los nuevos clientes suministrdos por cada pais
# #asignar la correcta notacion de clasificacion (traudccion)
# hacer un merge entre el df unificado y el df (con info compartida) para asignar clasificacion
# aquelos que quede sin info
    # resto paises
        #verificar si el cliente ya trae info y dejar esa, si no asignar a tradicional
   # colombia
    #verificar si ya trae info y dejar eso si no asignar showrroms
# concatenar el df con master customer 




#--------------------------------------------------
#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
# Liberia
import pandas as pd
import numpy as np
# Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico.
import glob
import os


from Fill_Rate.Process_ETL.Process_Files import asign_country_code, read_files
def complete_clasification(df_consolidated,df_customers_shared,df_customers_clasifications,df_country):
   """
    Completa la clasificación de los clientes en el DataFrame df_consolidated.

    Args:
    df_consolidated (pd.DataFrame): DataFrame con la información de clientes nuevos.

    Returns:
    pd.DataFrame: DataFrame con la clasificación completada.
    """
    # Crea la fk para relacionar info de clientes compartidos con los nuevos clientes  
   df_customers_shared['fk_country_customer']=df_customers_shared['Country'].astype(str)+'-'+df_customers_shared['fk_Customer_Code'].astype(str) 
   df_customers_shared['fk_country_customer']=df_customers_shared['fk_country_customer'].str.upper().str.strip().str.replace(' ', '')
     
   # . Obtener las Series de valores únicos
   Channels_values = df_customers_clasifications['pk_Sold-To Dist Channel'].str.upper().str.strip().str.replace(' ', '').unique()
   canal_map = list(set(Channels_values))
     

   df_consolidated=asign_country_code(df_consolidated, df_country)
   df_consolidated['fk_country_customer']=df_consolidated['fk_Country']+'-'+df_consolidated['Sold-To Customer Code']
   df_consolidated['fk_country_customer']=df_consolidated['fk_country_customer'].str.upper().str.strip().str.replace(' ', '')

   df_consolidated=pd.merge(df_consolidated,
   df_customers_shared[['fk_country_customer','Sold-To Dist Channel Shared']],
   how='left',
   on='fk_country_customer')
   df_consolidated['Sold-To Dist Channel Shared'] = df_consolidated['Sold-To Dist Channel Shared'].filna('NOT FOUND')
   df_consolidated['Sold-To Dist Channel Shared'] = df_consolidated['Sold-To Dist Channel Shared'].str.upper().str.strip().str.replace(' ', '')
    
   condiciones1 = [
   #canal no mapeado
   df_consolidated['Sold-To Dist Channel Shared'].isin(canal_map),
   ]

   valores1=[
   df_consolidated['Sold-To Dist Channel Shared'] # Si el canal está mapeado, se usa el valor actual
   ]
   df_consolidated['Sold-To Dist Channel1'] = np.select(condiciones1, valores1, default=df_consolidated['Sold-To Dist Channel'])
   df_consolidated['Sold-To Dist Channel1'] = df_consolidated['Sold-To Dist Channel1'].str.upper().str.strip().str.replace(' ', '')

   # condiciones para completar la clasificación de los clientes
   es_colombia = (df_consolidated['fk_Country'] == 'COLOMBIA')
   canal_no_mapeado = (~df_consolidated['Sold-To Dist Channel1'].isin(canal_map))

   # Definir las asignaciones (las opciones que se pueden aplicar)
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

   # 3. Definir los valores a asignar para cada condición
   valores = [
   'SHOWROOMS',# Condición 1: COLOMBIA y NO en subcanal_map
   df_consolidated['Sold-To Dist Channel1'], # Condición 2: COLOMBIA y SÍ en subcanal_map (usa el valor actual)
   'TRADITIONAL HARDWARE STORES', # Condición 3: OTRO país y NO en subcanal_map
   df_consolidated['Sold-To Dist Channel1'] # Condición 4: OTRO país y SÍ en subcanal_map (usa el valor actual)
   ]

   # Usar np.select para aplicar todas las condiciones de una vez
    
   df_consolidated['fk_Dist_Channel'] = np.select(
   condiciones, 
   valores, 
   default=df_consolidated['Sold-To Dist Channel1'] # Si ninguna condición aplica (por seguridad)
   )
   df_consolidated['fk_Dist_Channel'] = df_consolidated['fk_Dist_Channel'].str.upper().str.strip().str.replace(' ', '')

   df_customers_clasifications['fk_Dist_Channel']=df_customers_clasifications['pk_Sold-To Dist Channel'].str.upper().str.strip().str.replace(' ', '')
    
   df_consolidated=pd.merge(df_consolidated,
   df_customers_clasifications[['fk_Dist_Channel','pk_Sold-To Dist Channel','fk_Sold-To Dist Type']],
   how='left',
   on='fk_Dist_Channel'
   )

   df_consolidated[['Price_Segment', 'Cust_Group']]='-'
   lst_columns=['fk_Country', 'Sold-To Customer Code', 'Sold-To Customer',
   'pk_Sold-To Dist Channel', 'fk_Sold-To Dist Type', 'Price_Segment', 'Cust_Group']
   df_consolidated=df_consolidated[lst_columns]
   df_consolidated.rename(columns={
   'fk_Country': 'fk_Country',
   'Sold-To Customer Code': 'fk_Sold-To Customer',
   'Sold-To Customer': 'Sold-To Customer Name',
   'pk_Sold-To Dist Channel': 'fk_Dist_Channel',
   'fk_Sold-To Dist Type': 'fk_Dist_Type'
   }, inplace=True)
    
   return df_consolidated

def main():

    """
    Función principal para ejecutar el proceso de actualización de clientes.
    """
    #--------------------------------------------------
    #---------------- RUTAS ---------------------------
    path_country=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Shared_Information_for_Projects\Country\Region_Country_codes.xlsx'
    path_customer_shared=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Customers\Clasifications_Customers.xlsx'
    path_customers=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Master_Customers\Master_Customers.xlsx'
   
    path_fill_rate=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Fil Rate\Mothly_Update'
    path_sales=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Sales\Mothly_Update'
    path_prueba=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Master_Customers\prueba.xlsx'

    # --- LECTURA DE ARCHIVOS DE CONFIGURACIÓN ---
    df_customers_shared = pd.read_excel(path_customer_shared,sheet_name='Customers_Shared_by_Country', dtype=str, engine='openpyxl')
    df_customers_clasifications = pd.read_excel(path_customer_shared,sheet_name='Clasifications', dtype=str, engine='openpyxl')
    df_country = pd.read_excel(path_country,
                               sheet_name='Code Country Fillrate-Sales', dtype=str, engine='openpyxl')
    
    # Limpiar DataFrames de configuración una sola vez
    for col in df_country.columns:
        df_country[col] = df_country[col].astype(str).str.upper().str.strip()
    
    #df_country = df_country.drop_duplicates(subset=['Columna_Clave_De_Pais_Usada_En_Map'], keep='first')

    # --- LECTURA Y CONSOLIDACIÓN DE DATOS DE ACTUALIZACIÓN ---
    df_fill_rate=read_files(path_fill_rate)
    print( df_fill_rate.columns())
    df_sales=read_files(path_sales)  
    df_fill_rate = df_fill_rate[['Country Code', 'Destination Country', 'Sold-To Customer Code', 'Sold-To Customer', 'Sold-To Dist Channel']]
    df_sales = df_sales[['Country Code', 'Destination Country', 'Sold-To Customer Code', 'Sold-To Customer', 'Sold-To Dist Channel']]
  
    df_consolidated = pd.concat([df_fill_rate, df_sales], ignore_index=True)
    print(df_consolidated.head())
    # --- PROCESAMIENTO Y CLASIFICACIÓN DE CLIENTES ---
    #df_consolidated = complete_clasification(df_consolidated,df_customers_shared,df_customers_clasifications,df_country)
    '''
    # Leer el DataFrame de clientes maestros
    df_master_customers = pd.read_excel(path_customers, dtype=str, engine='openpyxl')
    
    # Concatenar el DataFrame consolidado con el DataFrame de clientes maestros
    df_final = pd.concat([df_master_customers, df_consolidated], ignore_index=True)
    '''

    # Guardar el DataFrame final en un archivo Excel
    #df_final.to_excel(path_customers, index=False)
    #df_consolidated.to_excel(path_prueba)


if __name__ == "__main__":
    main()
    print("Proceso de actualización de clientes completado exitosamente.")



