
"""
DATAMIND: Orquestador de Procesamiento de Datos de Ventas Semanales.

Este módulo es el encargado de consolidar, limpiar y estandarizar los datos de ventas
semanales provenientes de la plataforma Datamind para Argentina, México y Chile.
Su misión es transformar los reportes crudos en una estructura uniforme y lista
para el análisis, asegurando la consistencia en marcas y canales de venta.

¿Qué hace especial a este proceso?
 1. CONSOLIDACIÓN MULTIPAÍS: Lee y une archivos Excel de diferentes países en un
    único DataFrame, estandarizando nombres de columnas y asignando el país de origen.
 2. LIMPIEZA Y NORMALIZACIÓN: Convierte tipos de datos, maneja valores nulos y
    estandariza cadenas de texto (minúsculas, sin espacios).
 3. ASIGNACIÓN INTELIGENTE DE MARCAS: Utiliza un mapeo predefinido para corregir
    variaciones en los nombres de marcas y consolidarlas bajo un estándar único.
 4. CLASIFICACIÓN DE CANALES DE VENTA: Identifica y categoriza automáticamente
    los canales de venta como 'ecommerce' o 'store' basándose en patrones de texto.
 5. LÓGICA ESPECÍFICA DE RETAILER (The Home Depot): Implementa una lógica compleja
    para separar y ajustar las ventas de e-commerce y tienda física para The Home Depot
    en México, evitando duplicidades y asegurando la precisión de los datos.

💡 Nota Senior: Este módulo es crucial para la fiabilidad de los reportes de ventas
semanales, ya que unifica la información de múltiples fuentes y aplica reglas de
negocio específicas antes de la ingesta final.
"""

# Librerias
import pandas as pd
import numpy as np
import glob
import os
import sys
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from pathlib import Path
from Fill_Rate.Process_ETL.Process_Files import clean_sku
from Master_Products.Update_md_products import create_inverse_brand_map,BRAND_STANDARD_MAP


#=========================
#--- DATAMIND
#=========================
def consolidate_files(path_arg,path_mx,path_ch):
    r"""
    Consolida los archivos de ventas semanales de Datamind para Argentina, México y Chile.

    Lee los archivos Excel de cada país, renombra columnas para estandarización,
    asigna el país de origen a cada registro, concatena los DataFrames resultantes,
    maneja valores nulos en la columna 'Year-Week', normaliza columnas de texto
    y convierte columnas numéricas a tipo flotante.

    Args:
        path_arg (str): Ruta al archivo Excel de Datamind para Argentina.
        path_mx (str): Ruta al archivo Excel de Datamind para México.
        path_ch (str): Ruta al archivo Excel de Datamind para Chile.

    Returns:
        pd.DataFrame: DataFrame consolidado con los datos de ventas semanales de los tres países.
    """

    #r""" 
   #mex
    print('leyendo mexico')
    r1=r'C:\Users\SSN0609\Downloads\Venta-SemanalMEX23.csv'
    r2=r'C:\Users\SSN0609\Downloads\Venta-SemanalMEX24.csv'
    r3=r'C:\Users\SSN0609\Downloads\Venta-SemanalMEX25.csv'
    r4=r'C:\Users\SSN0609\Downloads\Venta-SemanalMEX26.csv'
    df23=pd.read_csv(r1,dtype=str)
    df24=pd.read_csv(r2,dtype=str)
    df25=pd.read_csv(r3,dtype=str)
    df26=pd.read_csv(r4,dtype=str)

    df_mx=pd.concat([df23,df24,df25,df26])
    #arg
    print('leyendo argentina')
    ruta23arg=r'C:\Users\SSN0609\Downloads\Venta-SemanalARG23.csv'
    ruta24arg=r'C:\Users\SSN0609\Downloads\Venta-SemanalARG24.csv'
    ruta25arg=r'C:\Users\SSN0609\Downloads\Venta-SemanalARG25.csv'
    ruta26arg=r'C:\Users\SSN0609\Downloads\Venta-SemanalARG26.csv'
    df23arg=pd.read_csv(ruta23arg, dtype=str)
    df24arg=pd.read_csv(ruta24arg, dtype=str)
    df25arg=pd.read_csv(ruta25arg, dtype=str)
    df26arg=pd.read_csv(ruta26arg, dtype=str)
    df_arg=pd.concat([df23arg,df24arg,df25arg,df26arg])
    #chi
    print('leyendo chile')
    ruta23ch=r'C:\Users\SSN0609\Downloads\Venta-SemanalCHI23.csv'
    ruta24ch=r'C:\Users\SSN0609\Downloads\Venta-SemanalCHI24.csv'
    ruta25ch=r'C:\Users\SSN0609\Downloads\Venta-SemanalCHI25.csv'
    ruta26ch=r'C:\Users\SSN0609\Downloads\Venta-SemanalCHI26.csv'
    df23ch=pd.read_csv(ruta23ch, dtype=str)
    df24ch=pd.read_csv(ruta24ch, dtype=str)
    df25ch=pd.read_csv(ruta25ch, dtype=str)
    df26ch=pd.read_csv(ruta26ch, dtype=str)
    df_ch=pd.concat([df23ch,df24ch,df25ch,df26ch])

    #"""


    #df_arg = pd.read_excel(path_arg, header=0,dtype=str)
    #df_mx = pd.read_excel(path_mx, header=0,dtype=str)
    #df_ch = pd.read_excel(path_ch, header=0,dtype=str)
    
    
    df_ch.rename(columns={'Semana':'Year-Week'},inplace=True)
    df_arg.rename(columns={'Semana':'Year-Week'},inplace=True)
    df_mx.rename(columns={'Semana':'Year-Week'},inplace=True)

    df_arg['Country'] = df_arg['(L) Retailer'].apply(lambda x: 'Uruguay' if 'Uruguay' in x else 'Argentina')
    df_mx['Country']='Mexico'
    df_ch['Country']='Chile'

    df_mx.rename(columns={'(L) Cadena':'(L) Retailer',
                                  '(L) Tienda':'(L) Local',
                                  '(I) Brand':'(I) MARCA'
                                  },inplace=True) # Se eliminó la reasignación 'df_mx =' ya que inplace=True modifica el DataFrame directamente y retorna None.

    columns_select=['Year-Week',
        'Country',
        '(L) Retailer',
        '(L) Local',
        '(I) MARCA',
        '(E) Marca',
        '(I) Producto Interno',
        '(I) Código Producto Interno',
        'Venta neta',
        'Venta bruta',
        'Venta costo',
        'Unidades vendidas',
        'Precio Publico Estimado',
        ]
    
    df_datamind_week=pd.concat([df_arg[columns_select],df_mx[columns_select],df_ch[columns_select]], ignore_index=True)
    #--- BRAND
    df_datamind_week[['(I) MARCA','(E) Marca']]=df_datamind_week[['(I) MARCA','(E) Marca']].fillna('other')
    df_datamind_week['(I) MARCA']=np.where(df_datamind_week['(I) MARCA']=='other',df_datamind_week['(E) Marca'],df_datamind_week['(I) MARCA'])

    #----RENAME
    df_datamind_week.rename(columns={
        '(I) Producto Interno':'Sku Description',
        '(I) Código Producto Interno':'SKU',
        '(L) Retailer':'Retailer',
        '(L) Local':'Local',
         '(I) MARCA':'Brand'},inplace=True)

    df_datamind_week.dropna(subset=['Year-Week'],inplace=True)

    # --- ESTANDARIZACIÓN DE TIPOS NUMÉRICOS (Métricas) ---
    # Es crítico convertir a float antes de las lógicas de THD y Coppel para evitar inconsistencias
    metrics = ['Venta neta', 'Venta bruta', 'Venta costo', 'Unidades vendidas', 'Precio Publico Estimado']
    for col in metrics:
        if col in df_datamind_week.columns:
            # Limpieza de comas y conversión a numérico
            df_datamind_week[col] = pd.to_numeric(df_datamind_week[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0)

    #----DATE
    # Convertir 'Year-Week' a objetos datetime (asumiendo formato YYYY-WW)
    df_datamind_week['Date'] = pd.to_datetime(df_datamind_week['Year-Week'].astype(str) + '-7', format='%G%V-%u')
    df_datamind_week['Year-Month'] = df_datamind_week['Date'].dt.strftime('%Y-%m')
    
    return df_datamind_week

def assing_sales_channel(df_datamind_week):
    """
    Asigna el canal de venta ('ecommerce' o 'store') a cada registro del DataFrame.

    Utiliza patrones de texto en las columnas '(L) Retailer' y '(L) Local' para
    identificar ventas de comercio electrónico. Si no se identifica como e-commerce,
    se clasifica como 'store'.

    Args:
        df_datamind_week (pd.DataFrame): DataFrame con las columnas '(L) Retailer' y '(L) Local'.
    Returns:
        pd.DataFrame: DataFrame con la nueva columna 'canal_venta' clasificada.
    """
    
    lst_canal = 'internet|online|distancia|digital|virtual|ecommerce|e-com'
    lst_retailer = 'mercado libre|e-comm|ecommerce|mercadolibre|amazon'

    mask_retailer = df_datamind_week['Retailer'].str.lower().str.contains(lst_retailer)
    mask_canal = df_datamind_week['Local'].str.lower().str.contains(lst_canal)
    mask_ecom= (mask_retailer|mask_canal)

    # Inicializar la columna 'canal_venta' con un valor por defecto
    df_datamind_week['canal_venta'] = 'traditional channel'
    df_datamind_week.loc[mask_ecom, 'canal_venta'] = 'ecommerce'

    df_datamind_week['canal_venta'] = df_datamind_week['canal_venta'].replace(['vacio','moderno','tradicional','tienda'], 'traditional channel')
    df_datamind_week['canal_venta'] = df_datamind_week['canal_venta'].replace(['ecommerce','e-commerce'], 'ecommerce')
    # Asegurar la consistencia de los nombres de canal
    df_datamind_week['canal_venta'] = df_datamind_week['canal_venta'].replace(['e-commerce'], 'ecommerce')

    return df_datamind_week

def solution_thd(df_datamind_week):
    """
    Aplica una lógica específica para The Home Depot (THD) en México.

    Esta función separa las ventas de THD en México en e-commerce y tienda física.
    Luego, agrupa las ventas de e-commerce y las resta de las ventas de tienda física
    para evitar la doble contabilización, ya que las ventas de e-commerce a menudo
    se reportan también en las tiendas físicas. Finalmente, consolida los datos ajustados.

    Args:
        df_datamind_week (pd.DataFrame): DataFrame consolidado de ventas semanales.
    Returns:
        pd.DataFrame: DataFrame con las ventas de THD en México ajustadas.
    """
    import pandas as pd
    import numpy as np

    #  Filtrado inicial
    df_thd = df_datamind_week.loc[
        (df_datamind_week['Country'] == 'Mexico') & 
        (df_datamind_week['Retailer'].isin(['the home depot', 'the home depot e-comm']))
        ].copy()
    df_datamind_without_thd = df_datamind_week.loc[
        (df_datamind_week['Country'] != 'Mexico') |
        ~(df_datamind_week['Retailer'].isin(['the home depot', 'the home depot e-comm']))
        ].copy()

    #  Definimos las columnas que forman nuestra "llave" de identidad
    llaves = ['Year-Week', 'Local', 'SKU']

    #  Separamos y agrupamos E-commerce para evitar duplicados y sumar sus montos
    # 💡 Ajustado a 'ecommerce' para coincidir con la estandarización previa
    df_ecom = df_thd[df_thd['canal_venta'] == 'ecommerce'].groupby(llaves)[
        [
        'Venta neta',
        'Venta bruta',
        'Venta costo',
        'Unidades vendidas',
        'Precio Publico Estimado',]].sum().reset_index()
    df_ecom.rename(columns={
        'Venta neta': 'Venta_ecom',
        'Venta bruta': 'Venta_bruta_ecom',
        'Venta costo': 'Venta_costo_ecom',
        'Unidades vendidas': 'Unidades_vendidas_ecom',
        'Precio Publico Estimado': 'Precio_Publico_Estimado_ecom',
        }, inplace=True)

    # Separamos las ventas de tienda física
    df_store = df_thd[df_thd['canal_venta'] == 'store'].copy()

    #  Cruzamos la información (Merge)
    # Esto trae la venta de e-commerce a la fila de la tienda física correspondiente
    df_store = df_store.merge(df_ecom, on=llaves, how='left')

    #  Realizamos la resta masiva
    # fillna(0) asegura que si no hubo venta e-comm, reste cero en lugar de dar Error/NaN
    df_store['Venta neta'] = df_store['Venta neta'] - df_store['Venta_ecom'].fillna(0)
    df_store['Venta bruta'] = df_store['Venta bruta'] - df_store['Venta_bruta_ecom'].fillna(0)
    df_store['Venta costo'] = df_store['Venta costo'] - df_store['Venta_costo_ecom'].fillna(0)
    df_store['Unidades vendidas'] = df_store['Unidades vendidas'] - df_store['Unidades_vendidas_ecom'].fillna(0)
    df_store['Precio Publico Estimado'] = df_store['Precio Publico Estimado'] - df_store['Precio_Publico_Estimado_ecom'].fillna(0)


    # Quitamos la columna auxiliar de la resta antes de unir
    df_store.drop(columns=['Venta_ecom', 'Venta_bruta_ecom', 'Venta_costo_ecom', 'Unidades_vendidas_ecom', 'Precio_Publico_Estimado_ecom'], inplace=True)
    df_ecom.rename(columns={
            'Venta_ecom': 'Venta neta',
            'Venta_bruta_ecom': 'Venta bruta',
            'Venta_costo_ecom': 'Venta costo',
            'Unidades_vendidas_ecom': 'Unidades vendidas',
            'Precio_Publico_Estimado_ecom': 'Precio Publico Estimado',
            }, inplace=True)
    
    df_final = pd.concat([df_store, df_ecom],ignore_index=True)
    df_datamind_week = pd.concat([df_datamind_without_thd, df_final],ignore_index=True)
    return df_datamind_week

def solution_coppel(df_datamind_week, path_coppel):
    # Lectura archvio precios coppel
    df_precios = pd.read_excel(path_coppel, usecols=['Year', 'Producto', 'Precio Publico'])
    
    df_precios['year_producto'] = (
        df_precios['Year'].astype(str) + '-' + df_precios['Producto'].astype(str)
    ).str.lower().str.strip()
    
    # 
    df_precios['Precio Publico'] = (
        df_precios['Precio Publico']
        .replace({',': ''}, regex=True)
        .astype(float)
    )
    df_precios = df_precios.drop_duplicates('year_producto')


    # 2. Separar Coppel del resto de los datos
    mask_coppel = df_datamind_week['Retailer'].str.lower().str.strip().str.contains('coppel', case=False, na=False)
    df_coppel = df_datamind_week[mask_coppel].copy()
    df_otros = df_datamind_week[~mask_coppel].copy() # El resto de los retailers

    # 3. Procesar Coppel
    df_coppel['year_producto'] = (
        df_coppel['Year-Week'].str[:4] + '-' + df_coppel['SKU'].astype(str)
    ).str.lower().str.strip()

    df_coppel = df_coppel.merge(
        df_precios[['year_producto', 'Precio Publico']], 
        on='year_producto', 
        how='left'
    ).fillna({'Precio Publico': 0})

    df_coppel['Venta neta'] = df_coppel['Unidades vendidas'].astype(float).fillna(0) * df_coppel['Precio Publico'].astype(float).fillna(0)
    df_coppel.drop(columns=['year_producto'], inplace=True)

    # 4. Reintegrar los datos (el concat que faltaba)
    df_final = pd.concat([df_otros, df_coppel], ignore_index=True)

    # Retornamos el DataFrame completo actualizado
    return df_final

def nsv_usd(df_datamind_week, path_fx):
    #  Leer archivo FX optimizando tipos de datos
    df_fx = pd.read_excel(path_fx, dtype={"Country": str, "Adjusted Rate": float})

    #  Convertir fecha correctamente y crear clave única
    df_fx["Year-Month"] = pd.to_datetime(df_fx["Date"]).dt.strftime("%Y-%m")
    df_fx["key"] = (df_fx["Year-Month"] + df_fx["Country"]).str.upper().str.strip()

    # Crear clave en el dataframe principal de forma vectorizada
    df_datamind_week["key"] = (
        (df_datamind_week["Year-Month"] + df_datamind_week["Country"])
        .str.upper()
        .str.strip()
    )
    # Realizar el cruce de datos (Merge)
    df_datamind_week = pd.merge(
        df_datamind_week, df_fx[["key", "Adjusted Rate"]], on="key", how="left"
    )

    # Asegurar tipos numéricos y prevenir divisiones por cero
    df_datamind_week["Adjusted Rate"] = (
        df_datamind_week["Adjusted Rate"].astype("float32").fillna(0)
    )

    df_datamind_week=df_datamind_week.rename(columns={'Venta neta':'Venta neta Local'})
    #Calcular NSV en USD manejando errores de división
    df_datamind_week["Venta neta"] = (df_datamind_week["Venta neta Local"].astype("float32") /
                     df_datamind_week["Adjusted Rate"].replace(0, float("nan")))
    df_datamind_week["Venta neta"] = df_datamind_week["Venta neta"].fillna(0)

    # 7. Limpiar columnas temporales
    df_datamind_week = df_datamind_week.drop(columns=["Adjusted Rate", "key","Venta neta Local"])

    return df_datamind_week



#======================
# --- MERCADO LIBRE
#======================
def group_meli(path_meli_sales):
    lst_files_meli = []
    all_files_meli = path_meli_sales.glob("*meli_sbd*.xlsx")

    # Se anexan los archivos en una sola lista

    for filename in all_files_meli:
        df = pd.read_excel(filename, index_col=None, header=0,dtype=str)
        lst_files_meli.append(df)
    df_meli_sales = pd.concat(lst_files_meli, axis=0, ignore_index=True)
    return df_meli_sales

def meli(df_meli_sales, path_meli_sku,lst_columns):

    df_meli_sku=pd.read_excel(path_meli_sku, dtype=str)

    # Estandarizar tipos de datos para la columna clave antes del merge
    df_meli_sales['ML_ID'] = df_meli_sales['ML_ID'].astype(str).str.strip()
    df_meli_sku['MELI_ID'] = df_meli_sku['MELI_ID'].astype(str).str.strip()

    df_meli_sales=df_meli_sales[['YearWeek','COUNTRY','marca','MODEL','ML_ID','TGMV_USD','PRODUCT_NAME', 'TSI_FCST']].copy()
    df_meli_sales.rename(columns={ 'YearWeek':'Year-Week',
                                   'COUNTRY':'Country',
                                   'marca':'Brand',
                                    'ML_ID':'MELI_ID',
                                   'TGMV_USD':'Venta bruta',
                                   'TSI_FCST':'Unidades vendidas',
                                   'PRODUCT_NAME':'Sku Description'},inplace=True)
    df_meli_sales=pd.merge(df_meli_sales,
                           df_meli_sku[['MELI_ID','MODEL_NEW']].drop_duplicates('MELI_ID'),
                           on='MELI_ID',how='left')
    df_meli_sales.rename(columns={'MODEL_NEW':'SKU'},inplace=True)
    df_meli_sales['Country']=df_meli_sales['Country'].str.lower().str.strip()
    df_meli_sales['Country'] = df_meli_sales['Country'].replace('méxico', 'mexico')


    # Asegurar que todas las métricas sean numéricas (float) de forma consistente
    metrics = [ 'Venta bruta', 'Venta costo', 'Unidades vendidas', 'Precio Publico Estimado']
    for col in metrics:
        if col in df_meli_sales.columns:
            df_meli_sales[col] = pd.to_numeric(df_meli_sales[col], errors='coerce').fillna(0.0)
    
    # Si Venta neta no existe o es 0, usamos Venta bruta para MELI
    df_meli_sales['Venta neta'] = df_meli_sales['Venta bruta']

    # Aseguramos que Year-Week sea string antes de concatenar el "-7"
    df_meli_sales['Date'] = pd.to_datetime(df_meli_sales['Year-Week'].astype(str) + "-7", format="%G%V-%u")
    df_meli_sales['Year-Month'] = df_meli_sales['Date'].dt.strftime('%Y-%m')

    #--Creo las columnas que estan en datamind pero no en meli
    for col in [c for c in lst_columns if c not in df_meli_sales.columns]:
            df_meli_sales[col] = np.nan

    df_meli_sales['canal_venta']='ecommerce'
    df_meli_sales['Retailer'] = 'Mercado Libre'
    df_meli_sales['Source']='Mercado Libre'


    return df_meli_sales

#=====================
#--- UPDATE
#====================

def read_files_parquets(output_path,df,name_file,metrics):
    """
    Lee archivos Parquet del directorio que contiene los archivos ya procesados
    Leo solo los archivos cuyo year-month, contiene una semana que debe ser actualizada o agregada
    
    Args:
        input_path (str): Ruta del directorio donde se encuentran los archivos Parquets (.parquet) a consolidar.
        df_new (pd.DataFrame): DataFrame con la data nueva para identificar meses requeridos.

    Returns:
        pd.DataFrame or None: DataFrame consolidado con todos los datos de los archivos, o None si no se encuentran
        archivos o la lectura falla sin consolidar nada.
     """
    lst_year_month = df['Year-Month'].unique().tolist()
    lst_year_month.sort()

    lst_files=[]
    for date in lst_year_month:
        lst_files.append(name_file+date+'.parquet')

    # --- LECTURA Y CONSOLIDACION DE ARCHIVOS ---
    all_files_parquets = glob.glob(os.path.join(output_path, "*.parquet"))

    archivos_filtrados = [
    archivo for archivo in all_files_parquets 
    if os.path.basename(archivo) in lst_files
    ]

    if not archivos_filtrados:
        print(f"Info: No se encontraron archivos históricos previos para los meses solicitados en '{output_path}'.")
        # Retornamos un DataFrame vacío con las mismas columnas para no romper el concat posterior
        return pd.DataFrame(columns=df.columns)

    # Leer cada archivo y agregarlo a una lista de DataFrames:
    lst_files_xlsx = []
    for filename in archivos_filtrados:
        try:
            print(f"Leyendo archivo: {os.path.basename(filename)}")
            # Evitar sobrescribir la variable 'df' que viene como argumento
            df_temp = pd.read_parquet(filename)
            lst_files_xlsx.append(df_temp)
        except PermissionError:
            print(f"  [ERROR] Permiso denegado para leer el archivo: {os.path.basename(filename)}."
                  "\n  Asegúrate de que no esté abierto y vuelve a intentarlo.")
        except Exception as e:
            print(f"  [ERROR] No se pudo procesar el archivo {os.path.basename(filename)}: {e}")

    # Concatenar todos los DataFrames en uno solo
    if not lst_files_xlsx:
        return pd.DataFrame(columns=df.columns)

    df_consolidated = pd.concat(lst_files_xlsx, axis=0, ignore_index=True)

    # Normalización robusta: Convertimos métricas a float y fechas a datetime.
    # Solo aplicamos transformaciones de texto (upper/strip) a columnas que no son métricas ni fechas.
    for col in df_consolidated.columns:
        if col in metrics:
            df_consolidated[col] = pd.to_numeric(df_consolidated[col], errors='coerce', downcast='float').fillna(0.0)

        elif col == 'Date':
            df_consolidated[col] = pd.to_datetime(df_consolidated[col], errors='coerce')
        else:
            df_consolidated[col] = df_consolidated[col].astype(str).str.upper().str.strip()

    return df_consolidated

def update_datamind_meli(df_datamind_meli, df_meli_sales_historic):
    """
    Realiza un Upsert incremental optimizado en memoria y velocidad.
    Evita la duplicación de DataFrames pesados mediante indexación booleana directa.
    """
    # 1. Validar casos base de forma proactiva para ahorrar cómputo
    if df_datamind_meli.empty:
        return df_meli_sales_historic
    if df_meli_sales_historic.empty:
        return df_datamind_meli

    # 2. Crear las llaves únicas de forma rápida
    # En el set nuevo (pequeño), crear la llave concatenada está bien
    df_datamind_meli['key_update'] = df_datamind_meli['Year-Week'] + df_datamind_meli['Country']
    
    # Convertir a 'set' de Python: la búsqueda con `.isin()` en un set es O(1) (instantánea)
    weeks_to_update = set(df_datamind_meli['key_update'].unique())

    # 3. Filtrar el histórico SIN crear columnas nuevas en él
    # Concatenamos de forma virtual en la evaluación booleana para ahorrar RAM
    historic_keys = df_meli_sales_historic['Year-Week'] + df_meli_sales_historic['Country']
    
    # Máscara booleana: Mantenemos solo lo que NO está en el set de actualización
    mask_to_keep = ~historic_keys.isin(weeks_to_update)

    # 4. Filtrar por asignación directa (In-place filtering simulado)
    # Conservamos solo las filas deseadas del histórico usando la máscara
    df_meli_sales_historic_filtered = df_meli_sales_historic.loc[mask_to_keep]

    # 5. Limpieza preventiva de la columna temporal en el set nuevo
    df_datamind_meli = df_datamind_meli.drop(columns=['key_update'])

    # 6. Concatenación final directa
    # Ya validamos arriba que ninguno de los dos está vacío si llega a este punto
    df_meli_sales_update = pd.concat(
        [df_meli_sales_historic_filtered, df_datamind_meli], 
        ignore_index=True
    )

    return df_meli_sales_update


        
#===============
#--- SAVE
#===============
def group_parquet(df_processed, output_path, name="Datamind_Meli_Sales"):
    """
    Guarda el dataframe segmentado por año-mes en archivos planos.
    Optimizado al extremo para baja RAM usando máscaras booleanas iterativas,
    evitando la conversión del bloque completo de 70 millones de filas.
    """
    # 1. Asegurar carpeta destino
    os.makedirs(output_path, exist_ok=True)

    # 2. Obtener los periodos únicos de forma nativa y rápida
    unique_periods = df_processed['Year-Month'].unique()

    # 3. Procesar un mes a la vez para no saturar la memoria RAM
    for period in unique_periods:
        # Construcción de la ruta plana requerida
        filename = f"{name}_{period}.parquet"
        full_path = os.path.join(output_path, filename)
        
        # Crear una máscara booleana para extraer SOLO las filas de este mes
        # .loc con máscara no duplica todo el bloque de memoria numérico
        df_month = df_processed.loc[df_processed['Year-Month'] == period]
        
        # Si el mes no tiene datos por alguna razón, saltar
        if df_month.empty:
            continue

        # Convertir a PyArrow únicamente el fragmento pequeño de ese mes
        table_month = pa.Table.from_pandas(df_month, preserve_index=False)
        
        # Escritura a disco directa e inmediata
        pq.write_table(
            table_month, 
            full_path, 
            compression='snappy',
            use_dictionary=True
        )
        
        # Liberación explícita del fragmento antes de ir al siguiente mes
        del df_month
        del table_month
   


#==============
# ---- EDAS
#==============
def edas(path_edas,lst_columns_datamind):
    df_edas=pd.read_parquet(path_edas)
    
    for col in [c for c in lst_columns_datamind if c not in df_edas.columns]:
            df_edas[col] = np.nan
    df_edas=df_edas[lst_columns_datamind]
    metrics = [ 'Venta bruta', 'Venta costo', 'Unidades vendidas', 'Precio Publico Estimado']
    for col in metrics:
        if col in df_edas.columns:
            df_edas[col] = pd.to_numeric(df_edas[col], errors='coerce').fillna(0.0)
    df_edas['Retailer']=df_edas['Retailer'].replace({'AMAZON.COM SERVICES INC':'Amazon'})
    df_edas['Source']='EDAS'

    cust_upper = df_edas['Retailer'].astype(str).str.upper()
    condiciones_clientes = [
                cust_upper.str.contains('EASY|SODIMAC', na=False),
                cust_upper.str.contains('AMAZON', na=False)
            ]
    resultados_clientes = ['MODERM CHANNEL', 'ECOMMERCE']
            
    df_edas['Retailer'] = np.select(condiciones_clientes, resultados_clientes, 'TRADITIONAL CHANNEL')
    return df_edas