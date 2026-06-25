
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

from pathlib import Path
from Fill_Rate.Process_ETL.Process_Files import clean_sku
from Master_Products.Update_md_products import create_inverse_brand_map,BRAND_STANDARD_MAP


#=========================
#--- DATAMIND
#=========================
def consolidate_files(path_arg,path_mx,path_ch):
    """
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

    #mex
    print('leyendo mexico')
    r1=r'C:\Users\SSN0609\Downloads\Venta-Semanal23.csv'
    r2=r'C:\Users\SSN0609\Downloads\Venta-Semanal24.csv'
    r3=r'C:\Users\SSN0609\Downloads\Venta-Semanal25.csv'
    r4=r'C:\Users\SSN0609\Downloads\Venta-Semanal26mex.csv'
    df23=pd.read_csv(r1,dtype=str)
    df24=pd.read_csv(r2,dtype=str)
    df25=pd.read_csv(r3,dtype=str)
    df26=pd.read_csv(r4,dtype=str)

    df_mx=pd.concat([df23,df24,df25,df26])
    #arg
    print('leyendo argentina')
    ruta23arg=r'C:\Users\SSN0609\Downloads\Venta-Semanal23ARG.csv'
    ruta24arg=r'C:\Users\SSN0609\Downloads\Venta-Semanal24ARG.csv'
    ruta25arg=r'C:\Users\SSN0609\Downloads\Venta-Semanal25ARG.csv'
    ruta26arg=r'C:\Users\SSN0609\Downloads\Venta-Semanal26arge.csv'
    df23arg=pd.read_csv(ruta23arg, dtype=str)
    df24arg=pd.read_csv(ruta24arg, dtype=str)
    df25arg=pd.read_csv(ruta25arg, dtype=str)
    df26arg=pd.read_csv(ruta26arg, dtype=str)
    df_arg=pd.concat([df23arg,df24arg,df25arg,df26arg])
    #chi
    print('leyendo chile')
    ruta23ch=r'C:\Users\SSN0609\Downloads\Venta-Semanal23chi.csv'
    ruta24ch=r'C:\Users\SSN0609\Downloads\Venta-Semanal24chi.csv'
    ruta25ch=r'C:\Users\SSN0609\Downloads\Venta-Semanal25chi.csv'
    ruta26ch=r'C:\Users\SSN0609\Downloads\Venta-Semanal26chile.csv'
    df23ch=pd.read_csv(ruta23ch, dtype=str)
    df24ch=pd.read_csv(ruta24ch, dtype=str)
    df25ch=pd.read_csv(ruta25ch, dtype=str)
    df26ch=pd.read_csv(ruta26ch, dtype=str)
    df_ch=pd.concat([df23ch,df24ch,df25ch,df26ch])


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

    mask_retailer = df_datamind_week['Retailer'].str.contains(lst_retailer)
    mask_canal = df_datamind_week['Local'].str.contains(lst_canal)
    mask_ecom= (mask_retailer|mask_canal)

    # Inicializar la columna 'canal_venta' con un valor por defecto
    df_datamind_week['canal_venta'] = 'store'
    df_datamind_week.loc[mask_ecom, 'canal_venta'] = 'ecommerce'

    df_datamind_week['canal_venta'] = df_datamind_week['canal_venta'].replace(['vacio','moderno','tradicional','tienda'], 'store')
    df_datamind_week['canal_venta'] = df_datamind_week['canal_venta'].replace(['ecommerce','e-commerce'], 'e-commerce')
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

#======================
# --- MERCADO LIBRE
#======================
def meli(path_meli_sales, path_meli_sku,lst_columns):


    # Forzamos la lectura como string para evitar errores de concatenación y pérdida de ceros a la izquierda
    ruta=r'C:\Users\SSN0609\Downloads\data_meli_hist.parquet'
    df_meli_sales=pd.read_parquet(ruta)

    #df_meli_sales=pd.read_excel(path_meli_sales, dtype=str)
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
    df_meli_sales['Retailer'] = 'Mercado Libre' + '-' + df_meli_sales['Country'].str[:3].str.upper()
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
            df_consolidated[col] = pd.to_numeric(df_consolidated[col], errors='coerce').fillna(0.0)
        elif col == 'Date':
            df_consolidated[col] = pd.to_datetime(df_consolidated[col], errors='coerce')
        else:
            df_consolidated[col] = df_consolidated[col].astype(str).str.upper().str.strip()

    return df_consolidated

def update_datamind_meli(df_datamind_meli,df_meli_sales_historic):
    """
    Realiza una actualización incremental (Upsert) de los datos de ventas a nivel semanal.

    Esta función identifica las semanas presentes en los nuevos datos y las reemplaza
    en el histórico. Para ello, filtra el DataFrame histórico eliminando los registros
    de las semanas que van a ser actualizadas y luego concatena los resultados.
    Incluye lógica de seguridad para manejar DataFrames vacíos y evitar advertencias
    de deprecación (FutureWarning) en la concatenación.

    Args:
        df_datamind_meli (pd.DataFrame): DataFrame con la información nueva o actualizada.
        df_meli_sales_historic (pd.DataFrame): DataFrame con la información histórica del periodo.

    Returns:
        pd.DataFrame: DataFrame consolidado con las semanas actualizadas y el histórico preservado.

    💡 Nota Senior: Este proceso es vital para la integridad del Data Lake, ya que garantiza
    la inexistencia de duplicados al sobrescribir semanas completas en lugar de solo agregar filas.
    """
    #semanas del historico que seran actualizadas
    weeks_to_update = df_datamind_meli['Year-Week'].unique()

    #conservo las semanas que SI seran actualizadas
    df_meli_sales_historic_filtered = df_meli_sales_historic[~df_meli_sales_historic['Year-Week'].isin(weeks_to_update)]
    
     # Crear una lista de DataFrames a concatenar, excluyendo los que estén vacíos.
    # Esto evita el FutureWarning al no pasar DataFrames vacíos a pd.concat
    # y asegura que la inferencia de tipos de datos sea consistente
    dataframes_to_concat = []
    if not df_meli_sales_historic_filtered.empty:
        dataframes_to_concat.append(df_meli_sales_historic_filtered)
    if not df_datamind_meli.empty:
        dataframes_to_concat.append(df_datamind_meli)

    # Si no hay DataFrames para concatenar (ambos inputs estaban vacíos o se filtraron completamente),
    # devolver un DataFrame vacío con las columnas correctas.
    # Se priorizan las columnas de df_datamind_meli si no estaba vacío,
    # de lo contrario, las de df_meli_sales_historic. Si ambos estaban vacíos, se devuelve un DataFrame sin columnas.

    if not dataframes_to_concat:
        if not df_datamind_meli.empty:
            return pd.DataFrame(columns=df_datamind_meli.columns)
        elif not df_meli_sales_historic.empty:
            return pd.DataFrame(columns=df_meli_sales_historic.columns)
        else:
            return pd.DataFrame()

    df_meli_sales_update = pd.concat(dataframes_to_concat, ignore_index=True)
    return df_meli_sales_update

        
#===============
#--- SAVE
#===============
def group_parquet(df_processed, output_path, name=str):
    """ Guarda un dataframe consolidado en archivos Parquet segmentados por año-mes.
    Args:
        df_processed (pd.DataFrame): DataFrame ya limpio y procesado. Debe contener la columna 'fk_year_month'.
        output_path (str): Ruta del directorio donde se guardarán los archivos Parquet particionados.
        name (str, optional): Prefijo para los nombres de los archivos Parquet generados. Por defecto es 'fill_rate'. 

    Returns:
        None: La función no devuelve un valor, sino que guarda los archivos en el disco.
    """
    # 1. Asegurar que la carpeta destino existe (evita errores de I/O)
    os.makedirs(output_path, exist_ok=True)

    # 2. Agrupamos
    # Se crea un mapeo de indices donde
        # llave: fk_year_month
        #valor: indice de filas que poseen dicha llave
    df_processed['year']=df_processed['Year-Week'].str[:4]
    
    groups = df_processed.groupby('Year-Month', sort=False)

    for period, group in groups:
        # Construcción eficiente de la ruta
        filename = f"{name}_{period}.parquet"
        full_path = os.path.join(output_path, filename)
        
        # 3. Guardar: Usamos el engine 'pyarrow' explícitamente (es el más rápido)
        # y desactivamos el índice para ahorrar espacio y tiempo de cómputo.
        group.to_parquet(
            full_path, 
            index=False, 
            engine='pyarrow', 
            compression='snappy' # Equilibrio perfecto entre peso y velocidad
        )
    


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
    return df_edas