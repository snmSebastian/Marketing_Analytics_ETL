"""
LIBRERÍA CENTRAL DE PROCESAMIENTO: Módulo Core de Fill Rate y Utilidades Compartidas.

Este script es el pilar técnico del proyecto. No solo procesa el ETL de Fill Rate, 
sino que funciona como la "Caja de Herramientas" oficial para Demand y Sales.

¿Por qué es el archivo más importante?
 • Centralización: Aquí viven 'group_parquet' y 'format_columns', funciones que 
   usan los demás módulos para garantizar que todos los datos tengan el mismo formato.
 • Resiliencia: La lectura de archivos (read_files) gestiona errores de permisos y 
   bloqueos de Excel, evitando que el pipeline se detenga por un archivo abierto.
 • Eficiencia: Al segmentar en Parquet por año-mes, transformamos procesos pesados 
   en consultas ultrarrápidas para Power BI.

Funciones que exporta a otros módulos:
 • format_columns: (Usada por Demand y Sales) Estandariza tipos de datos y limpieza.
 • group_parquet: (Usada por Demand y Sales) El estándar oficial de guardado particionado.
 • read_files: (Usada por Demand) El motor de consolidación de históricos.

💡 NOTA DE ARQUITECTURA: Cualquier cambio en estas funciones core se reflejará 
automáticamente en Demand y Sales. ¡Cuidado al editar, es el motor compartido!
"""



#--------------------------------------------------
#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
# Liberia
import pandas as pd
import numpy as np

# Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico.
import glob
import os


# Lectura de archivos
def read_files(input_path):
    """
    Lee archivos Excel de un directorio, los consolida en un dataframe
    
    Args:
        input_path (str): Ruta del directorio donde se encuentran los archivos Excel (.xlsx) a consolidar.
    
    Returns:
        pd.DataFrame or None: DataFrame consolidado con todos los datos de los archivos, o None si no se encuentran
        archivos o la lectura falla sin consolidar nada.
     """
    
    # --- LECTURA Y CONSOLIDACION DE ARCHIVOS ---
    # Buscar todos los archivos .xlsx en el directorio de entrada.
    all_files_xlsx = glob.glob(os.path.join(input_path, "*.xlsx"))

    if not all_files_xlsx:
        print(f"Advertencia: No se encontraron archivos .xlsx en '{input_path}'.")
        return

    # Leer cada archivo y agregarlo a una lista de DataFrames:
    lst_files_xlsx = []
    for filename in all_files_xlsx:
        try:
            print(f"Leyendo archivo: {os.path.basename(filename)}")
            # Usar pd.read_excel para archivos .xlsx, no pd.read_csv
            df = pd.read_excel(filename, dtype=str, engine='openpyxl')
            lst_files_xlsx.append(df)
        except PermissionError:
            print(f"  [ERROR] Permiso denegado para leer el archivo: {os.path.basename(filename)}."
                  "\n  Asegúrate de que no esté abierto en Excel y vuelve a intentarlo.")
        except Exception as e:
            print(f"  [ERROR] No se pudo procesar el archivo {os.path.basename(filename)}: {e}")

    # Concatenar todos los DataFrames en uno solo
    if len(lst_files_xlsx) == 0:
        df_consolidated = lst_files_xlsx[0]
    else:
         df_consolidated = pd.concat(lst_files_xlsx, axis=0, ignore_index=True)


    for col in df_consolidated.columns:
        df_consolidated[col] = df_consolidated[col].astype(str).str.upper().str.strip()
    
    return df_consolidated

# Asignacion pais
def asign_country_code(df_consolidated, df_country):
        """
        Asigna el código de país a cada fila del DataFrame df
        usando el DataFrame country como referencia.

        Args:
            df_consolidated (pd.DataFrame): DataFrame principal con los datos de Fill Rate.
                                            Debe contener las columnas 'Country Code' y 'Destination Country'.
            df_country (pd.DataFrame): DataFrame de referencia para el mapeo de países.
                                       Debe contener 'Country Code Concat' y 'Country'.

        Returns:
            pd.DataFrame: El DataFrame df_consolidated original, modificado con las nuevas columnas 'code concat country'
                          y 'fk_Country'.    
        """
        # Crear una columna 'code concat country' que concatena 'Country Code' y 'Destination Country'
        df_consolidated['code concat country'] = df_consolidated['Country Code'].astype(str) + df_consolidated['Destination Country'].astype(str)
        df_consolidated['code concat country'] = df_consolidated['code concat country'].str.upper().str.strip()

        # Crear un mapa de códigos de país a nombres de país
        for col in df_country.columns:
            df_country[col] = df_country[col].astype(str).str.upper().str.strip()
        country_map = df_country.set_index('Country Code Concat')['Country']

        # Usar .map() para crear la nueva columna 'new country'
        df_consolidated['fk_Country'] = df_consolidated['code concat country'].map(country_map)

        '''ESTA LINEA SE PUEDE ELIMINAR, ES PARA VER CUAL PAIS SE QUEDO SIN ASIGNACION
        
        '''
        #df_consolidated['pais']=df_consolidated['code concat country']+'-'+df_consolidated['fk_Country']
        
        #paises_unicos = df_consolidated['pais'].unique()
        #df_paises = pd.DataFrame(paises_unicos, columns=['code concat country-fk_Country'])
        #df_paises.to_excel(
        #    r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Processed-Dataflow\Shared_Information_for_Projects\Country\result-code concat country-fk_Country.xlsx', 
        #    index=False
        #)
        
      
        '''FIN DE LA LINEA'''
       
       
        return df_consolidated

def process_columns(df_consolidated,lst_columns):
    """    
        Renombra, calcula columnas clave ('fk_year_month', 'clasification', 'fk_date_country_customer_clasification',
        'fk_Date'), y selecciona el subconjunto final de columnas para el DataFrame procesado.
    Args:
        df_consolidated (pd.DataFrame): DataFrame consolidado que contiene todas las columnas sin procesar.
        lst_columns (list): Lista de strings con los nombres de las columnas finales deseadas, incluyendo las recién creadas (e.g., 'fk_Date', 'fk_Country').
    Returns:
        pd.DataFrame: DataFrame final, filtrado por lst_columns, listo para ser guardado.
    """
    try:
        

        # --- PROCESAMIENTO Y AGRUPACION ---
        # Crear una columna 'year_month' para usar en la agrupación (ej: '2023-01').
        # Se usa .str.zfill(2) para asegurar que los meses tengan dos dígitos (ej: '01', '02', etc.)
        # lo que mejora la consistencia y el orden de los nombres de archivo.   
        # 
        # Definir los mapeos que quieres aplicar
        mapeo_deseado = {
            'Sold-To-Customer Code': 'fk_Sold_To_Customer_Code',
            'Sold-To Customer Code': 'fk_Sold_To_Customer_Code', # La clave es diferente, el valor es el mismo
            'Country Material': 'fk_SKU',
            'Global Material': 'fk_SKU',
        }

        # Crear un nuevo diccionario de renombre que solo incluye las columnas existentes
        columnas_existentes = df_consolidated.columns
        mapeo_filtrado = {
            old_name: new_name
            for old_name, new_name in mapeo_deseado.items()
            if old_name in columnas_existentes
        }

        df_consolidated.rename(columns=mapeo_filtrado, inplace=True)    

        df_consolidated['fk_year_month'] = (df_consolidated['Fiscal Year'].astype(str) + '-' +
                                            df_consolidated['Fiscal Period'].astype(str).str.zfill(2))
        df_consolidated['clasification']=(df_consolidated['GPP Division'] + '-' +
                                         df_consolidated['GPP Category'] + '-' +
                                         df_consolidated['GPP Portfolio'])
        
        
       
        df_consolidated['fk_Date']=pd.to_datetime(df_consolidated['fk_year_month'],
                                                  format='%Y-%b',
                                                  errors='coerce')
       
        df_processed = df_consolidated[lst_columns].copy()                                                                                                                                                                           
        # Convertir todas las columnas a mayúsculas y eliminar espacios
        for col in df_processed.columns:        
            df_processed.loc[:,col] = df_consolidated[col].astype(str).str.upper().str.strip()    
       
    except KeyError as e:
                print(f"Error: La columna {e} no se encontró en los archivos. ")
    return df_processed

def format_columns(df: pd.DataFrame, lst_columns_str: list, lst_columns_float: list) -> pd.DataFrame:
    """
    Convierte las columnas especificadas a str o float, manejando errores de conversión:
    - Los errores en float se convierten a NaN y luego a 0.
    - Los valores NaN/nulos/errores en str se convierten a la cadena vacía "".
    """
    df=df.copy()
    df=df[lst_columns_str+lst_columns_float]
    # 1. CONVERSIÓN Y LIMPIEZA DE CADENAS (STR)
    for col in lst_columns_str:
        # 1.1. Convertir a str
        df[col] = df[col].astype(str)
        
        # 1.2. Limpieza de strings: minúsculas, reemplazo de 'nan' a '', y eliminar espacios
        df[col] = (df[col].str.lower()
                           .str.replace('nan', '', regex=False)
                           .str.strip())
        
        # 1.3. Opcional: Manejo de nulos originales que no son 'nan' string (ej. np.nan)
        # Aunque astype(str) maneja la mayoría, este fillna es un seguro extra.
        df[col] = df[col].fillna('')

    # 2. CONVERSIÓN A FLOTANTE (FLOAT) con manejo de errores
    for col in lst_columns_float:
        # 2.1. Conversión con manejo de errores (errors='coerce' -> errores a NaN)
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col]=df[col].fillna(np.nan)  # Asegurar que los NaN se manejen correctamente
        # 2.2. Reemplazar NaN (errores de conversión) por 0, y asegurar dtype float
        df[col] = df[col].fillna(0).astype(np.float32)
        
    return df


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
        
    groups = df_processed.groupby('fk_year_month', sort=False)

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

def clean_sku(df,name_column:str):
    """
    Limpia y estandariza la columna del argumento de entrada eliminando caracteres no deseados.
    Solo permite: Letras (A-Z), Números (0-9), y los caracteres /, \, ., -
    """
    
    # El patrón define una "lista blanca":
    # ^ dentro de [] significa "todo lo que NO sea lo siguiente"
    # A-Z0-9: Letras y números
    # /: Barra inclinada
    # \\: Barra invertida (se usan dos para escapar el carácter en Python)
    # \.: Punto (se escapa porque en regex el punto significa "cualquier carácter")
    # \-: Guion (se pone al final para evitar que defina un rango)
    allowed_pattern = r'[^A-Z0-9/\\.\-]'
    df[name_column] = df[name_column].fillna('').astype(str)
    df[name_column] = (
        df[name_column]
        .astype(str)          # Convierte a texto para evitar errores con nulos o números
        .str.upper()          # Convierte todo a mayúsculas
        # El replace elimina espacios y caracteres especiales en un solo paso:
        .str.replace(allowed_pattern, '', regex=True)
    )
    return df



