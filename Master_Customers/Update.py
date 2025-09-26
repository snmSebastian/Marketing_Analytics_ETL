# leer los archivos update de demand fill rate y sales
# realide cada uno de estos qobtener la informaicon de custome
#crear un dataframe unificado 
# crear un archivo  donde esten los nuevos clientes suministrdos por cada pais
# #asignar la correcta notacion de clasificacion (traudccion)
# hacer un merge entre el df unificado y el df (con info compartida) para asignar clasificacion
# aquellos que quede sin info
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

# Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico.
import glob
import os

#--------------------------------------------------
#---------------- RUTAS ---------------------------
path_demand=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Demand\Mothly_Update'
path_fill_rate=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Fill_Rate\Mothly_Update'
path_sales=r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Data\Raw\Sales\Mothly_Update'

def read_files_new_info(path_demand,path_fill_rate,path_sales):
    """
    Lee archivos Excel de los directorios especificados y los consolida en un DataFrame.
    
    Args:
        path_demand (str): Ruta del directorio de archivos de demanda.
        path_fill_rate (str): Ruta del directorio de archivos de fill rate.
        path_sales (str): Ruta del directorio de archivos de ventas.
    
    Returns:
        pd.DataFrame: DataFrame consolidado con la información de clientes nuevos.
    """
    files_demand = glob.glob(os.path.join(path_demand, '*.xlsx'))
    files_fill_rate = glob.glob(os.path.join(path_fill_rate, '*.xlsx'))
    files_sales = glob.glob(os.path.join(path_sales, '*.xlsx'))

    df_demand = pd.concat((pd.read_excel(f) for f in files_demand), ignore_index=True)
    df_fill_rate = pd.concat((pd.read_excel(f) for f in files_fill_rate), ignore_index=True)
    df_sales = pd.concat((pd.read_excel(f) for f in files_sales), ignore_index=True)

    #elegir las columnas relevantes de cada DataFrame
    df_demand



    df_consolidated = pd.concat([df_demand, df_fill_rate, df_sales], ignore_index=True)
    
    return df_consolidated