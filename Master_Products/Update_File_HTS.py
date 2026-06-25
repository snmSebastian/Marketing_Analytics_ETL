"""
EL GUARDIÁN DE HAND TOOLS: FILTRO DE CALIDAD HTS
-----------------------------------------------
Este script es el sensor de seguridad para la categoría de Herramientas Manuales (HMT). 
Su trabajo es detectar SKUs que están "en el limbo": ya sea porque son nuevos o porque 
son viejos conocidos pero les falta información vital de clasificación (Categoría, 
Familia, Clase, etc.).

Sin este filtro, el reporte de HTS saldría con huecos, afectando la visibilidad del 
negocio sobre qué estamos vendiendo realmente.

FLUJO DE TRABAJO:
1. Filtrado Selectivo: Separa del Maestro de Productos únicamente los registros 
   de la SBU 'HMT'.
2. Cruce Histórico: Compara los SKUs actuales contra el Workfile de HTS para 
   identificar quién es un "New SKU".
3. Auditoría de Datos: Realiza una limpieza y concatenación de los campos clave 
   de HTS para detectar registros con guiones ("-") o vacíos.
4. Clasificación de Revisión: Etiqueta cada registro como 'Verified', 'New SKU' 
   o 'Faltan datos' para que el analista sepa exactamente dónde meter mano.
5. Generación de Tareas: Exporta el Excel listo para la validación manual de marketing.

💡 NOTA DE SENIOR:
Ojo aquí: este script **sobrescribe** el archivo de trabajo (`HTS_Classification_Workfile.xlsx`). 
Si alguien tiene el archivo abierto mientras corre el proceso, Python va a lanzar un 
`PermissionError` y el pipeline se va a detener. Avisen al equipo que no lo dejen 
abierto en el servidor.
"""

import pandas as pd
import numpy as np
import sys

from Fill_Rate.Process_ETL.Process_Files import clean_sku



def update_file_hts(md_product,lst_columns_hts,df_hts):
    """
    Filtra el Maestro de Productos por la SBU 'HMT' y realiza una validación de calidad sobre las columnas HTS. Clasifica
    cada SKU en base a dos reglas:
        1) Si es un 'New sku' (no está en el archivo HTS de referencia).
        2) Si es un 'SKU Existente' pero le faltan datos en campos HTS clave (valores que solo contienen el separador '-' después del concatenado).
    Args:
        md_product (pd.DataFrame): El DataFrame del Maestro de Productos principal.
        lst_columns_hts (list): Lista de columnas HTS y de clasificación necesarias.
        df_hts (pd.DataFrame): El DataFrame de referencia de HTS (archivo de trabajo).
    Returns: pd.DataFrame: DataFrame listo para exportar que contiene solo los SKUs HMT con la columna 'check_sku'
             actualizada para indicar si es un SKU nuevo o si requiere revisión de datos faltantes.
    """
    # Filtro de md_products aquellos sku de hts y las columnas que necesito
    df_filter_hts=md_product[(md_product['GPP SBU']=='HMT' )|
                             (md_product['GPP SBU']=='STR' ) ][lst_columns_hts].copy()
    # determino una lst que indica si el sku del md esta en el archivo de hts
    mask_sku_md=df_filter_hts['SKU'].isin(df_hts['SKU'])
    # asigno si el sku es nuevo o no
    df_filter_hts['check_sku']=np.where(
                                mask_sku_md, 
                                'Old sku', 
                                'New sku'
                             )
  
    # ordeno dataframe de salida
    df_filter_hts=df_filter_hts.sort_values(by=['check_sku','SKU','SKU Base'])
    df_filter_hts=df_filter_hts[lst_columns_hts+['check_sku']]
    df_filter_hts=df_filter_hts.fillna('-')
    return df_filter_hts

def main():
    """	
    Función principal que orquesta la generación del archivo de trabajo HTS.	
    Carga el Maestro de Productos y el archivo HTS de referencia. Llama a la función de validación	
    y sobrescribe el archivo de trabajo HTS con la lista de SKUs que necesitan verificación y los que ya están verificados.	
    Returns: None: La función orquesta el proceso y guarda el resultado en un archivo Excel (Workfile HTS).
    """
    print("=" * 55)
    print("---  INICIANDO PROCESO: HTS UPDATE ETL ---")
    print("=" * 55)
    try:
        from config_paths import MasterProductsPaths
        path_md_product = MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE
        path_hts=MasterProductsPaths.WORKFILE_HTS_FILE
        
        lst_columns_hts=['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP SBU',
        'GPP Division Code', 'GPP Division Description',
        'GPP Category Description', 'GPP Portfolio Description', 'Big Rock',
        'Category Group',  'Categoria HTS', 'Familia HTS',
        'Sub Familia HTS', 'Clase HTS', 'NPI Project HTS',
        'Posicionamiento HTS']
        df_hts=pd.read_excel(path_hts, dtype=str, engine='openpyxl')
        df_md_product=pd.read_excel(path_md_product, dtype=str, engine='openpyxl')

        #--------------------
        #---- limpieza sku
        #-------------------
        df_hts=clean_sku(df_hts,'SKU')
        df_md_product=clean_sku(df_md_product,'SKU')
        
        df_filter_hts=update_file_hts(df_md_product,lst_columns_hts,df_hts)
        df_filter_hts.to_excel(path_hts, index=False)
        print("Proceso de actualización de productos completado exitosamente.")
        pass
    except Exception as e:
        print(f'Error en procesamiento de datos de HTS: {e}')
        sys.exit(1)

if __name__ == "__main__":
    main()