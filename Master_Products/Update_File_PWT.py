"""
EL CENTINELA DE POWER TOOLS: FILTRO DE CALIDAD PWT/FAS
-----------------------------------------------------
Este script es el filtro de seguridad para que el reporte de Power Tools no sea un caos. 
Su misión es encontrar SKUs de PWT y FAS (especialmente de DEWALT) que están "volando" 
sin una clasificación de 'Group 1' o 'Group 2' adecuada. 

Básicamente, le ahorra horas al analista diciéndole exactamente qué productos nuevos 
entraron al sistema o cuáles tienen datos incompletos que arruinarían las gráficas de ventas.

FLUJO DE TRABAJO:
1. Selección de Élite: Filtra el Maestro de Productos para quedarse solo con lo que 
   importa aquí: SBUs de Power Tools (PWT) o Fastening (FAS) bajo la marca DEWALT.
2. Cruce de Identidad: Compara contra el archivo de trabajo histórico para ver quién 
   es un "New SKU" y quién ya estaba en el radar.
3. Detector de Vacíos: Escanea las columnas 'Group 1' y 'Group 2'. Si encuentra el 
   famoso guion "-", levanta una bandera de "Revisión: Faltan datos".
4. Entrega de Tareas: Genera un Excel ordenado por prioridad para que el equipo de 
   Marketing solo rellene lo que falta.

💡 NOTA DE SENIOR:
¡Cuidado con los filtros! Este script tiene "hardcodeado" que solo mira DEWALT y 
ciertas SBUs. Si mañana el negocio pide incluir Stanley o Black+Decker en este 
flujo de PWT, vas a tener que ajustar la lógica en `update_file_pwt`. 
Además, recuerda que este archivo *sobrescribe* el workfile, así que asegúrate 
de que nadie lo tenga abierto o el script va a tronar por permisos de Excel.
"""
import pandas as pd
import numpy as np
import sys
from Fill_Rate.Process_ETL.Process_Files import clean_sku

def update_file_pwt(md_product,lst_columns_pwt,df_pwt):
    """
    Filtra el Maestro de Productos por la SBU 'PWT' y realiza una validación de calidad sobre sus columnas de clasificación
    (Group 1, Group 2). Clasifica cada SKU en base a dos estados:
        1) 'New sku' (no existe en el archivo PWT de referencia).
        2) 'SKU Existente - Revisión' (existe, pero sus campos clave Group 1 y/o Group 2 contienen datos faltantes o por defecto,
           indicados por el separador '-').

    Args:
        md_product (pd.DataFrame): El DataFrame del Maestro de Productos principal.
        lst_columns_pwt (list): Lista de columnas PWT y de clasificación necesarias.
        df_pwt (pd.DataFrame): El DataFrame de referencia de PWT (archivo de trabajo).

    Returns: pd.DataFrame: DataFrame listo para exportar que contiene solo los SKUs PWT, incluyendo la columna 'check_sku' con el estado de
             verificación y la necesidad de revisión.
    """
    df_filter_pwt=md_product[((md_product['GPP SBU']=='PWT') | (md_product['GPP SBU']=='FAS')) &
                             (md_product['Brand'].str.upper()=='DEWALT')][lst_columns_pwt].copy()
    mask_sku_md=df_filter_pwt['SKU'].isin(df_pwt['SKU'])
    df_filter_pwt['check_sku']=np.where(
                                mask_sku_md, 
                                'Verified', 
                                'New sku'
                             )
    df_filter_pwt['concat_info']=(df_filter_pwt['Group 1']+df_filter_pwt['Group 2']).str.lower().str.strip().str.replace(' ', '')
    mask_sku_old=((df_filter_pwt['check_sku']=='Verified') &
                  (df_filter_pwt['concat_info'].str.contains('-')))
    df_filter_pwt['check_sku']=np.where(
                                mask_sku_old, # Condición
                                'SKU Existente - Revisión: Faltan datos en campos clave', # Valor si la condición es True
                                df_filter_pwt['check_sku'] # Valor si la condición es False (mantiene el valor original, que en este caso es 'verified')
                                )
    df_filter_pwt=df_filter_pwt.sort_values(by=['check_sku','SKU','SKU Base'])
    df_filter_pwt=df_filter_pwt[lst_columns_pwt+['check_sku']]
    df_filter_pwt=df_filter_pwt.fillna('-')
    return df_filter_pwt

def main():
    """	
    Función principal que orquesta la generación del archivo de trabajo PWT.	
    Carga el Maestro de Productos y el archivo PWT de referencia. Llama a la función de validación	
    y sobrescribe el archivo de trabajo PWT con la lista de SKUs que necesitan verificación y los que ya están verificados.	
    
    Returns: None: La función orquesta el proceso y guarda el resultado en un archivo Excel (Workfile PWT).
    """
    print("=" * 55)
    print("--- 🔄 INICIANDO PROCESO: PWT UPDATE ETL ---")
    print("=" * 55)
    try:
        from config_paths import MasterProductsPaths
        path_md_product = MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE
        path_pwt=MasterProductsPaths.WORKFILE_PWT_FILE
        lst_columns_pwt=['SKU', 'SKU Base', 'SKU Description', 'Brand', 'GPP SBU',
        'GPP Division Code', 'GPP Division Description',
        'GPP Category Description', 'GPP Portfolio Description','Group 1','Group 2']
        df_pwt=pd.read_excel(path_pwt, dtype=str, engine='openpyxl')
        df_md_product=pd.read_excel(path_md_product, dtype=str, engine='openpyxl')
        

        #------------------------
        #-- limpieza sku
        #---------------------
        df_pwt=clean_sku(df_pwt,'SKU')
        df_md_product=clean_sku(df_md_product,'SKU')

        
        df_update_pwt=update_file_pwt(df_md_product,lst_columns_pwt,df_pwt)
        df_update_pwt.to_excel(path_pwt, index=False)
        print("Proceso de actualización de productos completado exitosamente.")
        pass
    except Exception as e:
        print(f'Error en procesamiento de datos de PWT: {e}')
        sys.exit(1)

if __name__ == "__main__":
    main()