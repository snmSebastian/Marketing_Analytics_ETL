import pandas as pd
import numpy as np

from Sales.Process_ETL.Update import read_files_parquets

# =========================================================================
# FUNCIONES DE SOPORTE PARA MÉTRICAS AVANZADAS
# =========================================================================

def  generate_key(df_campaigns,df_datamind_meli_sales):
    #====================
    # ---- CAMPAIGNS
    #====================
    df_campaigns = df_campaigns.rename(columns={
        'País': 'Country',
    })
    #---COUNTRY
    df_campaigns.columns = df_campaigns.columns.str.strip()
    df_campaigns['Country'] = df_campaigns['Country'].str.strip()
    
    # Corrección sintáctica usando un diccionario {} y comas
    df_campaigns['Country'] = df_campaigns['Country'].replace({
        'AR': 'Argentina',
        'BR': 'Brazil',
        'CL': 'Chile',
        'CO': 'Colombia',
        'CR': 'Costa Rica',
        'GT': 'Guatemala',
        'HN': 'Honduras',
        'MX': 'Mexico',
        'PA': 'Panama',
        'SV': 'El Salvador'
    })
     #--- DATE CAMPAÑAS
    df_campaigns['Week End'] = df_campaigns['Semana'].str[-10:]
    df_campaigns['Week End'] = pd.to_datetime(df_campaigns['Week End'], errors='coerce')
    iso_cal_sales = df_campaigns['Week End'].dt.isocalendar()
    df_campaigns['Year_Week'] = (iso_cal_sales.year.astype(str) + 
                            iso_cal_sales.week.astype(str).str.zfill(2)).astype(int)

    #=================
    #--- SALES
    #=================
    df_datamind_meli_sales = df_datamind_meli_sales.rename(columns={
        'Year-Week': 'Year_Week'
    })
    # Aseguramos que Year_Week sea entero para que coincida con el tipo de df_campaigns
    df_datamind_meli_sales['Year_Week'] = pd.to_numeric(df_datamind_meli_sales['Year_Week'], errors='coerce').fillna(0).astype(int)

    # Asegurar que las métricas sean numéricas antes de agrupar. 
    # Esto convierte los strings 'NAN' en 0 y permite cálculos matemáticos.
    metrics = ['Venta neta', 'Venta bruta', 'Venta costo', 'Unidades vendidas', 'Precio Publico Estimado']
    for col in metrics:
        df_datamind_meli_sales[col] = pd.to_numeric(df_datamind_meli_sales[col], errors='coerce').fillna(0)

    df_campaigns['key'] = (
        df_campaigns['Country'].astype(str) + "_" + 
        df_campaigns['Brand'].astype(str) + "_" + 
        df_campaigns['Year_Week'].astype(str)
    )
    
    df_datamind_meli_sales['key'] = (
        df_datamind_meli_sales['Country'].astype(str) + "_" + 
        df_datamind_meli_sales['Brand'].astype(str) + "_" + 
        df_datamind_meli_sales['Year_Week'].astype(str)
    )
    df_campaigns['key'] = df_campaigns['key'].astype(str).str.lower().str.replace(' ', '')
    df_datamind_meli_sales['key'] = df_datamind_meli_sales['key'].astype(str).str.lower().str.replace(' ', '')
   

    df_campaigns=df_campaigns.sort_values(by=['key'])
    df_datamind_meli_sales=df_datamind_meli_sales.sort_values(by=['key'])

    #----VENTAS AGRUPADAS POR KEY
    df_sales_agg=df_datamind_meli_sales.groupby(['key']).agg({
         'Country': 'first',      
        'Brand': 'first',
        'Year_Week': 'first',
        'Venta neta': 'sum',
        'Venta costo': 'sum',
        'Venta bruta': 'sum',
        'Unidades vendidas': 'sum',
        'Precio Publico Estimado': 'mean' 
    }).reset_index()
    print('df_sales_agg')
    print(f'len(df_sales_agg){len(df_sales_agg)}')
    df_sales_agg.to_excel
    
    #--- MERGE
    df_campaigns=df_campaigns.drop(columns=['Country', 'Brand', 'Year_Week'])
    df_merged = pd.merge(
        df_campaigns, 
        df_sales_agg[['key','Country', 'Brand', 'Year_Week',  'Venta neta', 'Venta costo', 'Venta bruta', 'Unidades vendidas', 'Precio Publico Estimado']], 
        on='key', 
        how='right'
    )
   

    df_merged = df_merged.sort_values(by=['key'])
    print('df_camp')
    df_merged.to_excel("dataset_atribucion_final.xlsx", index=False)

    return df_merged


def aplicar_adstock(series, alpha=0.15):
    """
    Aplica decaimiento geométrico a la inversión publicitaria por grupo.
    """
    inversion = series.values
    adstock = np.zeros(len(inversion))
    for t in range(len(inversion)):
        if t == 0:
            adstock[t] = inversion[t]
        else:
            adstock[t] = inversion[t] + alpha * adstock[t-1]
    return adstock
import pandas as pd
import numpy as np

def metrics_campaigns(df_campaigns):
    """
     Procesa el detalle histórico microscópico de marketing.
    """
    # Clonamos para evitar advertencias de copia en Pandas
    df_medios = df_campaigns.copy()
    
    # =========================================================================
    # 0. LIMPIEZA INICIAL
    # =========================================================================
    # Forzar que la inversión sea numérica y rellenar vacíos con 0
    df_medios['Importe gastado (USD)'] = pd.to_numeric(df_medios['Importe gastado (USD)'], errors='coerce').fillna(0)
    df_medios['Impresiones'] = pd.to_numeric(df_medios['Impresiones'], errors='coerce').fillna(0)
    df_medios['Alcance'] = pd.to_numeric(df_medios['Alcance'], errors='coerce').fillna(0)
    
    # Ordenamiento cronológico por si se requiere auditoría visual
    df_medios = df_medios.sort_values(by=['Country', 'Brand', 'Year_Week']).reset_index(drop=True)

    # =========================================================================
    # BLOQUE 1: MÉTRICAS DE MEDIOS DIGITALES (El Esfuerzo Puro)
    # =========================================================================
    # Frecuencia de Medios: Cuántas veces vio el anuncio una persona única en esta campaña
    df_medios['Frecuencia_Medios'] = np.where(
        df_medios['Alcance'] > 0, 
        df_medios['Impresiones'] / df_medios['Alcance'], 
        0
    )
    
    # CPM Real: Costo por cada 1,000 impresiones a nivel de campaña individual
    df_medios['CPM_Real'] = np.where(
        df_medios['Impresiones'] > 0,
        (df_medios['Importe gastado (USD)'] / df_medios['Impresiones']) * 1000,
        0
    )

    # =========================================================================
    # BLOQUE 2: TRANSICIÓN Y MODELADO DE PRESUPUESTO
    # =========================================================================
    # Adstock Individual: Memoria publicitaria que deja esta campaña específica en su cohorte
    # (Nota: En Power BI se sumará por semana para ver el Adstock consolidado de la marca)
    df_medios['Inversion_Adstock_USD'] = df_medios.groupby(['Country', 'Brand'])['Importe gastado (USD)'].transform(
        lambda x: aplicar_adstock(x, alpha=0.15)
    )

    # =========================================================================
    # 3. SELECCIÓN Y RETORNO DEL ENTREGABLE DE MARKETING (TABLA A)
    # =========================================================================
    # Excluimos de forma estricta cualquier columna de ventas, costos, márgenes o stock
    columnas_finales_a = [
        'Country', 'Year_Week', 'Brand', 'Nombre de la campaña', 'Campaign Consolidated', 'Type Campaign',
        'Importe gastado (USD)', 'Impresiones', 'Alcance', 'Frecuencia_Medios', 'CPM_Real', 'Inversion_Adstock_USD'
    ]
    
    # Aseguramos retornar solo las columnas existentes mapeadas
    df_tabla_a_final = df_medios[[col for col in columnas_finales_a if col in df_medios.columns]]
    
    print(f"*** TABLA A CREADA exitosamente. Detalle de campañas exportado con {len(df_tabla_a_final)} registros. ***")
    return df_tabla_a_final


# =========================================================================
# INTEGRACIÓN EN TU FUNCIÓN MAIN
# =========================================================================

def main():
    from config_paths import Datamind_PATHS, Campaigns_PATHS

    path_datamind_meli_sales = Datamind_PATHS.OUTPUT_PROCESSED_PARQUETS_DIR
    PATH_CAMPAIGNS = Campaigns_PATHS.OUTPUT_FILE_PROCESSED_EXCEL_CAMPAIGNS
    PATH_SOV = Campaigns_PATHS.OUTPUT_FILE_PROCESSED_EXCEL_SOV
    PATH_CAMPAIGNS_ANALYSIS = Campaigns_PATHS.OUTPUT_FILE_PROCESSED_EXCEL_ANALYSIS_CAMPAIGNS

    # --------------------------
    # --- LECTURA ARCHIVOS
    # -------------------------
    df_sov = pd.read_excel(PATH_SOV)
    df_datamind_meli_sales = read_files_parquets(Datamind_PATHS.OUTPUT_PROCESSED_PARQUETS_DIR)
    df_campaigns = pd.read_excel(PATH_CAMPAIGNS)
    df_sov.columns = df_sov.columns.str.strip()

    print('df_datamind_meli_sales')
    print(f'len(df_datamind_meli_sales){len(df_datamind_meli_sales)}')
    # Limpiar espacios en blanco en los nombres de las columnas para evitar KeyErrors
   


    # --------------------------
    # ---  AGREGACIÓN DE VENTAS DATAMIND
    # --- (Se agrupa de SKU/Retailer diario a nivel País, Semana y Marca)
    # ------------------------
    
    df_merged=generate_key(df_campaigns,df_datamind_meli_sales)
    
    # --------------------------
    # --- PROCESAMIENTO AVANZADO
    # -------------------------
    df_output_powerbi = calcular_metricas_avanzadas(df_merged)
    df_output_powerbi.to_excel(PATH_CAMPAIGNS_ANALYSIS)

    # Opcional: Integrar aquí la tabla df_sov si necesitas consolidar el Share of Voice

    # --------------------------
    # --- EXPORTAR RESULTADOS A TU RUTA DE PREFERENCIA
    # -------------------------
    # df_output_powerbi.to_csv("Ruta_OneDrive_SBD/Dataset_Atribucion_Final.csv", index=False, encoding='utf-8-sig')
    print("¡Dataset final procesado con éxito!")

if __name__ == "__main__":
    main()

    

 