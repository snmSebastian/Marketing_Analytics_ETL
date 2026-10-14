import pandas as pd
import numpy as np

from Sales.Process_ETL.Update import read_files_parquets

# =========================================================================
# FUNCIONES DE SOPORTE PARA MÉTRICAS AVANZADAS
# =========================================================================

def  generate_sales_agg(df_datamind_meli_sales):


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

   
    
    df_datamind_meli_sales['key'] = (
        df_datamind_meli_sales['Country'].astype(str) + "_" + 
        df_datamind_meli_sales['Brand'].astype(str) + "_" + 
        df_datamind_meli_sales['Year_Week'].astype(str)
    )
    df_datamind_meli_sales['key'] = df_datamind_meli_sales['key'].astype(str).str.lower().str.replace(' ', '')
   
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
   
    return df_sales_agg

def generate_campaings_agg(df_campaigns):
    # =========================================================================
    # 0. INYECCIÓN DEL PRESUPUESTO SEMANAL CONSOLIDADO
    # =========================================================================
    # Agrupamos el archivo de campañas para saber cuánto se gastó en total por marca/semana
    df_camp_clean = df_campaigns.copy()
    df_camp_clean['Country'] = df_camp_clean['Country'].replace({
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
    df_camp_clean['Week End'] = df_camp_clean['Semana'].str[-10:]
    df_camp_clean['Week End'] = pd.to_datetime(df_camp_clean['Week End'], errors='coerce')
    iso_cal_sales = df_camp_clean['Week End'].dt.isocalendar()
    df_camp_clean['Year_Week'] = (iso_cal_sales.year.astype(str) + iso_cal_sales.week.astype(str).str.zfill(2)).astype(int)

    df_camp_clean['key']=df_camp_clean['Country']+'_'+df_camp_clean['Brand']+'_'+df_camp_clean['Year_Week'].astype(str)
    df_camp_clean['key'] = df_camp_clean['key'].astype(str).str.lower().str.replace(' ', '')

     #----CAMPAÑAS AGRUPADAS POR KEY
    df_camp_clean=df_camp_clean.groupby(['key']).agg({
         'Country': 'first',      
        'Brand': 'first',
        'Year_Week': 'first',
        'Alcance': 'sum',
        'Impresiones': 'sum',
        'Importe gastado (USD)': 'sum' 
    }).reset_index()     

    return df_camp_clean

def merge_info(df_sales_agg,df_campaigns_agg):
    df_merge=pd.merge(
        df_campaigns_agg,
        df_sales_agg[['key','Venta neta','Venta costo','Venta bruta','Unidades vendidas','Precio Publico Estimado']],
        on='key',
        how='left'
    )
    df_merge=df_merge.sort_values(by=['key'])
    return df_merge

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


def metrics(df_merge):
    """
    Agrupa informacion de venta y campañas por country-brand-yearweek, y genera metricas
    """
    df_camp_sls=df_merge.copy()
    # Orden cronológico estricto para no corromper las series temporales
    df_camp_sls = df_camp_sls.sort_values(by=['key']).reset_index(drop=True)
    df_camp_sls=df_camp_sls.rename(columns={'Importe gastado (USD)':'Inversion_Total_Semana'})

    # =========================================================================
    #  MÉTRICAS DE MEDIOS DIGITALES (El Esfuerzo Puro)
    # =========================================================================
    # Frecuencia de Medios: Cuántas veces vio el anuncio una persona única en esta campaña
    df_camp_sls['Frecuencia_Medios'] = np.where(
        df_camp_sls['Alcance'] > 0, 
        df_camp_sls['Impresiones'] / df_camp_sls['Alcance'], 
        0
    )
    
    # CPM Real: Costo por cada 1,000 impresiones a nivel de campaña individual
    df_camp_sls['CPM_Real'] = np.where(
        df_camp_sls['Impresiones'] > 0,
        (df_camp_sls['Inversion_Total_Semana'] / df_camp_sls['Impresiones']) * 1000,
        0
    )
    # =========================================================================
    #  MÉTRICAS DE TRANSICIÓN Y MODELADO
    # =========================================================================
    # Inversión Adstock: Usa la inversión consolidada de la semana sobre la línea temporal continua
    df_camp_sls['Inversion_Adstock_USD'] = df_camp_sls.groupby(['Country', 'Brand'])['Inversion_Total_Semana'].transform(
        lambda x: aplicar_adstock(x, alpha=0.15)
    )
    
    '-----------NOTA--'
    # =======================================================================
    #-- EVALUAR SI HAY UNA MEJOR OPCION PARA CLASIFICAR LE EFICIENCIA
    #  eficiente si una campaña saturada vendio mas que el promedio de semanas anteriores donde estuvo activa
    # la saturacion debe ser con inversion adstock
    # =======================================================================
        
    # Umbral de saturación por País y Marca basado en el gasto consolidado de la semana
    
    df_camp_sls['Umbral_Saturacion'] = df_camp_sls.groupby(['Country', 'Brand'])['Inversion_Total_Semana'].transform(
        lambda x: x.quantile(0.80)
    ) 
    df_camp_sls['sales_avg'] = (
        df_camp_sls.groupby(['Country', 'Brand'])['Venta neta']
        .transform(lambda x: x.rolling(window=4, min_periods=1).mean())
    )

    # esta medida no es apropiada para el contexto
    df_camp_sls['Alerta_Saturacion'] = np.where(
        (df_camp_sls['Inversion_Total_Semana'] > df_camp_sls['Umbral_Saturacion']) & 
        (df_camp_sls['Venta neta'] < df_camp_sls['sales_avg']),
        "Saturado: Reducir Presupuesto",
        "Eficiente"
    )


    #df_merge = df_merge.drop(columns=['Margen_Bruto_Total_Temp', 'Umbral_Saturacion'], errors='ignore')


    # =========================================================================
    # 5. SELECCIÓN VARIABLES
    # =========================================================================
    columnas_finales_b = [
        'Country', 'Year_Week', 'Brand', 'Venta neta',
        'Venta bruta', 'Venta costo', 'Unidades vendidas', 'Precio Publico Estimado',       
          'Margen_Bruto_Total', 'Inversion_Total_Semana', 'Inversion_Adstock_USD', 'Alerta_Saturacion',
        'Margen_Base_Organico', 'Margen_Incremental_Estimado',
        'Alerta_Disponibilidad_Stock', 'Costo_Oportunidad_Stock_USD', 'Profundidad_Descuento_Retail'
    ]
    
    #df_merge = df_merge[[col for col in columnas_finales_b if col in df_merge.columns]]
    #print(f"*** Metricas Avanzadas: {len(df_merge)} ***")
    
    return df_camp_sls



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

    #===========================================================
    #--- ESTE PEDASO DE CODIGO SE CREO SOLO PARA CREAR UN  ARCHIVO DE VENTAS AGRUPADAS
    #===========================================================
    df_sales_agg= generate_sales_agg(df_datamind_meli_sales)
    print('df_datamind_meli_sales')
    df_campaigns_agg=generate_campaings_agg(df_campaigns)
    df_camp_sales=merge_info(df_sales_agg,df_campaigns_agg)

    df_metrics=metrics(df_camp_sales)
     
    print('df_sales_metrics')

    df_metrics.to_excel(PATH_CAMPAIGNS_ANALYSIS)

    print("¡Dataset final procesado con éxito!")

if __name__ == "__main__":
    main()

    

 