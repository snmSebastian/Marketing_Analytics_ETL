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
    print('df_sales_agg')  
   
    return df_sales_agg



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


def metrics_sales_agg(df_sales_agg, df_campaigns):
    """
    TABLA 2: Genera la tabla consolidada de hechos de retail (facts_sales_agg).
    Calcula Adstock continuo, Línea de Base y Margen Incremental a nivel de País-Marca-Semana.
    """
    # Evitar advertencias de copia en la memoria de Pandas
    df_retail = df_sales_agg.copy()
    
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
 
    df_camp_clean['Importe gastado (USD)'] = pd.to_numeric(df_camp_clean['Importe gastado (USD)'], errors='coerce').fillna(0)
  

    inv_semanal = df_camp_clean.groupby(['key'])['Importe gastado (USD)'].sum().reset_index()
    inv_semanal.rename(columns={'Importe gastado (USD)': 'Inversion_Total_Semana'}, inplace=True)

    df_retail['key']=df_retail['Country']+'_'+df_retail['Brand']+'_'+df_retail['Year_Week'].astype(str)
    df_retail['key'] = df_retail['key'].astype(str).str.lower().str.replace(' ', '')

    # Cruzamos el gasto total de la semana hacia nuestra tabla de ventas
    df_retail = pd.merge(df_retail, inv_semanal, on='key', how='left')
    df_retail['Inversion_Total_Semana'] = df_retail['Inversion_Total_Semana'].fillna(0)
    



    # Orden cronológico estricto para no corromper las series temporales
    df_retail = df_retail.sort_values(by=['Country', 'Brand', 'Year_Week']).reset_index(drop=True)

    # =========================================================================
    # BLOQUE 2: MÉTRICAS DE TRANSICIÓN Y MODELADO (El Puente)
    # =========================================================================
    # Inversión Adstock: Usa la inversión consolidada de la semana sobre la línea temporal continua
    df_retail['Inversion_Adstock_USD'] = df_retail.groupby(['Country', 'Brand'])['Inversion_Total_Semana'].transform(
        lambda x: aplicar_adstock(x, alpha=0.15)
    )
    
    # Umbral de saturación por País y Marca basado en el gasto consolidado de la semana
    df_retail['Umbral_Saturacion'] = df_retail.groupby(['Country', 'Brand'])['Inversion_Total_Semana'].transform(
        lambda x: x.quantile(0.80)
    )
    
    df_retail['Margen_Bruto_Total_Temp'] = df_retail['Venta neta'] - df_retail['Venta costo']
    df_retail['Alerta_Saturacion'] = np.where(
        (df_retail['Inversion_Total_Semana'] > df_retail['Umbral_Saturacion']) & 
        (df_retail['Margen_Bruto_Total_Temp'] < df_retail['Inversion_Total_Semana']),
        "Saturado: Reducir Presupuesto",
        "Eficiente"
    )
    df_retail = df_retail.drop(columns=['Margen_Bruto_Total_Temp', 'Umbral_Saturacion'], errors='ignore')

    # =========================================================================
    # BLOQUE 3: MÉTRICAS FINANCIERAS Y DE RETORNO (El Impacto Real)
    # =========================================================================
    df_retail['Margen_Bruto_Total'] = df_retail['Venta neta'] - df_retail['Venta costo']
    
    # Aislamiento de Línea de Base Orgánica (Usa Inversion_Total_Semana para detectar silencio)
    df_retail['Margen_Solo_Organico'] = np.where(
        df_retail['Inversion_Total_Semana'] == 0,
        df_retail['Margen_Bruto_Total'],
        np.nan
    )
    
    df_retail['Margen_Base_Organico'] = (
        df_retail.groupby(['Country', 'Brand'])['Margen_Solo_Organico']
        .transform(lambda x: x.ffill().bfill().rolling(window=4, min_periods=1).median())
    )
    df_retail['Margen_Base_Organico'] = df_retail['Margen_Base_Organico'].fillna(df_retail['Margen_Bruto_Total'] * 0.70)
    
    df_retail['Margen_Incremental_Estimado'] = (df_retail['Margen_Bruto_Total'] - df_retail['Margen_Base_Organico']).clip(lower=0)
    df_retail = df_retail.drop(columns=['Margen_Solo_Organico'], errors='ignore')

    # =========================================================================
    # BLOQUE 4: MÉTRICAS DE OPERACIONES E INVENTARIO (El Contexto Comercial)
    # =========================================================================
    mediana_unidades = df_retail.groupby(['Country','Brand'])['Unidades vendidas'].transform('median')
    df_retail['Alerta_Disponibilidad_Stock'] = np.where(
        df_retail['Unidades vendidas'] < (mediana_unidades * 0.20),
        "Riesgo de Quiebre / Stock Out",
        "Stock Sano"
    )
    
    df_retail['Margen_Unidad_Historico'] = df_retail.groupby(['Country', 'Brand'])['Margen_Bruto_Total'].transform('median') / mediana_unidades.replace(0, 1)
    
    df_retail['Costo_Oportunidad_Stock_USD'] = np.where(
        (df_retail['Alerta_Disponibilidad_Stock'] == "Riesgo de Quiebre / Stock Out") & (df_retail['Inversion_Total_Semana'] > 0),
        (mediana_unidades - df_retail['Unidades vendidas']).clip(lower=0) * df_retail['Margen_Unidad_Historico'],
        0
    )
    df_retail = df_retail.drop(columns=['Margen_Unidad_Historico'], errors='ignore')

    # Profundidad Descuento Retail
    precio_real_implicito = np.where(
        df_retail['Unidades vendidas'] > 0, 
        df_retail['Venta neta'] / df_retail['Unidades vendidas'], 
        df_retail['Precio Publico Estimado']
    )
    calculated_discount = np.where(
        df_retail['Precio Publico Estimado'] > 0,
        (df_retail['Precio Publico Estimado'] - precio_real_implicito) / df_retail['Precio Publico Estimado'],
        0
    )
    df_retail['Profundidad_Descuento_Retail'] = np.maximum(calculated_discount, 0)

    # =========================================================================
    # 5. SELECCIÓN Y RETORNO DEL ENTREGABLE FINANCIERO (TABLA 2)
    # =========================================================================
    columnas_finales_b = [
        'Country', 'Year_Week', 'Brand', 'Venta neta',
        'Venta bruta', 'Venta costo', 'Unidades vendidas', 'Precio Publico Estimado',       
          'Margen_Bruto_Total', 'Inversion_Total_Semana', 'Inversion_Adstock_USD', 'Alerta_Saturacion',
        'Margen_Base_Organico', 'Margen_Incremental_Estimado',
        'Alerta_Disponibilidad_Stock', 'Costo_Oportunidad_Stock_USD', 'Profundidad_Descuento_Retail'
    ]
    
    df_retail = df_retail[[col for col in columnas_finales_b if col in df_retail.columns]]
    print(f"*** Metricas Avanzadas: {len(df_retail)} ***")
    
    return df_retail



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
    df_sales_metrics=metrics_sales_agg(df_sales_agg, df_campaigns)
     
    print('df_sales_metrics')

    df_sales_metrics.to_excel(PATH_CAMPAIGNS_ANALYSIS)

    # Opcional: Integrar aquí la tabla df_sov si necesitas consolidar el Share of Voice

    # --------------------------
    # --- EXPORTAR RESULTADOS A TU RUTA DE PREFERENCIA
    # -------------------------
    # df_output_powerbi.to_csv("Ruta_OneDrive_SBD/Dataset_Atribucion_Final.csv", index=False, encoding='utf-8-sig')
    print("¡Dataset final procesado con éxito!")

if __name__ == "__main__":
    main()

    

 