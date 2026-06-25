
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
from Datamind.Process_Files import *


def  main():
    """
    Orquesta el pipeline integral de procesamiento para las ventas semanales de Datamind y Mercado Libre.

    Este flujo centraliza la extracción de datos multipaís (Argentina, México, Chile), 
    aplica reglas de negocio críticas (ajuste THD, precios Coppel), integra las ventas 
    de Mercado Libre y ejecuta una actualización incremental (Upsert) en el repositorio 
    de archivos Parquet segmentados.

    Pasos del flujo:
        1. Consolidación: Unifica reportes regionales de Datamind y limpia SKUs.
        2. Enriquecimiento: Clasifica canales de venta y aplica lógicas de Retailers específicos.
        3. Integración MELI: Procesa y anexa las transacciones de Mercado Libre al set semanal.
        4. Estandarización: Normaliza marcas mediante mapas de referencia globales.
        5. Upsert Incremental: Identifica periodos afectados y sobrescribe el histórico.
        6. Persistencia: Almacena resultados particionados por Año-Mes en formato Parquet.

    Returns:
        None

    💡 Nota Senior: Este orquestador garantiza la visibilidad del sell-out regional. 
    Su diseño permite escalar a nuevos países o retailers simplemente conectando 
    módulos de transformación antes de la fase de consolidación final.
    """
    from config_paths import Datamind_PATHS
    path_arg = Datamind_PATHS.INPUT_RAW_ARGENTINA_FILE
    path_mx = Datamind_PATHS.INPUT_RAW_MEXICO_FILE
    path_ch = Datamind_PATHS.INPUT_RAW_CHILE_FILE
    path_coppel = Datamind_PATHS.INPUT_RAW_COPPEL_FILE
    path_edas=Datamind_PATHS.INPUT_RAW_EDAS_FILE

    path_meli_sales=Datamind_PATHS.INPUT_RAW_MELI_SALES_FILE
    path_meli_sku=Datamind_PATHS.INPUT_RAW_MELI_SKU_FILE

    try:
        lst_columns=[
        'Source','Date','Year-Month','Year-Week','Country','Retailer','Brand','canal_venta','SKU','Sku Description',
        
        'Venta neta','Venta bruta','Venta costo','Unidades vendidas','Precio Publico Estimado'
        ]
        
        # ==========================
        # --------- DATAMIND
        # ==========================
        df_datamind_week = consolidate_files(path_arg,path_mx,path_ch)
        df_datamind_week['Source']='Datamind'
        df_datamind_week = clean_sku(df_datamind_week,'SKU')
        print('>>> Datamind: Información consolidada y SKUs limpios.')

        df_datamind_week = assing_sales_channel(df_datamind_week)
        df_datamind_week = solution_thd(df_datamind_week)
        df_datamind_week = solution_coppel(df_datamind_week, path_coppel)
        print('>>> Datamind: Canales asignados y lógica THD/Coppel aplicada.')


        #===========================
        #--- MERCADO LIBRE
        #===========================
        df_meli_sales=meli(path_meli_sales, path_meli_sku,lst_columns)
        print('>>> Mercado Libre: Datos procesados.')


        #================
        #---EDAS
        #==============
        df_edas=edas(path_edas,lst_columns)
        print('>>> EDAS: Datos procesados.')
       

        #==================================
        #--- DATAMIND - MERCADO LIBRE
        #==================================
        df_datamind_meli_edas=pd.concat([df_datamind_week[lst_columns],df_meli_sales[lst_columns],df_edas[lst_columns]],ignore_index=True)
        #-- BRAND
        
         # Crear el mapa inverso solo una vez
        brand_map_inverse = create_inverse_brand_map(BRAND_STANDARD_MAP)

        # Crear el mapa inverso solo una vez
        #  Normalizar y mapear Brand (vectorizado y rápido)
        df_datamind_meli_edas['Brand_Normalized'] = df_datamind_meli_edas['Brand'].str.upper().str.strip().str.replace(' ', '')
        df_datamind_meli_edas['Brand'] = df_datamind_meli_edas['Brand_Normalized'].map(brand_map_inverse).fillna(df_datamind_meli_edas['Brand'])
        df_datamind_meli_edas = df_datamind_meli_edas.drop(columns=['Brand_Normalized']) 

        #--- UPDATE
        #dataframe con el year-month que contiene week que sera actualizada o agregada
        print('>>> Iniciando proceso de Upsert Incremental...')
        metrics = ['Venta neta', 'Venta bruta', 'Venta costo', 'Unidades vendidas', 'Precio Publico Estimado']
        df_datamind_meli_historic = read_files_parquets(Datamind_PATHS.OUTPUT_PROCESSED_PARQUETS_DIR, df_datamind_meli_edas,'Datamind_Meli_Sales_',metrics)
        
        #agrego a los archivos year-month, la actualizacion de las semanas
        df_datamind_meli_edas_update = update_datamind_meli(df_datamind_meli_edas, df_datamind_meli_historic)
        print('>>> Consolidación final completada.')
        df_datamind_meli_edas_update['Country'] = df_datamind_meli_edas_update['Country'].str.strip().str.lower()
        df_datamind_meli_edas_update['Country'] = df_datamind_meli_edas_update['Country'].replace({'méxico': 'mexico', 'perú': 'peru','salvador':'el salvador'})

        df_datamind_meli_edas_update=clean_sku(df_datamind_meli_edas_update,'SKU')

        
        group_parquet(df_datamind_meli_edas_update, Datamind_PATHS.OUTPUT_PROCESSED_PARQUETS_DIR,name='Datamind_Meli_Sales')

        df_sku=df_datamind_meli_edas_update[['Source','Date','Year-Month','Year-Week','Country','Retailer','Brand','SKU']].drop_duplicates('SKU')
        r = r"C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Sebastian\sku_datamind.xlsx"
        df_sku.to_excel(r,index=False)
        print(">>> Proceso finalizado exitosamente. ✅")
    except Exception as e:
        print(f"Error en procesamiento de datos de Ventas Datamind: {e}")
        sys.exit(1)
        

if __name__ == "__main__":
    
    main()