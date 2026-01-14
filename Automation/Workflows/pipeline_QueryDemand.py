"""
Control Maestro de Flujo de Trabajo (Workflow Orchestrator) del proyecto ETL.

Este script es el punto de entrada principal para la ejecución secuencial de todos
los procesos ETL (Demand, Fill Rate, Sales, Master Data). Gestiona la ejecución
mediante subprocess y, al finalizar, genera y envía un reporte de estado
detallado (éxito o fallo) por correo electrónico utilizando la función send_etl_report.

Nota: Contiene manejo de excepciones específico para suprimir errores COM de Outlook
después de un envío exitoso.

"""

#==================
#--- LIBRERIAS
#==================
import subprocess
import win32com.client as win32
import time
from pathlib import Path
import sys
import snowflake.connector

from .Emails import execute_file_py,send_etl_report

def main():
    """
    Define, itera y ejecuta secuencialmente todos los módulos ETL configurados (MODULOS_ETL). Captura el código
    de salida y el output de cada ejecución para generar un reporte de estado final. Finalmente, utiliza
    send_etl_report para notificar el resultado por correo electrónico.
    
    Args: None
    Returns: None: La función ejecuta procesos externos y envía una notificación por email
    """
    print(f'{"="*80}')
    print("--- 🔄 INICIANDO PROCESO:UPDATES ETL ---")
    print(f'{"="*80}')

    #==================================================================
    # --- Definición Específica del Entorno Virtual ---

    # La ruta que proporcionaste:
    RUTA_ENTORNO = Path(r"C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Scripts\venv_Scripts_RMA")

    # En Windows, el ejecutable está dentro de la carpeta 'Scripts'
    # y el nombre del archivo es 'python.exe'
    PYTHON_EXEC_PATH = str(RUTA_ENTORNO / "Scripts" / "python.exe")

    # Opcional: Una verificación rápida para asegurarte de que la ruta es correcta
    if not Path(PYTHON_EXEC_PATH).exists():
        print(f"ADVERTENCIA CRÍTICA: El ejecutable '{PYTHON_EXEC_PATH}' no existe. Revisa la ruta.")
    else:
        print(f"Usando ejecutable de entorno virtual: {PYTHON_EXEC_PATH}")



    
    # =========================================================================
    #  CONFIGURACIÓN DE RUTAS Y MÓDULOS
    # =========================================================================
    BASE_PATH = Path(
       # r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics'
        r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics'
    )
    # Directorio donde se encuentran todos tus módulos (la carpeta 'Scripts')
    DIRECTORIO_RAIZ_MODULOS = BASE_PATH / 'Scripts'
    # Nombre del MÓDULO ETL que queremos ejecutar (NO la ruta del archivo)
    # Corresponde a Master_Products/Update_md_products.py
    
    #modulo_products = 'Master_Products.Update_md_products' 
    #modulo_sku_review='Master_Products.Generate_sku_review'
    #modulo_hts='Master_Products.Update_File_HTS'
    #modulo_pwt='Master_Products.Update_File_PWT'
    #modulo_customers='Master_Customers.Update'
    #modulo_demand='Demand.Process_ETL.Update'
    #modulo_fill_rate='Fill_Rate.Process_ETL.Update'
    #modulo_sales='Sales.Process_ETL.Update'
    #modulo_QuerySkuName='Snowflake.Conections.QueryNameSku'
    modulo_QueryDemand='Snowflake.Conection.QueryDemand'

    # Diccionario de módulos a ejecutar: {nombre_amigable: nombre_del_modulo}
    MODULOS_ETL = {
        #"Demand Update": modulo_demand,
        #"Fill Rate Update": modulo_fill_rate,
        #"Sales Update": modulo_sales,
        #"Master Customers Update": modulo_customers,

        #"Master Products Update": modulo_products,
        #"HTS Update": modulo_hts,
        #"PWT Update": modulo_pwt,
        #"SKU Review Generation": modulo_sku_review,

        "QueryDemand": modulo_QueryDemand,
        #"QuerySkuName": modulo_QuerySkuName

    }
    
    # ===============================
    # --- EJECUCIÓN MODULOS
    # ===============================

    # Diccionario para almacenar los resultados: {nombre_amigable: (codigo_salida, output)}

    resultados_ejecucion = {}
    print("\n--- 🚀 INICIANDO EJECUCIÓN SECUENCIAL ---")

    for nombre_amigable, modulo in MODULOS_ETL.items():
        #print(f"\n| Ejecutando: {nombre_amigable} ({modulo})...")
        # Capturando CÓDIGO y OUTPUT con la función mejorada
        code, output = execute_file_py(modulo, DIRECTORIO_RAIZ_MODULOS,PYTHON_EXEC_PATH) 
        print(output)
        # Almacenando el resultado completo
        resultados_ejecucion[nombre_amigable] = (code, output) 

    # =================================================== 
    # --- DEFINICION SUBJECT Y BOD
    # ===================================================
    SUBJECTS_ETL = [
        "Información de Demanda actualizada",  # Éxito (lst_subject[0])
        "🚨 ERROR CRÍTICO: Fallo en ETL - Archivo Demanda NO GENERADO" # Fallo (lst_subject[1])
    ]
    # Definición de Body (Plantillas HTML COMPLETAS)
        # lst_body[0]: Body de Éxito
        # Puedes usar {total_modulos}
    BODY_TEMPLATE_EXITO = f"""
    <html>
    <body>
        <h2 style="color: green;">✅ La query que extrae la información de demanda de snowflake del presente mes en adelante se ejecuto de manera exitosa
        y está lista para revisión</h2>
        
        <p>Saludos.</p>

        <p style="margin-top: 20px; font-family: Calibri, sans-serif; font-size: 11pt;">
        <b>Sebastian Nuñez.</b><br>
        Data Scientist & Data Base Analyst.<br>
        Stanley Black & Decker, Inc.
        </p>
    </body>
    </html>
    """

    #### **B. Body de Fallo (`lst_body[1]`): Error en la Generación**

    BODY_TEMPLATE_FALLO = f"""
    <html>
    <body>
        <h2 style="color: red;">🚨 ¡ALERTA! Fallos en la ejecución de la query de demanda de snowflake</h2>
        <p>Estimado equipo,</p>
        <p>La query ha fallado. Se detectaron errores
        <p>Por esta razón, la información de<b>Demanda</b> <b>NO se ha actualizado</b> o podría contener información incompleta/errónea.</p>
        
        
        <p><b>Detalle de Módulos con Error:</b></p>
        {{detalle_errores}}
        
        <p>Saludos.</p>

        <p style="margin-top: 20px; font-family: Calibri, sans-serif; font-size: 11pt;">
        <b>Sebastian Nuñez.</b><br>
        Data Science & Data Base Analyst.<br>
        Stanley Black & Decker, Inc.
        </p>
        
    </body>
    </html>
    """
    BODY_TEMPLATES = [BODY_TEMPLATE_EXITO, BODY_TEMPLATE_FALLO]
    # =============================
    # --- ENVIO DE CORREO
    # =============================
    
    # Llamada a la función
    lst_email = ['sebastian.nunez@sbdinc.com']
    send_etl_report(
        resultados_ejecucion, 
        lst_email, 
        SUBJECTS_ETL, 
        BODY_TEMPLATES
    )

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Solo reportamos si NO es el error específico de Outlook COM
        outlook_error_code = -2147352567 
        if hasattr(e, 'args') and e.args and e.args[0] == outlook_error_code:
            print("\n| ✅ Correo enviado con éxito (Error de limpieza COM suprimido).")
            # Salida exitosa (código 0) aunque hubo una excepción COM "fantasma"
            sys.exit(0) 
        else:
            # Si es otro error inesperado, lo mostramos
            print(f"\n| 🚨 ERROR CRÍTICO INESPERADO en Pipeline: {e}")
            sys.exit(1)
    time.sleep(10) # Puedes dejarlo para mayor seguridad en el cierre del proceso.


