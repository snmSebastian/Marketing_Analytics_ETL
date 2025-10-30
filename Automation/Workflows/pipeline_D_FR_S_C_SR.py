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
    
    # =========================================================================
    #  CONFIGURACIÓN DE RUTAS Y MÓDULOS
    # =========================================================================
    BASE_PATH = Path(
        r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics'
    )
    # Directorio donde se encuentran todos tus módulos (la carpeta 'Scripts')
    DIRECTORIO_RAIZ_MODULOS = BASE_PATH / 'Scripts'
    # Nombre del MÓDULO ETL que queremos ejecutar (NO la ruta del archivo)
    # Corresponde a Master_Products/Update_md_products.py
    
    #modulo_products = 'Master_Products.Update_md_products' 
    modulo_sku_review='Master_Products.Generate_sku_review'
    #modulo_hts='Master_Products.Update_File_HTS'
    #modulo_pwt='Master_Products.Update_File_PWT'
    modulo_customers='Master_Customers.Update'
    modulo_demand='Demand.Process_ETL.Update'
    modulo_fill_rate='Fill_Rate.Process_ETL.Update'
    modulo_sales='Sales.Process_ETL.Update'

    # Diccionario de módulos a ejecutar: {nombre_amigable: nombre_del_modulo}
    MODULOS_ETL = {
        "Demand Update": modulo_demand,
        "Fill Rate Update": modulo_fill_rate,
        "Sales Update": modulo_sales,
        "Master Customers Update": modulo_customers,

        #"Master Products Update": modulo_products,
        #"HTS Update": modulo_hts,
        #"PWT Update": modulo_pwt,
        "SKU Review Generation": modulo_sku_review

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
        code, output = execute_file_py(modulo, DIRECTORIO_RAIZ_MODULOS) 
        print(output)
        # Almacenando el resultado completo
        resultados_ejecucion[nombre_amigable] = (code, output) 

    # =================================================== 
    # --- DEFINICION SUBJECT Y BOD
    # ===================================================
    SUBJECTS_ETL = [
        "Proceso ETL Finalizado con Éxito",                      # Éxito (lst_subject[0])
        "ERROR: {num} de {total} Módulos Fallaron en ETL Diario"  # Fallo (lst_subject[1])
    ]
    # Definición de Body (Plantillas HTML COMPLETAS)
        # lst_body[0]: Body de Éxito
        # Puedes usar {total_modulos}
    BODY_TEMPLATE_EXITO = f"""
    <html>
    <body>
        <h2 style="color: green;">✅ ¡Proceso ETL Finalizado con Éxito!</h2>
        <p>Hola,</p>
        <p>La orquestación ETL se ejecutó correctamente, procesando <b>{{total_modulos}} módulos</b> sin registrar errores críticos.</p>
        
        <hr style="border: 1px solid #ccc;">

        <h3>1. Acciones Automáticas (Dataflows)</h3>
        <p>A continuación, se procederá automáticamente a la <b>actualización de los Dataflows</b> en Power BI Service:</p>
        <ul>
            <li>Demand</li>
            <li>Sales</li>
            <li>Fill Rate</li>
            <li>Master Customers</li>
        </ul>

        <h3>2. Revisión de SKUs (Acción Manual)</h3>
        <p>El archivo <b>Sku_for_Review.xlsx</b> ha sido generado y actualizado en SharePoint con los siguientes criterios:</p>
        <ul>
            <li><b>Nuevos SKUs</b> (Para categorización inicial).</li>
            <li><b>SKUs existentes</b> que requieren revisión porque su SKU base comparte diferentes categorías.</li>
            <li><b>SKUs aleatorios</b> por revisar (Mantenimiento de calidad).</li>
        </ul>
        
        <p style="font-weight: bold; color: #cc0000; margin-top: 15px;">
            ⚠️ Acción Requerida: Marcar en la columna <b>"check_sku"</b> con <b>'OK'</b> los SKUs que ya hayan sido revisados.
        </p>

        <p style="margin-top: 20px;">Accede al archivo a través del siguiente enlace:</p>
        <p style="font-size: 1.2em;">
            <a href="https://ecentral.sharepoint.com/:x:/r/sites/GTS_Marketing/LAG-IPGPDR/_layouts/15/Doc.aspx?sourcedoc=%7B956C36AE-F7E5-4F48-9C6E-FD516560DBDA%7D&file=Sku_for_Review.xlsx&action=default&mobileredirect=true" target="_blank">
                🔗 Abrir Sku_for_Review.xlsx
            </a>
        </p>

        <p style="margin-top: 30px;">Saludos,</p>
        
        <p style="margin-top: 20px; font-family: Calibri, sans-serif; font-size: 11pt;">
            <b>Sebastian Nuñez.</b><br>
            Data Science & Data Base Analyst.<br>
            Stanley Black & Decker, Inc.
        </p>
    </body>
    </html>
    """
    BODY_TEMPLATE_FALLO = f"""
    <html>
    <body>
        <h2 style="color: red;">🚨 ¡ATENCIÓN CRÍTICA! Fallos en la Orquestación ETL</h2>
        <p>Estimado equipo,</p>
        <p>El proceso ETL falló. Se detectaron <b>{{num_fallidos}} errores</b> de un total de <b>{{total_modulos}} módulos</b>. Se requiere revisión inmediata.</p>
        
        <p style="font-weight: bold; color: red; margin-top: 15px;">
            🚫 Proceso Detenido: Hasta que el problema se solucione, la <b>actualización de los Dataflows</b> de Demand, Sales, Fill Rate y Master Customers ha sido <b>paralizada</b> para evitar inyectar datos corruptos.
        </p>
        
        <hr style="border: 1px solid #ccc;">

        <h3>Resumen de Errores:</h3>
        {{detalle_errores}} 
        
        <p style="margin-top: 20px;">Por favor, revise los logs en el servidor para el detalle completo.</p>
        
        <p style="margin-top: 30px;">Saludos.</p>
        
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


