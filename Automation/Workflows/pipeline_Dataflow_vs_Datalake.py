"""
ORQUESTADOR MAESTRO: Cerebro Flexible del Pipeline ETL Regional.

Este script es el centro de mando diseñado para ejecutar procesos de datos de forma modular. 
Su arquitectura permite "encender o apagar" módulos según la necesidad del usuario, 
simplemente comentando o descomentando las líneas en el diccionario de configuración.

Módulos que este script puede gestionar:
    • modulo_customers = 'Master_Customers.Update'
    • modulo_demand = 'Demand.Process_ETL.Update'
    • modulo_fill_rate = 'Fill_Rate.Process_ETL.Update'
    • modulo_sales = 'Sales.Process_ETL.Update'
    • modulo_products = 'Master_Products.Update_md_products' 
    • modulo_sku_review = 'Master_Products.Generate_sku_review'
    • modulo_hts = 'Master_Products.Update_File_HTS'
    • modulo_pwt = 'Master_Products.Update_File_PWT'
    • modulo_vs = 'Sales.Process_ETL.DataflowvsDatalake'
¿Qué hace exactamente este script?
 1. PREPARACIÓN: Mapea las rutas y activa el Python del VENV para evitar conflictos.
 2. EJECUCIÓN: Itera sobre los módulos seleccionados, capturando logs y códigos de salida.
 3. NOTIFICACIÓN: Genera un reporte dinámico en Outlook:
    - ÉXITO: Confirma que la data está lista para los Dataflows de Power BI Service.
    - FALLO: Detiene el flujo y lanza una ALERTA CRÍTICA para proteger la integridad de los datos.

Nota técnica: Incluye un parche para ignorar errores irrelevantes de la API COM de Outlook 
que ocurren tras un envío exitoso.
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
import pandas as pd
def alerta(path_vsDatalake):
    try:
        df = pd.read_excel(path_vsDatalake)
        
        if 'Dif GSV' in df.columns:
            # 1. Aseguramos que la columna sea tratada como números
            valores_num = pd.to_numeric(df['Dif GSV'], errors='coerce')
            
            # 2. Verificamos si AL MENOS UN registro en valor absoluto es mayor a 1
            if (valores_num.abs() > 1).any():
                return 1
                
        return 0
    except Exception as e:
        print(f"⚠️ No se pudo calcular la alerta debido al error: {e}")
        return 0



def main():
    """
    Define, itera y ejecuta secuencialmente todos los módulos ETL configurados (MODULOS_ETL). Captura el código
    de salida y el output de cada ejecución para generar un reporte de estado final. Finalmente, utiliza
    send_etl_report para notificar el resultado por correo electrónico.
    
    Args: None
    Returns: None: La función ejecuta procesos externos y envía una notificación por email
    """
    print(f'{"="*80}')
    print("--- 🔄 INICIANDO PROCESO:DATAFLOW VS DATALAKE ---")
    print(f'{"="*80}')

    #==================================================================
    # --- Definición Específica del Entorno Virtual ---
    # La ruta que proporcionaste:
    RUTA_ENTORNO = Path(r"C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace\Scripts\venv_Scripts_RMA")
    path_vsDatalake=    r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace\Data\Raw\Sales\Historic\DataflowvsDatalake.xlsx'
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
        r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace'
    )
    # Directorio donde se encuentran todos tus módulos (la carpeta 'Scripts')
    DIRECTORIO_RAIZ_MODULOS = BASE_PATH / 'Scripts'
   
    modulo_vs='Sales.Process_ETL.Dataflow_vs_Datalake'

    # Diccionario de módulos a ejecutar: {nombre_amigable: nombre_del_modulo}
    MODULOS_ETL = {
     
        "Dataflow vs Datalake":modulo_vs
    }
    
    # ===============================
    # --- EJECUCIÓN MODULOS
    # ===============================

    # Diccionario para almacenar los resultados: {nombre_amigable: (codigo_salida, output)}

    resultados_ejecucion = {}
    print("\n---  INICIANDO EJECUCIÓN PIPELINE Dataflow vs Datalake ---")

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
    BODY_TEMPLATE_FALLO = f"""
    <html>
    <body>
        <h2 style="color: red;">¡ALERTA! Fallo  en la Actualización Dataflow vs Datalake</h2>
        <p>Estimado equipo,</p>
        <p>El proceso de actualización del archivo Dataflow vs Datalake fallo
        <p>Por esta razón, el archivo <b>Dataflow vs Datalake.xlsx</b> <b>NO se ha actualizado</b> o podría contener información incompleta/errónea.</p>
        
   
        <p style="margin-top: 20px;">Ya me encuentro trabajando en la solución  para restaurar el flujo correcto. Les notificaré de inmediato en cuanto la nueva versión esté limpia, actualizada y lista para trabajar.</p>
    
        <p>Lamento los inconvenientes y agradezco tu comprensión.</p>
        <p>Saludos.</p>

        
        <p>Saludos.</p>

        <p style="margin-top: 20px; font-family: Calibri, sans-serif; font-size: 11pt;">
        <b>Sebastian Nuñez.</b><br>
        Data Science & Data Base Analyst.<br>
        Stanley Black & Decker, Inc.
        </p>
        
    </body>
    </html>
    """
    
    
    BODY_TEMPLATE_EXITO_NO_ALERTA = f"""
    <html>
    <body>
        <h2 style="color: green;">✅ Archivo Dataflow vs Datalake Actualizado</h2>
        <p>Hola equipo buenos días,</p>
        <p> Les comento que el archivo <b>Dataflow vs Datalake</b> ha sido actualizado. Y no se han presentado cambios </p>
        
        <hr style="border: 1px solid #ccc;">


        <p style="margin-top: 20px;"><b>Pueden acceder al archivo a través del siguiente enlace:</b></p>
        <p style="font-size: 1.2em;">
            <a href="https://ecentral.sharepoint.com/:x:/r/sites/LAGAnalyticsDataRepository/Shared%20Documents/Analytics_Workspace/Data/Raw/Sales/Historic/DataflowvsDatalake.xlsx?d=w17acd68b392c4325b2422526cb7f9e0e&csf=1&web=1&e=iW2Eg5" target="_blank">
                🔗 Dataflow vs Datalake.xlsx
            </a>
        </p>

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

    BODY_TEMPLATE_EXITO_ALERTA = f"""
     <html>
    <body>
        <h2 style="color: green;"> Archivo Dataflow vs Datalake Actualizado</h2>
        <p>Hola equipo buenos días,</p>
        <p> Les comento que el archivo <b>Dataflow vs Datalake</b> ha sido actualizado. Y se han presentado cambios </p>
        
        <hr style="border: 1px solid #ccc;">


        <p style="margin-top: 20px;"><b>Pueden acceder al archivo a través del siguiente enlace:</b></p>
        <p style="font-size: 1.2em;">
            <a href="https://ecentral.sharepoint.com/:x:/r/sites/LAGAnalyticsDataRepository/Shared%20Documents/Analytics_Workspace/Data/Raw/Sales/Historic/DataflowvsDatalake.xlsx?d=w17acd68b392c4325b2422526cb7f9e0e&csf=1&web=1&e=iW2Eg5" target="_blank">
                🔗 Dataflow vs Datalake.xlsx
            </a>
        </p>

        <p>Saludos.</p>

        <p style="margin-top: 20px; font-family: Calibri, sans-serif; font-size: 11pt;">
        <b>Sebastian Nuñez.</b><br>
        Data Scientist & Data Base Analyst.<br>
        Stanley Black & Decker, Inc.
        </p>
    </body>
    </html>
    """
    BODY_TEMPLATES_ALERTA = [BODY_TEMPLATE_EXITO_ALERTA,BODY_TEMPLATE_FALLO]
    BODY_TEMPLATES_NO_ALERTA = [BODY_TEMPLATE_EXITO_NO_ALERTA,BODY_TEMPLATE_FALLO]
      
    SUBJECTS_ETL_ALERTA = [
         "ALERTA DE CAMBIOS: Archivo Dataflow vs Datalake Actualizado ",  # Éxito (lst_subject[0])

        "ALERTA:Fallo el proceso Dataflow vs Datalake"  # Éxito (lst_subject[0])
    ]

    SUBJECTS_ETL_NO_ALERTA = [
         "Archivo Dataflow vs Datalake Actualizado",  # Éxito (lst_subject[0])
        "ALERTA:Fallo el proceso Dataflow vs Datalake"  # Éxito (lst_subject[0])
    ]
    # =============================
    # --- ENVIO DE CORREO
    # =============================
    var_alerta=alerta(path_vsDatalake)
    print(f'alerta_ {var_alerta}')
    # Llamada a la función
    if var_alerta==0:
        lst_email = ['sebastian.nunez@sbdinc.com','Adrian.Orozco@sbdinc.com','Horacio.Osorio@sbdinc.com']

        send_etl_report(
            resultados_ejecucion, 
            lst_email, 
            SUBJECTS_ETL_NO_ALERTA, 
            BODY_TEMPLATES_NO_ALERTA
        )
    else:
        lst_email = ['sebastian.nunez@sbdinc.com','Adrian.Orozco@sbdinc.com','Horacio.Osorio@sbdinc.com']
        #lst_email = ['sebastian.nunez@sbdinc.com']
        send_etl_report(
            resultados_ejecucion, 
            lst_email, 
            SUBJECTS_ETL_ALERTA, 
            BODY_TEMPLATES_ALERTA
        )

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Solo reportamos si NO es el error específico de Outlook COM
        outlook_error_code = -2147352567 
        if hasattr(e, 'args') and e.args and e.args[0] == outlook_error_code:
            print("\n| ✅ Correo enviado con éxito.")
            # Salida exitosa (código 0) aunque hubo una excepción COM "fantasma"
            sys.exit(0) 
        else:
            # Si es otro error inesperado, lo mostramos
            print(f"\n| 🚨 ERROR CRÍTICO INESPERADO en Pipeline: {e}")
            sys.exit(1)
    time.sleep(10) # Puedes dejarlo para mayor seguridad en el cierre del proceso.


