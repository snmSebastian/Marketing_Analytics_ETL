"""
CAPA DE ORQUESTACIÓN Y NOTIFICACIONES
-------------------------------------
Este módulo es el motor central que se encarga de dos tareas críticas:
1. Ejecutar de forma segura los scripts de Python (ETLs) usando subprocesos aislados.
2. Gestionar la mensajería automática vía Outlook para reportar si todo salió bien o si algo explotó.

Funciones clave:
    * execute_file_py: Lanza un módulo de Python, captura su resultado y detecta si terminó en error 
                      sin detener el flujo principal.
    * correo: Motor de envío que habla con la API de Outlook (COM). Incluye limpieza profunda de 
              memoria para que Outlook no se quede "colgado".
    * send_etl_report: El "juez" del proceso. Analiza los resultados de todos los módulos, decide si 
                       el estado global es Éxito o Fallo, y arma el reporte HTML dinámico.
"""
#====================
#--- LIBRERIAS
#====================
import subprocess
import win32com.client as win32
import pythoncom
import time
from pathlib import Path
from typing import Dict, Tuple
import gc
import os




def execute_file_py(modulo: str, directorio: Path,PYTHON_EXECUTABLE) -> tuple[int, str]:
    """
    Ejecuta un módulo Python y captura el código de salida y el output de error.
    Retorna: (código_salida, mensaje_error_o_output)
    """
    try:
        # 1. Copiamos todo el contexto actual (incluye el VENV activo)
        entorno_hijo = os.environ.copy()
        # 2. Agregamos solo el encoding sin borrar el resto
        entorno_hijo["PYTHONIOENCODING"] = "utf-8"
        # Usamos subprocess.run() que es más simple y bloquea hasta terminar.
        # Capturamos stdout y stderr para el reporte.
        command = [PYTHON_EXECUTABLE, "-m", modulo]
        
        resultado = subprocess.run(
            command,
            cwd=directorio,
            capture_output=True, # Captura stdout y stderr
            text=True,           # Decodifica el output a texto
            check=False,
            env=entorno_hijo
            #env={"PYTHONIOENCODING": "utf-8"}          # No lanza excepción por código de salida != 0
        )
        
        codigo_salida = resultado.returncode
        
        if codigo_salida != 0:
            # Si falla, retornamos el stderr (el error del script)
            output = resultado.stderr
            print(f"| ❌ ERROR al ejecutar {modulo}. Código: {codigo_salida}")
        else:
            # Si tiene éxito, retornamos el stdout (para un posible log de éxito)
            output = resultado.stdout
            print(f"| ✅ Éxito al ejecutar {modulo}")
            
        return codigo_salida, output.strip() # Devolvemos código y output/error

    except FileNotFoundError:
        # Esto ocurre si 'python' no está en PATH o el 'directorio' es inválido
        error_msg = f"ERROR CRÍTICO (99): No se encontró 'python' o ruta {directorio} inválida."
        print(error_msg)
        return 99, error_msg
    except Exception as e:
        error_msg = f"ERROR INESPERADO (100) en orquestación: {e}"
        print(error_msg)
        return 100, error_msg

def correo(destinatarios: list[str], subject: str, body: str):
    """Envía un correo electrónico a una lista de destinatarios usando Outlook."""
    outlook=None
    mail=None
    
    if not destinatarios:
        print("Advertencia: No hay destinatarios definidos.")
        return

    try:
        outlook = win32.Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        
        # Unir todos los destinatarios en una sola cadena (más eficiente)
        mail.To = "; ".join(destinatarios)
        mail.Subject = subject
        
        # Usamos HTMLBody para manejar mejor los saltos de línea y formateo
        # Aquí es donde el Body de tu reporte ETL se verá bien.
        mail.HTMLBody = body 
        
        mail.Send()
        print(f"Correo enviado a: {mail.To}")
        time.sleep(15) # El time.sleep(10) es opcional, puede ser útil si los procesos son muy rápidos y Outlook necesita tiempo.    
    finally:
        # 💡 LIBERACIÓN EXPLÍCITA Y ROBUSTA (Tu código, que es correcto)
        if mail is not None:
             # Liberar objeto mail
             try: win32.client.CastTo(mail, 'win32com.client.Dispatch')._oleobj_.Release()
             except: pass
             del mail 
             
        if outlook is not None:
             # Liberar objeto outlook
             try: win32.client.CastTo(outlook, 'win32com.client.Dispatch')._oleobj_.Release()
             except: pass
             del outlook      
        gc.collect() 
        try:
             # Finalizar explícitamente el hilo COM
             pythoncom.CoUninitialize()
        except:
             pass

def send_etl_report(
    resultados: Dict[str, Tuple[int, str]],  # {nombre_modulo: (codigo_salida, output)}
    lst_emails: list[str],
    lst_subject: list[str],
    lst_body: list[str] # lst_body[0] = Éxito, lst_body[1] = Fallo
):
    """Selecciona el Subject y el Body del reporte basándose en listas y lo envía, 
    permitiendo la inyección de detalles en el body (placeholders)."""
    
    total_modulos = len(resultados)
    modulos_fallidos = {
        nombre: (code, out) 
        for nombre, (code, out) in resultados.items() 
        if code != 0
    }
    num_fallidos = len(modulos_fallidos)
    todos_ok = num_fallidos == 0
    
    # --- 1. Generación de Subject ---
    if todos_ok:
        # Formateo dinámico del Subject de éxito
        subject = lst_subject[0].format(total=total_modulos)
    else:
        # Formateo dinámico del Subject de fallo
        subject = lst_subject[1].format(num=num_fallidos, total=total_modulos)
    
    # --- 2. Generación del Detalle de Errores (para inyectar en el Body de Fallo) ---
    # Creamos un bloque HTML específico para los errores, que es la única parte 
    # que es dinámica en el cuerpo del correo.
    detalle_errores_items = ""

# 1. Iteramos sobre TODOS los resultados para construir la lista completa
    for nombre, (code, output) in resultados.items():
        
        # 2. Definir Estado y Estilos
        if code == 0:
            estado_texto = "Proceso finalizado con éxito."
            color = "green"
            # Para ÉXITO, mostramos un mensaje simple en el recuadro de output
        else:
            estado_texto = "❌ ERROR CRÍTICO"
            color = "red"
            # Para ERROR, mostramos el detalle del output del proceso

        # 3. Generar la viñeta (<li>) para CADA módulo
        detalle_errores_items += f"""
        <li style="margin-bottom: 20px; padding: 10px; border-bottom: 1px dashed #ccc;">
            <div style="font-size: 1.1em;">
                Módulo: <b>{nombre}</b> 
                <span style="float: right; font-size: 0.9em; color: {color};">(Código: {code})</span>
            </div>
            <div style="margin-top: 5px;">
                Estado: <span style="color: {color}; font-weight: bold;">{estado_texto}</span>
            </div>
            
        </li>
        """

    # 4. Envolvemos todos los items en una lista no ordenada (<ul>)
    # Esta variable es la que inyecta en {{detalle_errores}}
    detalle_errores = f"<ul style='list-style-type: none; padding-left: 0;'>{detalle_errores_items}</ul>"


    # --- 3. Generación de Body HTML (Totalmente Parametrizado) ---
    if todos_ok:
        # Usamos lst_body[0] (Éxito) y lo formateamos
        body_html = lst_body[0].format(total_modulos=total_modulos)
    else:
        # Usamos lst_body[1] (Fallo) y lo formateamos con variables de error
        body_html = lst_body[1].format(
            num_fallidos=num_fallidos, 
            total_modulos=total_modulos,
            detalle_errores=detalle_errores # Inyectamos el detalle dinámico
        )
            
    # 4. Envío del Correo
    print(f"Generando y enviando reporte. Estado global: {'Éxito' if todos_ok else 'Fallo'}")
    correo(lst_emails, subject, body_html)
