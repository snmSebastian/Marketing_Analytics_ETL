import subprocess
import win32com.client as win32
import time
from pathlib import Path

# =========================================================================
# 1. CONFIGURACIÓN DE RUTAS Y MÓDULOS
# =========================================================================

BASE_PATH = Path(
    r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics'
)

# Directorio donde se encuentran todos tus módulos (la carpeta 'Scripts')
DIRECTORIO_RAIZ_MODULOS = BASE_PATH / 'Scripts'

# Nombre del MÓDULO ETL que queremos ejecutar (NO la ruta del archivo)
# Corresponde a Master_Products/Update_md_products.py
MODULO_ETL_A_EJECUTAR = 'Master_Products.Update_md_products' 

# =========================================================================
# 2. FUNCIÓN DE CORREO
# =========================================================================

def enviar_correo(destinatarios: list[str], subject: str, body: str):
    """Envía un correo electrónico a una lista de destinatarios usando Outlook."""
    try:
        outlook = win32.Dispatch("Outlook.Application")
    except Exception as e:
        print(f"Error al iniciar Outlook: {e}")
        return

    for destinatario in destinatarios:
        mail = outlook.CreateItem(0)
        mail.To = destinatario
        mail.Subject = subject
        # Usamos HTMLBody para manejar mejor los saltos de línea (\n)
        mail.HTMLBody = f'<p>{body.replace("\n", "<br>")}</p>'
        try:
            mail.Send()
        except Exception as e:
            print(f"Error al enviar correo a {destinatario}: {e}")

# =========================================================================
# 3. EJECUCIÓN DEL PROCESO ETL COMO MÓDULO
# =========================================================================

print("=======================================================")
print("--- 🔄 INICIANDO PROCESO: MD PRODUCTS UPDATE ETL ---")
print("=======================================================")

try:
    # Ejecutar el script ETL usando 'python -m' y estableciendo el CWD (Current Working Directory)
    # CWD debe ser la carpeta 'Scripts' para que Python encuentre 'Master_Products' como paquete.
    process_update_md_product = subprocess.Popen(
        ["python", "-m", MODULO_ETL_A_EJECUTAR],
        cwd=DIRECTORIO_RAIZ_MODULOS
    )
    
    # Esperar a que el proceso termine
    codigo_salida_update_md_product = process_update_md_product.wait()
    print("Finalizando actualizacion md products")

except FileNotFoundError:
    codigo_salida_update_md_product = 99
    print(f"Error: No se encontró el ejecutable 'python' o la ruta {DIRECTORIO_RAIZ_MODULOS} no es válida.")

time.sleep(10) # Espera post-ejecución

# =========================================================================
# 4. NOTIFICACIÓN DE ESTADO
# =========================================================================

lista_correos = ['sebastian.nunez@sbdinc.com']

if (codigo_salida_update_md_product == 0):
    correo_exitoso = {
        "subject": "✅ Actualizacion MD Products exitosa",
        "body": "Los códigos de actualización:\n Master_Products.Update_md_products se han ejecutado de manera existosa (Código 0)."
    }
    print("Los códigos se ejecutaron con éxito. Enviando correo...")
    enviar_correo(lista_correos, correo_exitoso["subject"], correo_exitoso["body"])

else:
    correo_fallido = {
        "subject": f"❌ ERROR en Actualización MD Products (Código {codigo_salida_update_md_product})",
        "body": f"⚠️ El proceso Master_Products.Update_md_products falló.\nEl código de salida fue: {codigo_salida_update_md_product}.\nPor favor, revise el log del proceso."
    }
    print(f"Error detectado (Código {codigo_salida_update_md_product}). Enviando correo de alerta...")
    enviar_correo(lista_correos, correo_fallido["subject"], correo_fallido["body"])

time.sleep(10)