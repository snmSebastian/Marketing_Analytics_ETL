import subprocess
import win32com.client as win32
import time

from pathlib import Path

BASE_PATH = Path(
    r'C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics'
)

ruta_archivo_update_md_product = BASE_PATH / 'Scripts' / 'Master_Products' / 'Update_md_products_File.py'

# Intentar ejecutar el archivo primero
process_update_md_product = subprocess.Popen(["python", ruta_archivo_update_md_product])
# Esperar a que el proceso termine y obtener el código de salida
codigo_salida_update_md_product = process_update_md_product.wait()
print("Finalizando actualizacion md products")
time.sleep(10)

correo_exitoso = {
    "subject": "Actualizacion MD Products exitosa",
    "body": "Los codigos de actualizacion:\n Updatr_md_products.py Se han ejecutado de manera existosa"
}



# Función para enviar correo electrónico a una lista de destinatarios
def enviar_correo(destinatarios, subject, body):
    outlook = win32.Dispatch("Outlook.Application")
    for destinatario in destinatarios:
        mail = outlook.CreateItem(0)
        mail.To = destinatario
        mail.Subject = subject
        mail.Body = body
        mail.Send()

# Lista de destinatarios de correo electrónico
#lista_correos = ['sebastian.nunez@sbdinc.com', 'adrian.orozco@sbdinc.com']
lista_correos = ['sebastian.nunez@sbdinc.com']


# Verificar el código de salida y enviar correo correspondiente a todos los destinatarios
if (codigo_salida_update_md_product == 0):
    print("Los codigos de actualizacion:\n Update_md _products.py Se han ejecutado de manera existosa\n Enviando correo...")
    enviar_correo(lista_correos, correo_exitoso["subject"], correo_exitoso["body"])
time.sleep(10)
