"""
Automatización para el Programador de Tareas: Prepara el entorno virtual (venv) y 
lanza el pipeline modular 'pipeline_pwt.py' que procesa:

 - Master_Products.Update_File_PWT

El script garantiza que la consola use UTF-8 para evitar errores de lectura, 
ejecuta el código de Python y, al finalizar, valida el código de salida ($LASTEXITCODE) 
para enviarte una notificación por correo confirmando si la carga fue exitosa o si 
el proceso falló en algún punto.
"""

# ---  Definir la ubicación del proyecto ---
$ProjectRoot = "C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace\Scripts"
$PythonExe = "$ProjectRoot\venv_Scripts_RMA\Scripts\python.exe"

# ---  Cambiar al Directorio de Trabajo ---
# CRÍTICO: Esto asegura que 'Automation.Workflows' sea importable.
Set-Location -Path $ProjectRoot
# --- . Saltarse la Política de Ejecución (Solo si es necesario para el SET) ---
Write-Host ">>> Configurando la política de ejecución..." -ForegroundColor Yellow
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force

# CLAVE: CAMBIAR LA CODIFICACIÓN DE LA CONSOLA A UTF-8 (65001)
[System.Console]::OutputEncoding = [System.Text.Encoding]::UTF8
cmd /c "chcp 65001" | Out-Null # Asegura que la consola subyacente use UTF-8

# ---  EJECUTAR EL PIPELINE DIRECTAMENTE CON EL BINARIO DEL VENV ---
Write-Host "Iniciando pipeline modular con el interprete de VENV..."
# ...
& $PythonExe -m Automation.Workflows.pipeline_pwt
# --- 5. Manejo de Errores ---
# $LASTEXITCODE captura el código de retorno (0 para éxito) del último proceso (python.exe)
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Ejecución exitosa." -ForegroundColor Green
}
else {
    Write-Host "❌ ERROR: Fallo en la ejecución. Código de error: $LASTEXITCODE" -ForegroundColor Red
}

# --- 6. Pausa para Depuración ---
Write-Host "Revisión completa. La ventana se cerrará en 10 segundos..." -ForegroundColor Cyan
Start-Sleep -Seconds 5