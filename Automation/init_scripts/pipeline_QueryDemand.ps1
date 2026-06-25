"""
Automatización para el Programador de Tareas: Prepara el entorno virtual (venv) y 
lanza el pipeline modular 'pipeline_QueryDemand.py' que procesa:

 - Snowflake.Conection.QueryDemand

El script garantiza que la consola use UTF-8 para evitar errores de lectura, 
ejecuta el código de Python y, al finalizar, valida el código de salida ($LASTEXITCODE) 
para enviarte una notificación por correo confirmando si la carga fue exitosa o si 
el proceso falló en algún punto.
"""

# =========================================================================
# SCRIPT DE LANZAMIENTO ETL - STANLEY BLACK & DECKER
# =========================================================================

# --- 1. Definir la ubicación del proyecto ---
$ProjectRoot = "C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace\Scripts"
$PythonExe = "$ProjectRoot\venv_Scripts_RMA\Scripts\python.exe"

# --- 2. Cambiar al Directorio de Trabajo ---
# Usamos -LiteralPath para que los espacios y símbolos de OneDrive no rompan la ruta
Set-Location -LiteralPath $ProjectRoot

# --- 3. Configuración de Entorno y Codificación ---
Write-Host ">>> Configurando entorno y política de ejecución..." -ForegroundColor Yellow
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force

# Forzar UTF-8 para evitar errores con tildes y emojis en la consola
[System.Console]::OutputEncoding = [System.Text.Encoding]::UTF8
cmd /c "chcp 65001" | Out-Null

# --- 4. Ejecución del Pipeline ---
Write-Host "🚀 Iniciando pipeline modular con el interprete de VENV..." -ForegroundColor Cyan

# Definimos el módulo a ejecutar
$Modulo = "Automation.Workflows.pipeline_QueryDemand"

# Ejecutamos usando el operador de llamada (&) pero envolviendo la ruta en comillas
# Esto es vital por los espacios en 'Stanley Black & Decker'
& "$PythonExe" -m $Modulo

# --- 5. Manejo de Errores ---
# $LASTEXITCODE captura el código de retorno del proceso Python recién terminado
$Resultado = $LASTEXITCODE

if ($Resultado -eq 0) {
    Write-Host "✅ Ejecución exitosa (Código: 0)." -ForegroundColor Green
}
else {
    Write-Host "❌ ERROR: El script de Python falló con código: $Resultado" -ForegroundColor Red
    Write-Host "Revise los logs de Snowflake o la conexión de red." -ForegroundColor Gray
}

# --- 6. Pausa para Depuración ---
Write-Host "`nRevisión completa. La ventana se cerrará en 10 segundos..." -ForegroundColor Cyan
Start-Sleep -Seconds 10
