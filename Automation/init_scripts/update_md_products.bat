@echo off
chcp 65001 > nul

echo Iniciando Proceso_Update_md_products.py
call "C:\Users\SSN0609\AppData\Local\Programs\Python\Python313\Lib\venv\scripts\nt\activate.bat"
cd "C:\Users\SSN0609\Stanley Black & Decker\Latin America - Regional Marketing - Marketing Analytics\Scripts\Automation\Workflows"
python  W_Update_Md_Customers.py

IF %errorlevel% EQU 0 (
  echo "Ejecución exitosa"
) else (
  echo "Error en la ejecución"
)


echo Fin del script
rem timeout /t 1500