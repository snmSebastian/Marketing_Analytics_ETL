# 🚀 Guía de Instalación y Configuración del Proyecto ETL

> ⭐ **Propósito Principal:** Este repositorio alberga la lógica de negocio y la orquestación de todos los **procesos de Extracción, Transformación y Carga (ETL)** del proyecto de Marketing Analytics.
>
> 🎯 **Foco de Master_Products:** El módulo clave de **Dimensión Producto (SKU)**, cuyo objetivo es identificar **nuevos SKUs**, aplicar la **lógica de negocio compleja** (asignación de GPP, Corded/Cordless, Bare, etc.) y consolidar el *Maestro de Productos* limpio y validado.

---

## 1. Arquitectura y Estructura del Directorio 📂

La arquitectura está modularizada, siguiendo el patrón de datos (*Sales*, *Demand*, *Fill Rate*) y las dimensiones maestras (*Master Customers*, *Master Products*).

### 1.1 Estructura del Directorio `scripts/`
scripts/
├── __init__.py         # Inicialización del módulo Python

├── config_paths.py     # ⚙️ Módulo central de gestión de rutas

├── requirements.txt    # Dependencias del proyecto

├── .gitignore          # Archivos a ignorar (logs, temporales, etc.)

├── Fill_Rate/

    └──Process_File.py
    └──Update.py
    └──Readme_Fill_Rate.md

├── Sales/

    └──Process_File.py
    └──Update.py
    └──Readme_Update.md

├── Demand/

    └──Process_File.py
    └──Update.py
    └──Readme_Demand.md

├── Master_Customers/   # 👔 Lógica de la Dimensión Cliente

    └──Update.py
    └──Readme_Customers.md

├── Master_Products/    # 📦 Lógica de la Dimensión Producto (SKU)

    ├── column_processing.py
    └── Generate_sku_review.py
    └── Update_File_HTS.py
    └── Update_File_HTS.py
    └── Update_md_product_File.py

├── Automation/ # 📧 Scripts de automatización y monitoreo de procesos (Ej: envío de notificaciones por email al finalizar los ETL).

    └── ...

├── Shared_Information_for_Projects / # 🌍 Contiene datos maestros, tablas de referencia o utilidades transversales (Dimensiones Comunes) que son consumidas por otros módulos.

    └── Calendar.py



├── (New folder...) # ❗ Nota Importante: Las nuevas carpetas deben ser creadas teniendo en cuenta el área funcional o la entidad de datos específica, por ejemplo: el área de demanda tiene su propia carpeta (e.g., 'Demand/').

    └──...
---


### 1.2 Componentes Clave de la Lógica (T) 🛠️

| Módulo/Carpeta | Propósito Principal | Rol ETL y Ciencia de Datos |
| :--- | :--- | :--- |
| **`Data_Sources/`** | Manejo de datos transaccionales brutos. | **E y L Inicial:** Limpieza, tipado y estandarización para generar archivos **Parquet** listos para modelos. |
| **`Master_Products/`** | Construcción de la **Dimensión SKU**. | **T Avanzada:** Lógica de negocio crítica (Asignación de GPP, derivación de atributos como *Corded/Cordless*), y flujo **Upsert** (`Update_md_products_file.py`). |
| **`Master_Customers/`** | Construcción de la Dimensión Cliente. | **T de Normalización:** Normalización de nombres (`Notation_Name_Customers`) y asignación de jerarquías. |

---

## 2. Archivo Clave: `config_paths.py` 🔑

Este módulo es fundamental para la **robustez** y **mantenibilidad** del proyecto. Implementa principios de diseño para centralizar y proteger las rutas:

| Componente | Patrón de Diseño | Función |
| :--- | :--- | :--- |
| **`BASE_PATH`** | Principio de la Ruta Única | Define la raíz del proyecto para asegurar la **portabilidad** entre diferentes entornos. |
| **`PATHS_CONFIG`** | Centralización (Diccionario) | Almacena todas las rutas de entrada (`Raw`) y salida (`Processed`) de cada módulo, organizadas por entidad. |
| **`@dataclass`** | Acceso Rápido e Inmutable | Crea objetos `...Paths` *frozen* (**inmutables**) para que cada script solo importe las rutas que necesita de forma clara y segura. |

> **💡 Beneficio para ETL:** Cualquier cambio en la estructura de carpetas de la red solo requiere modificar `BASE_PATH` y `PATHS_CONFIG`, minimizando la necesidad de tocar la lógica de los scripts ETL de Python.

---



## 3. Configuración del Entorno de Trabajo ⬇️

### 3.1 Requisitos del Sistema 📋

Asegúrese de tener instalado lo siguiente antes de proceder:

* **Python 3.8+**
* **Git**
* Acceso a la Ruta Base de Red: `Latin America - Regional Marketing - Marketing Analytics`.

### 3.2 Descarga del Código Fuente (Clonación)

1.  Abre tu terminal o Git Bash.
2.  Clona el repositorio y navega al directorio principal:

    ```bash
    git clone [URL_DEL_REPOSITORIO]
    cd [NOMBRE_DEL_DIRECTORIO] 
    ```

### 3.3 Creación y Activación del Entorno Virtual 🐍

Crea un entorno virtual con un nombre específico para el proyecto y actívalo.

1.  **Creación del Entorno:**
    ```bash
    python -m venv venv_Scripts_Marketing_Analytics
    ```
2.  **Activación del Entorno (PowerShell - Windows):**
    > **⚠️ IMPORTANTE:** Si usas PowerShell en Windows, debes ajustar temporalmente la política de ejecución.
    ```powershell
    # 1. Ajuste Temporal de la Política de Ejecución
    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
    
    # 2. Active el Entorno Virtual
    .\venv_Scripts_Marketing_Analytics\Scripts\Activate.ps1
    ```
3.  **Activación del Entorno (CMD/Bash):**
    ```bash
    # Command Prompt
    venv_Scripts_Marketing_Analytics\Scripts\activate.bat 
    
    # Bash/Linux
    source venv_Scripts_Marketing_Analytics/bin/activate
    ```

### 3.4 Instalación de Dependencias 📦

Con el entorno virtual activado, instala todas las librerías necesarias. Esto incluye bibliotecas clave para tu trabajo en Data Science como **pandas**, **numpy**, y **pyarrow** (para el formato Parquet).

```bash
pip install -r requirements.txt
```
---

## 4. Verificación y Ejecución ✅
1. Verificación de Rutas: Asegúrate de que la variable BASE_PATH en scripts/config_paths.py apunte correctamente a tu ubicación de red.

2. Ejecución de Prueba: Puedes probar el proceso de actualización de la Dimensión Producto (SKU):
```
python scripts/Master_Products/Update_md_products_file.py
```
(Asegúrate de que los archivos de entrada requeridos existan en la ruta de red configurada antes de ejecutar.)