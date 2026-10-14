# 🚀 Documentación Técnica: Proyecto ETL Marketing Analytics

> ⭐ **Propósito:** Repositorio central que orquesta los flujos de datos para **Ventas, Fill Rate, Demanda y Datos Maestros**. Implementa una arquitectura modular para la extracción, transformación y carga (ETL) de datos hacia modelos analíticos.

**Stack Tecnológico:** Python 3.8+ | Pandas | PowerShell | Parquet
---

## 1. Arquitectura y Estructura del Directorio 📂

La arquitectura está modularizada, siguiendo el patrón de datos (*Sales*, *Demand*, *Fill Rate*) y las dimensiones maestras (*Master Customers*, *Master Products*).

### 1.1 Estructura del Directorio `scripts/`
scripts/
├── __init__.py         # Inicialización del módulo Python
├── config_paths.py     # ⚙️ Módulo central de gestión de rutas
├── requirements.txt    # Dependencias del proyecto
├── .gitignore          # Archivos a ignorar (logs, temporales, etc.)
│
├── Automation/         # 🤖 Orquestación y Pipelines
│   ├── init_scripts/
│   │   └── pipeline_QueryDemand.ps1  # Script de lanzamiento PowerShell
    └── Workflows
│       └── pipeline_QueryDemand.py
│
├── Fill_Rate/          # 📉 Módulo de Nivel de Servicio
│   └── Process_ETL/
│       ├── Process_Files.py
│       └── Update.py         # Lógica incremental (Partition Replace)
│
├── Sales/              # 💰 Módulo de Ventas
│   └── Process_ETL/
│       ├── Process_Files.py
│       └── Update.py         # Lógica Upsert + NPI
│
├── Demand/             # 📊 Módulo de Demanda
│   └── Process_ETL/
│       └── ...
│
├── Master_Customers/   # 👔 Dimensión Cliente
│   ├── Update.py             # Lógica de Upsert y Clasificación
│   └── README_MD_Customers.md
│
├── Master_Products/    # 📦 Dimensión Producto (SKU)
│   ├── Generate_sku_review.py
│   ├── Update_File_HTS.py
│   ├── Update_File_PWT.py
│   ├── Update_md_products.py # Consolidación Final
│   └── README_MD_Products.md
│
└── Shared_Information_for_Projects/ # 🌍 Utilidades Transversales
    └── Calendar.py


---


### 1.2 Componentes Clave de la Lógica (T) 🛠️

| Módulo/Carpeta | Propósito Principal | Rol ETL y Ciencia de Datos |
| :--- | :--- | :--- |
| **`Sales/` & `Fill_Rate/`** | Procesamiento transaccional. | **ETL Incremental:** Utilizan `Update.py` para leer fuentes crudas, calcular métricas (ej. NSV, Launch Year) y actualizar históricos en formato **Parquet**. |
| **`Master_Products/`** | Construcción de la **Dimensión SKU**. | **T Avanzada:** Lógica de negocio crítica (Asignación de GPP, derivación de atributos), y flujo **Upsert** (`Update_md_products.py`). |
| **`Master_Customers/`** | Construcción de la Dimensión Cliente. | **T de Normalización:** Limpieza de nombres, asignación de canales (`Dist Channel`) y tipos de distribución. |
| **`Automation/`** | Orquestación. | Scripts `.ps1` para ejecución en entornos Windows/Servidores, manejo de entornos virtuales y logging. |

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

2. **Ejecución Manual (Python):** Puedes probar el proceso de actualización de la Dimensión Producto (SKU):
```
python scripts/Master_Products/Update_md_products.py
```
(Asegúrate de que los archivos de entrada requeridos existan en la ruta de red configurada antes de ejecutar.)

# 🚀 Gestión del Proyecto con Poetry

Este proyecto utiliza **Poetry** para la gestión de dependencias y entornos virtuales en lugar de los archivos tradicionales `requirements.txt`. Esto garantiza que todas las librerías de Ciencia de Datos, Automatización y Machine Learning (`scikit-learn`, `skforecast`, `statsmodels`, `scrapy`, `pywin32`, etc.) sean 100% compatibles entre sí y no generen conflictos de versiones.

---

## 📓 Guía Rápida (Tu Día a Día)

> ⚠️ **Regla de oro:** Ejecuta siempre estos comandos parado dentro de tu carpeta central de trabajo:  
> `C:\Users\SSN0609\...\Analytics_Workspace\Scripts`

### 1. Iniciar a Trabajar (Activar Entorno)
Ejecuta esto cada vez que abras una terminal nueva en Visual Studio Code para enlazar tus librerías:

```powershell
# Opción Directa de Windows (Recomendada y más rápida):
.\.venv\Scripts\Activate.ps1

# Opción Oficial de Poetry:
Invoke-Expression (& C:\Users\SSN0609\AppData\Roaming\Python\Scripts\poetry env activate)
```
*💡 Sabrás que funcionó porque verás el indicador `(scripts-py3.12)` o `(.venv)` al inicio de la línea de comandos.*

### 2. Ejecutar tus Códigos
Con el entorno activado (viendo los paréntesis en la terminal), ejecuta tus módulos y scripts analíticos exactamente igual que siempre:

```powershell
python -m Master_Products.Update_File_HTS
```

### 3. Instalar Nuevas Librerías
*Olvídate de `pip install` y de modificar archivos a mano.* Puedes añadir librerías estando dentro del entorno virtual. Usa la ruta directa de Poetry para que valide la compatibilidad e inyecte el paquete automáticamente:

```powershell
# Para librerías del código (ej. openpyxl)
& C:\Users\SSN0609\AppData\Roaming\Python\Scripts\poetry add openpyxl

# Para herramientas de desarrollo que solo usas tú (ej. Jupyter)
& C:\Users\SSN0609\AppData\Roaming\Python\Scripts\poetry add ipykernel --group dev
```

### 4. Eliminar Librerías
Si dejas de usar un paquete, este comando lo remueve limpiamente de tu entorno físico y de los archivos de configuración:

```powershell
& C:\Users\SSN0609\AppData\Roaming\Python\Scripts\poetry remove nombre_libreria
```

### 5. Salir del Entorno Virtual
Cuando termines tus tareas y quieras apagar el entorno virtual en esa pestaña de la terminal:

```powershell
exit
```

---

## ⚙️ Comandos de Mantenimiento y Despliegue

### Reconstruir el proyecto desde cero
Si clonas el repositorio en otra computadora o borras accidentalmente la carpeta `.venv`, este comando lee el archivo de bloqueo e instala exactamente las mismas versiones del equipo:
```powershell
& C:\Users\SSN0609\AppData\Roaming\Python\Scripts\poetry install
```

### Sincronización Estricta (Limpiar basura)
Si cambias de rama en Git o eliminas dependencias y quieres borrar inmediatamente cualquier librería vieja o huérfana de tu carpeta `.venv`:
```powershell
& C:\Users\SSN0609\AppData\Roaming\Python\Scripts\poetry install --sync
```

---

## 📄 Archivos Clave de Configuración (No los edites a mano)

*   **`pyproject.toml`**: Contiene la lista general de librerías comerciales y la versión de Python requerida (`>=3.12,!=3.14.1`). Tiene configurado el parámetro `package-mode = false` para trabajar como repositorio de scripts.
*   **`poetry.lock`**: El archivo más importante del sistema. Bloquea las versiones exactas bit por bit (incluyendo subdependencias). **Debe subirse siempre al repositorio Git de la empresa.**
