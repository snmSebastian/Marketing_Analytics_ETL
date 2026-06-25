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
│       └──Update.py
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