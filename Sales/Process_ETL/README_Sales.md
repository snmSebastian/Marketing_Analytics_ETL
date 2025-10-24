# 💰 Módulo ETL: Procesamiento de Datos de Ventas (*Sales*)

**Ruta del Módulo:** `scripts/Sales/`

> 🚀 **Propósito:** Este módulo se dedica a transformar los datos brutos de Ventas en archivos Parquet limpios, estructurados y listos para el análisis. Reutilizamos la lógica de procesamiento (limpieza, mapeo de país y creación de claves) que ya fue establecida en el módulo `Fill_Rate`, lo que garantiza la uniformidad y eficiencia de nuestra arquitectura ETL.

***

## 1. Componentes y Funcionalidad 🧩

Este módulo se compone de dos *scripts* principales que gestionan el ciclo de vida de los datos de ventas:

| Script | Rol Principal | ¿Qué Orquesta? |
| :--- | :--- | :--- |
| **`Process_File.py`** | **Carga Inicial (Full Load)** | Coordina la lectura masiva de archivos históricos de ventas, aplica las transformaciones y realiza la primera carga completa de los datos particionados. |
| **`Update.py`** | **Actualización Incremental (Upsert)** | Gestiona las nuevas entradas de datos. Identifica y reemplaza de forma inteligente los registros existentes en el histórico para mantener la información actualizada. |

***

## 2. Flujo de Datos y Estándares (Técnica ETL) ⚙️

La mayor parte de la lógica de Extracción (E) y Transformación (T) proviene del módulo `Fill_Rate`. Esto estandariza nuestro enfoque en limpieza y creación de claves.

### Reutilización Clave:

* **Estandarización:** Uso consistente de mayúsculas y limpieza de texto para las columnas de origen.
* **Claves de Modelo:** La creación de las claves primarias y foráneas técnicas (`fk_Country`, `fk_year_month`, `fk_date_country_customer_clasification`) es idéntica a la utilizada en `Fill_Rate`.

### Flujo del Proceso de **Actualización** (`Update.py`)

El proceso de actualización se apoya en la clave compuesta (`fk_date_country_customer_clasification`) para garantizar un reemplazo de registros (tipo **Upsert**) eficiente a nivel de partición.

```mermaid
    A[Archivos Brutos Nuevos (Ventas)] --> B(Procesamiento Estándar (Reutilizado de Fill_Rate));
    B --> C{Identificación de Meses Afectados (fk_year_month)};
    C --> D[Carga Archivos Parquet Históricos de esos Meses];
    D --> E(Reemplazo de Registros por Clave Única);
    E --> F[Guarda Data Final, Sobreescribiendo los Parquet];
    F --> G[Proceso Completado];
```

### 3. Guía de Ejecución ▶️
Los scripts se ejecutan desde la raíz del proyecto. Las rutas de entrada (INPUT_RAW_HISTORIC_DIR, INPUT_RAW_UPDATE_DIR) deben estar definidas correctamente en el módulo config_paths.py.TareaComando de EjecuciónCarga Histórica (Inicial)python scripts/Sales/Process_File.pyActualización Incrementalpython scripts/Sales/Update.py