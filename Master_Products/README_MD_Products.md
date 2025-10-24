
# 📦 Módulo ETL: Maestro de Productos (*Master Products*)

**Ruta del Módulo:** `scripts/Master_Products/`

> ⭐ **Propósito:** Este módulo es el corazón de nuestra **Dimensión Producto (SKU)**. Su objetivo es identificar **nuevos SKUs**, aplicar la **lógica de negocio más compleja** (asignación de GPP, Corded/Cordless, Bare, etc.) y consolidar toda la información de clasificación para generar una fuente única, limpia y validada del Maestro de Productos.

***

## 1. Flujo del Proceso (ETL de Alta Complejidad) ⚙️

El módulo opera en dos fases principales: **Generación de Revisión** y **Consolidación Final (Upsert)**.

| Archivo | Fase | Descripción |
| :--- | :--- | :--- |
| **`Generate_sku_review.py`** | **T** (Transformación) | **ORQUESTACIÓN Y CLASIFICACIÓN**. Identifica nuevos SKUs (de `Fill_Rate`, `Sales`, `Demand`), asigna **SKU Base**, **GPP** (por Base o Portafolio) y atributos como **Corded/Cordless**, **Voltaje**, y **Bare**. Genera el archivo de trabajo (`WORKFILE_NEW_PRODUCTS_REVIEW_FILE`) para que el analista valide las asignaciones automáticas. |
| **`Update_File_HTS.py`** | **QC** (Control de Calidad) | Valida los SKUs de la unidad de negocio **HMT** (Hand Tools) contra su clasificación interna, identificando los SKUs **nuevos** o los **existentes con datos faltantes**. Sobrescribe el *Workfile HTS*. |
| **`Update_File_PWT.py`** | **QC** (Control de Calidad) | Valida los SKUs de la unidad de negocio **PWT** (Power Tools) contra sus atributos específicos (`Group 1`, `Group 2`). Sobrescribe el *Workfile PWT*. |
| **`Update_md_products_file.py`**| **L** (Carga/Consolidación) | **CARGA FINAL (UPSERT)**. Consolida los datos. Aplica los cambios verificados (*'verified'* u *'ok'*) del archivo de revisión, actualiza los atributos de clasificación HTS/PWT y aplica la **normalización de Brand** para generar el Maestro de Productos definitivo. |

***

## 2. Lógica de Negocio Crucial 🧠

La lógica de transformación de este módulo es la más detallada del proyecto.

1.  **Asignación de SKU Base:** Utiliza la función `assign_sku_base` para buscar el prefijo más largo del SKU que coincida con un SKU Base conocido, permitiendo la asignación de jerarquía y clasificación GPP de manera *heredada*.
2.  **Clasificación GPP:**
    * **Prioridad 1:** Asignación por el **SKU Base** (si existe).
    * **Prioridad 2:** Asignación por la descripción del **Portafolio GPP**.
    * **Prioridad 3:** Asignación por existencia en la base compartida de **PSD**.
3.  **Atributos Técnicos:** Se utiliza lógica basada en reglas (**`corded_or_cordless_or_gas`**, **`assing_qty_batteries`**) que infieren atributos clave (Voltaje, tipo de energía) a partir de prefijos SKU y palabras clave en la descripción.
4.  **Control de Calidad (QC):** Se identifican SKUs con potenciales inconsistencias: nuevos SKUs, SKUs antiguos con datos faltantes y SKUs Base que tienen múltiples clasificaciones de SBU/Categoría (requiriendo revisión manual).
5.  **Normalización de Brand:** Se aplica un diccionario de mapeo inverso (`BRAND_STANDARD_MAP`) para forzar la estandarización de las marcas (ej. `B+D`, `BLACK&DECKER` -> `BLACK + DECKER`) antes de asignar el `Brand Group`.

***

## 3. Guía de Ejecución ▶️

Los procesos deben ejecutarse en la secuencia establecida para asegurar que los *Workfiles* de revisión estén completos antes de la Consolidación Final.

| Tarea | Comando de Ejecución |
| :--- | :--- |
| **1. Generar Archivo de Revisión de Nuevos SKUs** | `python scripts/Master_Products/Generate_sku_review.py` |
| **2. Generar Workfile HTS** | `python scripts/Master_Products/Update_File_HTS.py` |
| **3. Generar Workfile PWT** | `python scripts/Master_Products/Update_File_PWT.py` |
| **4. Actualizar y Consolidar el Maestro Final** | `python scripts/Master_Products/Update_md_products_file.py` |

***

## 4. Estructura de Salida (Output) 📄

* **Principal:** El archivo **Maestro de Productos** definitivo, listo para su uso en los modelos de Power BI, se sobrescribe en la ruta `OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE`.
* **Archivos de Trabajo (Workfiles):** Archivos temporales utilizados por el analista para validar y corregir clasificaciones antes del paso de Consolidación.
    * `WORKFILE_NEW_PRODUCTS_REVIEW_FILE` (SKUs nuevos y con inconsistencias de GPP)
    * `WORKFILE_HTS_FILE` (SKUs HMT a revisar)
    * `WORKFILE_PWT_FILE` (SKUs PWT a revisar)