# 📦 Módulo ETL: Procesamiento de la Tasa de Cumplimiento (*Fill Rate*)

**Ruta del Módulo:** `scripts/fill_rate/`

> ✨ **Objetivo**  tomar los datos brutos en Excel, limpiarlos a fondo y guardarlos de forma organizada en archivos Parquet listos para ser usados en tus análisis y modelos.

---

## 🚀 1. ¿Qué Hacemos Aquí? (Propósito y Uso)

Este módulo gestiona todo el ciclo ETL (*Extract, Transform, Load*) de los datos de *Fill Rate*. Tenemos dos scripts principales para diferentes tareas:

| Script | Función Principal | ¿Cuándo Usarlo? |
| :--- | :--- | :--- |
| **`process_file.py`** | **Carga Inicial / Completa** 🔄 | Únicamente la primera vez, para procesar **todos** los datos históricos. |
| **`update.py`** | **Actualización Incremental** ➕ | Regularmente (diario/semanal), para procesar nuevos datos y **reemplazar** solo los registros de los meses afectados. |

---

## 🛠️ 2. El Flujo de Transformación (La Magia ETL)

El corazón de este módulo es garantizar que los datos estén limpios y tengan las claves correctas para unirse a tu Data Mart.

### Lógica de Limpieza y Estandarización:

1.  **Consolidación:** Unimos todos los archivos Excel (`.xlsx`) de la carpeta de origen.
2.  **Limpieza Rápida:** Todos los textos se pasan a **MAYÚSCULAS** y se eliminan los espacios extra para evitar errores de *matching*.
3.  **Mapeo Geográfico:** Asignamos un código de país único (`fk_Country`) combinando dos campos de origen para estandarizar la geografía.
4.  **Creación de Claves (FKs):** Generamos identificadores clave para el *data warehousing*:
    * `fk_year_month`: Fundamental para la **organización de carpetas/archivos**.
    * `fk_date_country_customer_clasification`: Nuestra **Clave Única** (PK técnica), vital para saber exactamente qué registro debemos **reemplazar** en las actualizaciones.

### Diagrama del Proceso de Actualización (`update.py`)

Así nos aseguramos de que no haya duplicados ni datos viejos:

```mermaid
graph TD
    A[Archivos Excel Nuevos] --> B(Procesamiento y Creación de Clave Única);
    B --> C{Identificamos Meses Afectados};
    C --> D[Cargamos los Parquet Históricos SOLO de esos Meses];
    D --> E(Reemplazo Inteligente - Descartar Viejos / Concatenar Nuevos);
    E --> F[Guardamos el Archivo Final (Parquet) - ¡Sobrescribimos el anterior!];
    F --> G[Listo ✅];
```

### ▶️ 3. Ejecución del Módulo
Para correr el proceso, solo necesitas asegurarte de que tus rutas de entrada/salida estén bien configuradas en config_paths.py y ejecutar el script adecuado desde la raíz del proyecto:
Tarea,Comando a Ejecutar
Carga Histórica (Una vez),python scripts/fill_rate/process_file.py
Actualización (Regular),python scripts/fill_rate/update.py