
# 📈 Módulo ETL: Procesamiento de Datos de Demanda (*Demand*)

**Ruta del Módulo:** `scripts/Demand/`

> 🎯 **Propósito:** Este módulo es el encargado de convertir la data bruta de Demanda (Histórico y Proyección) en información estructurada. Reutilizamos la base de nuestro *pipeline* ETL (lectura y carga), pero aplicamos una **lógica de transformación propia** para crear claves y mapeos específicos para este dominio.

***

## 1. Componentes Clave y Roles ⚙️

Al igual que en otros módulos, tenemos dos *scripts* de orquestación que usan las funciones de procesamiento:

| Script | Función Principal | Detalle de la Operación |
| :--- | :--- | :--- |
| **`Process_File.py`** | **Carga Histórica Inicial** 💾 | Procesa y consolida todos los archivos brutos de Demanda para la primera carga. |
| **`Update.py`** | **Actualización Incremental** 🔄 | Gestiona la actualización periódica, implementando la lógica de **reemplazo (Upsert)** para los meses con nuevos datos. |

***

## 2. Lógica de Transformación Especializada 🧠

Aunque compartimos librerías con `Fill_Rate` y `Sales`, la Demanda requiere dos adaptaciones cruciales en el proceso de transformación (T):

### A. Mapeo de País
* **Diferencia Clave:** A diferencia de `Fill_Rate` y `Sales`, el código de país (`fk_Country`) se asigna directamente utilizando la columna **`Demand Group`** como clave de referencia.

### B. Clave Única (PK Técnica)
* **Adaptación:** La clave de unicidad que usamos para el *Upsert* es más simple, ya que los datos de Demanda no contienen (o no usan) el nivel de detalle de Cliente.
* **Nueva Clave:** Se genera **`fk_date_country_clasification`** (excluyendo el código de cliente) para identificar de forma única cada registro a nivel de fecha, país y clasificación de producto.

### Flujo del Proceso de Actualización (`Update.py`)

El proceso es robusto y se enfoca en la clave adaptada:

```mermaid
graph TD
    A[Archivos Brutos Nuevos (Demand)] --> B(Mapeo de País por 'Demand Group');
    B --> C(Creación de Clave Única 'fk_date_country_clasification');
    C --> D{Identificación de Meses a Actualizar};
    D --> E[Carga de Parquets Históricos Afectados];
    E --> F(Reemplazo de Registros usando la PK Adaptada);
    F --> G[Guardado de Data Final Parquet por Partición];
    G --> H[Proceso Finalizado ✅];
```
### 3. Guía de Ejecución ▶️
Los scripts se ejecutan como puntos de entrada del proyecto. Asegúrate que las rutas en config_paths.py estén configuradas para el módulo Demand.TareaComando a EjecutarCarga Histórica (Una vez)python scripts/Demand/Process_File.pyActualización Incrementalpython scripts/Demand/Update.py