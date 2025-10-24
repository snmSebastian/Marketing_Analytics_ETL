# 👥 Módulo ETL: Actualización del Maestro de Clientes (*Master Customers*)

**Ruta del Módulo:** `scripts/Master_Customers/`

> ⭐ **Propósito:** Este módulo es fundamental para mantener la calidad de nuestra **Dimensión Clientes**. Su objetivo principal es consolidar, estandarizar y clasificar la información de nuevos clientes proveniente de las actualizaciones de `Fill_Rate` y `Sales`, y aplicar esta información al archivo maestro (Excel) existente.

***

## 1. Funcionamiento Clave (`Update.py`) 🔄

El proceso de este módulo es la orquestación, ya que no tiene un `Process_File.py` separado. Se enfoca exclusivamente en la actualización y enriquecimiento de la data de clientes.

| Tarea Principal | Descripción |
| :--- | :--- |
| **Consolidación de Fuentes** | Combina los datos de cliente (código y nombre) de las carpetas de actualización de `Fill_Rate` y `Sales`. |
| **Asignación de Clasificación** | Aplica la lógica de negocio para asignar el **Canal de Distribución** y el **Tipo de Distribución** a cada cliente, utilizando tablas de referencia. |
| **Corrección de Nombres** | Aplica un proceso de limpieza para estandarizar y corregir errores de notación (*typos*) en los nombres de los clientes. |
| **Gestión del Maestro** | Implementa un **reemplazo inteligente (Upsert)** directamente sobre el archivo maestro Excel, para asegurar que los registros antiguos se mantengan y los nuevos/actualizados se inserten. |

***

## 2. Lógica de Negocio Avanzada 🧠

La lógica de clasificación es la parte más compleja del módulo y está centrada en las reglas del negocio:

1.  **Limpieza del Código:** Estandariza el código de cliente (eliminando prefijos y ceros iniciales).
2.  **Mapeo de Canal Compartido:** Intenta asignar el canal de distribución según una tabla de clientes compartidos.
3.  **Lógica Condicional (Asignación Final):** Si el canal no se pudo mapear en el paso anterior o si el país es **Colombia**, se aplica una regla específica utilizando un sistema condicional (`np.select`) para asignar un canal por defecto (`SHOWROOMS` o `TRADITIONAL HARDWARE STORES`).
4.  **Mapeo Final:** Une el cliente con la tabla de Clasificaciones para obtener el campo **`fk_Dist_Type`**.

### Flujo de Actualización al Archivo Maestro

El *upsert* es crucial aquí porque el destino es un archivo Excel maestro, no Parquet.

1.  Se genera la clave única **`fk_country_customer`** en la data nueva y en el maestro existente.
2.  Se **filtran** del maestro todos los registros cuya clave esté presente en la data nueva (los que serán reemplazados).
3.  Se **concatenan** los registros antiguos y los registros nuevos.
4.  El resultado final sobrescribe el archivo maestro de clientes.

***

## 3. Guía de Ejecución ▶️

Este módulo está diseñado para ser ejecutado después de que los archivos de actualización de `Fill_Rate` y `Sales` hayan sido generados.

| Tarea | Comando de Ejecución |
| :--- | :--- |
| **Actualización del Maestro** | `python scripts/Master_Customers/Update.py` |

***

## 4. Estructura de Salida (Output) 📄

El resultado del proceso es la sobrescritura del archivo maestro de clientes.

* **Salida:** Archivo Excel (`.xlsx`) en la ruta definida por `OUTPUT_FILE_PROCESSED_MASTER_CUSTOMERS_FILE`.

### Esquema Final (Columnas Clave)

El maestro final contiene las siguientes columnas limpias y clasificadas:

| Nombre de Columna | Descripción | Rol |
| :--- | :--- | :--- |
| `fk_Country` | País del cliente. | Dimensión Geográfica |
| `fk_Sold-To Customer` | Código de cliente estandarizado. | **Identificador Único** |
| `Sold-To Customer Name` | Nombre del cliente (corregido por notación). | Atributo |
| `fk_Dist_Channel` | Canal de distribución asignado por lógica de negocio. | Clasificación (Dimensión) |
| `fk_Dist_Type` | Tipo de distribución asociado al canal. | Clasificación (Dimensión) |
