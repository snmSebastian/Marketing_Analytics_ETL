# 📊 Demand: Procesamiento de Forecast y Demanda

### ¿Para qué hice esto realmente?
Este módulo toma la data de demanda (que viene de Snowflake o Excels de planeación) y la prepara para el análisis financiero. Su truco principal es convertir el Forecast de dólares a moneda local usando tasas de cambio dinámicas (`OP Rate`), algo que Power BI no hace bien solo.

### Estado actual
**Estable con dependencias.** Funciona bien, pero es un "híbrido": toma funciones de `Fill_Rate` y de `Sales`. Si mueves esas carpetas, este módulo deja de respirar.

### Conexiones
- **Importa de `Fill_Rate`:** `read_files`, `group_parquet` y `format_columns`.
- **Importa de `Sales`:** Lógica de asignación de NPI y NSV.
- **Data Source:** Snowflake (vía el orquestador de Automation).

### Piedras en el zapato
- **Mapeo de Países:** A diferencia de Fill Rate, aquí el país se asigna por el `Demand Group`. Si el planeador de demanda inventa un grupo nuevo en Excel, el script le asignará el nombre del grupo como país por defecto hasta que lo mapees formalmente.
- **Tasas de Cambio (FX):** Si el reporte sale con valores en 0 en moneda local, es porque no hay tasa de cambio cargada para ese mes/país en la tabla de FX Rate.

### Glosario para humanos
- **`OP Rate`**: La tasa de cambio oficial planeada para el año.
- **`fk_YearMonthCountry`**: La "Super Llave". Conecta la demanda con la tasa de cambio correcta para ese mes específico y ese país.
- **`delete_parquet_files`**: Una función de limpieza para borrar basura vieja antes de procesar lo nuevo.

### Rescate de Datos
**Comando de ejecución:**
```bash
python -m Demand.Process_ETL.Process_Files
```
*Nota: Asegúrate de tener el VENV activo o el pipeline de PowerShell se encargará de esto.*
