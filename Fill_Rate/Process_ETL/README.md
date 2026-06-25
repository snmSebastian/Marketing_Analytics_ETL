# 📉 Fill Rate: La Caja de Herramientas Core

### ¿Para qué hice esto realmente?
Este no es solo un script para Fill Rate. Es el **corazón técnico** de todo el repositorio. Aquí diseñé las funciones de "limpieza universal" (`format_columns`) y el motor de guardado en Parquet (`group_parquet`). Lo hice así para no tener que escribir la misma lógica de guardado en Sales o Demand. Si cambias algo aquí, afectas a todo el sistema.

### Estado actual
**Estable y Crítico.** Es código de producción. Si estas funciones fallan, se detienen los pipelines de Ventas y Demanda porque dependen de este módulo.

### Conexiones
- **Exporta a:** `Demand\Process_ETL` y `Sales\Process_ETL`.
- **Dependencias:** Necesita las librerías `pyarrow` y `pandas` para que el particionado funcione.

### Piedras en el zapato
- **El Error del Archivo Abierto:** `read_files` suele fallar con un `PermissionError` si algún analista dejó el Excel de Fill Rate abierto en la red. Agregué un `try-except` para que te avise en consola, pero el script se detendrá.
- **Snappy Compression:** Uso compresión 'snappy' en los Parquet. Es rápida, pero si intentas leer los archivos con herramientas muy viejas de Python, podrías necesitar instalar `python-snappy`.

### Glosario para humanos
- **`group_parquet`**: Toma la base de datos gigante y la pica en trozos por "Año-Mes". Así Power BI no tiene que leer 5 años de datos si solo quieres ver el mes pasado.
- **`format_columns`**: Mi "estandarizador". Convierte todo a minúsculas, quita espacios y asegura que los números no tengan texto para que no exploten los cálculos.
- **`errors='coerce'`**: Una instrucción que le dice a Python: "si encuentras un texto donde debería haber un número, no llores, ponle un 0 y sigue adelante".

### Rescate de Datos
- **Para ejecutar la limpieza manual:**
  ```python
  from Fill_Rate.Process_ETL.Process_Files import read_files, group_parquet
  df = read_files("ruta/al/raw")
  group_parquet(df, "ruta/salida", name="fill_rate")
  ```