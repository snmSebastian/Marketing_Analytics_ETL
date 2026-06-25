# 📦 Master_Products: El Guardián de la Dimensión SKU

### ¿Para qué hice esto realmente?
Este módulo es la "Fuente de Verdad" de todos los productos. Lo hice para centralizar la jerarquía (GPP), clasificaciones aduaneras (HTS) y especificaciones técnicas (PWT). Su función principal es asegurar que cada SKU tenga una sola marca, categoría y nombre estándar, evitando que los reportes de Power BI dupliquen datos por errores de dedo en Excel.

### Estado actual
**Estable pero Semi-Manual.** El script `Update_md_products.py` funciona perfectamente, pero depende totalmente de que yo (u otro analista) abra el Excel de revisión y escriba "ok" en los nuevos productos. Sin ese paso humano, el maestro no se actualiza.

### Conexiones
- **Depende de:** `Generate_sku_review.py` (el que busca productos nuevos).
- **Alimenta a:** Todo el universo analítico. Si este archivo falla, `Sales` y `Demand` se quedan sin nombres de productos.

### Piedras en el zapato
- **El Filtro del "OK":** Si un SKU nuevo no aparece, es porque olvidaste escribir "ok" en la columna `check_sku`. El script es despiadado: no "ok", no entra.
- **Nombres de Marcas:** Si aparece una marca rara, hay que actualizar el `BRAND_STANDARD_MAP` dentro del código, si no, se queda como está.
- **Excel Bloqueado:** No corras el script con el `Master Products.xlsx` abierto. El error de "Permission Denied" es un clásico aquí.

### Glosario para humanos
- **`Upsert`**: Actualizar si ya existe, insertar si es nuevo.
- **`GPP_Key`**: Una llave inventada (División-Categoría-Portafolio) para que el cruce de datos no falle por un espacio extra.
- **`huérfanos`**: Productos que aparecen en las ventas pero no tenemos idea de qué son porque no están en este maestro.

### Rescate de Datos
Para actualizar el maestro después de poner los "ok":
1. Ejecuta **`python Master_Products/Update_md_products.py`**
2. Verifica que el archivo final no haya perdido filas (el script hace un backup automático antes de sobrescribir).

---
*Última auditoría: Abril 2026.*
