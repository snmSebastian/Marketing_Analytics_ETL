# 🤖 Automation Hub: El Orquestador

### ¿Para qué hice esto realmente?
Este es el "botón de encendido" automático de la operación. Lo creé para que el **Programador de Tareas de Windows** pueda ejecutar los procesos de Python sin que se rompan por las rutas de OneDrive (que tienen espacios y el símbolo `&`) o por problemas de codificación. Es el puente que prepara el entorno virtual (venv) y asegura que la consola hable el mismo idioma que nuestros datos.

### Estado actual
**Estable.** El script `pipeline_QueryDemand.ps1` ya incluye manejo de errores mediante códigos de salida (`$LASTEXITCODE`) y soporte para UTF-8. Es la plantilla base para cualquier nuevo proceso que necesitemos automatizar.

### Conexiones
- **Snowflake:** A través de los workflows de Python, este módulo dispara las queries de demanda.
- **Entorno Virtual (`venv_Scripts_RMA`):** Depende totalmente de que el venv esté en la raíz del proyecto.
- **Workflows:** Llama dinámicamente a los módulos dentro de `Automation.Workflows` usando la bandera `-m` de Python.

### Piedras en el zapato (Lo que siempre olvido)
- **El Ampersand Maldito (`&`):** La ruta "Stanley Black & Decker" rompe los scripts de consola normales. Por eso usamos `chcp 65001` y comillas literales. Si lo quitas, el script dejará de encontrar los archivos.
- **Política de Ejecución:** Si el script no arranca en una PC nueva, recuerda que PowerShell requiere `Set-ExecutionPolicy Bypass`.
- **Rutas Absolutas:** El Programador de Tareas suele arrancar en `System32`. Nunca confíes en rutas relativas; usa siempre la variable `$ProjectRoot`.
- **Pausa de 10 segundos:** Al final de los scripts `.ps1` hay un `Start-Sleep`. Es para que, si falla, alcances a leer el error antes de que la ventana se cierre sola.

### Glosario en Lenguaje Humano
- **`$ProjectRoot`**: Es la dirección de nuestra "casa" en el disco duro. Si movemos la carpeta de lugar, hay que cambiar esta ruta aquí primero.
- **`chcp 65001`**: Es un conjuro para que la terminal de Windows entienda tildes, emojis y caracteres especiales de las rutas modernas.
- **`$LASTEXITCODE`**: Es el mensajero que nos dice qué pasó. Si Python termina bien, nos manda un `0`. Si algo explotó, nos manda un número diferente y el script nos pintará un error rojo en la consola.
- **`python -m`**: Ejecuta Python en "modo paquete". Esto permite que los scripts de una carpeta puedan "ver" y usar las funciones de otra carpeta sin errores de importación.

---
### Estructura de la Carpeta

Esta carpeta se organiza de la siguiente manera para mantener la claridad y la funcionalidad:
- **`init_scripts/`**: Contiene los scripts de PowerShell (`.ps1`) que actúan como puntos de entrada para el Programador de Tareas de Windows. Son los encargados de configurar el entorno y lanzar los workflows de Python.
- **`Workflows/`**: Aquí residen los scripts de Python (`.py`) que orquestan los procesos ETL. Cada archivo en esta carpeta representa un pipeline modular que puede ser ejecutado de forma independiente por los `init_scripts`.
- **`images Emails/`**: Aqui residen las imagenes generadas mediante power automate (exporta una pagina a .png para ser enviada por correo electronico)

---
*Última auditoría de memoria: Abril 2026 (Basado en el análisis de `pipeline_QueryDemand.ps1`)*
