
# 🚀 Módulo de Orquestación de Flujos de Trabajo (`Workflows`)

**Ruta:** `scripts/Automation/Workflows/`

> ⭐ **Propósito:** Este directorio es el cerebro de la automatización. Contiene los scripts "orquestadores" (`pipeline_*.py`) que actúan como puntos de entrada para ejecutar secuencias de procesos ETL, monitorear su estado y notificar los resultados automáticamente por correo electrónico.

---

## 1. Arquitectura y Componentes Clave 🏗️

La carpeta `Workflows` se organiza en torno a dos tipos de scripts principales:

| Script / Patrón | Propósito Principal |
| :--- | :--- |
| **`pipeline_*.py`** | **Orquestadores de Flujo de Trabajo.** Cada archivo es un punto de entrada para una secuencia específica de tareas (ej: `pipeline_hts.py` para actualizar HTS). Define *qué* módulos ejecutar y *cómo* notificar el resultado. |
| **`Emails.py`** | **Módulo de Utilidades Central.** Proporciona las herramientas fundamentales para la orquestación: ejecutar scripts externos de forma segura (`execute_file_py`) y enviar reportes de estado detallados por correo (`send_etl_report`). |

---

## 2. Flujo de Ejecución de un Orquestador ⚙️

Cada script `pipeline_*.py` sigue un patrón de ejecución estandarizado para garantizar robustez y un monitoreo efectivo.

```mermaid
graph TD
    A[▶️ Iniciar Pipeline] --> B{1. Configuración del Pipeline};
    B --> C[Definir Módulos a Ejecutar<br><em>(MODULOS_ETL)</em>];
    C --> D{2. Ejecución Secuencial};
    D --> E[Bucle: Por cada módulo...];
    E --> F["Ejecutar script con<br><code>execute_file_py</code>"];
    F --> G[Capturar Código de Salida y Logs];
    G --> H{Almacenar Resultado<br><em>(Éxito/Fallo)</em>};
    H --> E;
    E --> I{3. Generación de Reporte};
    I --> J[Verificar si hubo fallos];
    J -- No Hubo Fallos --> K[Seleccionar Plantilla de Éxito ✅];
    J -- Sí Hubo Fallos --> L[Seleccionar Plantilla de Fallo 🚨<br><em>(Inyectar detalle de errores)</em>];
    K & L --> M{4. Envío de Notificación};
    M --> N["Enviar email con<br><code>send_etl_report</code>"];
    N --> O[⏹️ Fin del Proceso];
```

### Características Clave:
- **Aislamiento de Procesos:** Gracias al uso de `subprocess`, un error en un módulo ETL no detiene al orquestador. Simplemente se registra el fallo y la ejecución continúa con el siguiente módulo.
- **Notificaciones HTML:** Los correos de estado utilizan plantillas HTML para una visualización clara y profesional, incluyendo enlaces directos a los archivos generados.
- **Configuración Centralizada:** Cada pipeline define claramente qué scripts ejecuta y personaliza los mensajes de éxito o error, facilitando el mantenimiento.
- **Manejo de Errores de Outlook:** Incluye lógica para suprimir errores COM específicos que ocurren después de un envío de correo exitoso, evitando falsas alarmas.

---

## 3. ¿Cómo Crear un Nuevo Flujo de Trabajo? 📝

Crear un nuevo pipeline es un proceso sencillo y rápido:

1.  **Copiar un Orquestador Existente:** Duplica uno de los archivos `pipeline_*.py` y renómbralo según su función (ej: `pipeline_new_process.py`).
2.  **Definir los Módulos:** En el nuevo archivo, modifica el diccionario `MODULOS_ETL` para incluir los nombres de los módulos Python que deseas ejecutar.
    ```python
    MODULOS_ETL = {
        "Nombre Amigable del Proceso": "nombre_del_paquete.nombre_del_script"
    }
    ```
3.  **Personalizar las Notificaciones:** Ajusta las listas `SUBJECTS_ETL` y `BODY_TEMPLATES` con los asuntos y cuerpos de correo electrónico específicos para este nuevo flujo de trabajo.
4.  **Configurar Destinatarios:** Modifica la lista `lst_email` con las direcciones de correo que deben recibir la notificación.

---

## 4. Ejecución y Automatización ▶️

Estos flujos de trabajo están diseñados para ser ejecutados de forma desatendida. La ejecución se inicia a través de los archivos `.bat` ubicados en la carpeta `Automation/init_scripts/`.

Estos archivos `.bat` son invocados por el **Programador de Tareas de Windows** (`Task Scheduler`) según la frecuencia definida (diaria, semanal, etc.), logrando una automatización completa del ciclo ETL.

**Ejemplo de ejecución manual (para pruebas):**
```bash
# Navegar a la raíz del proyecto y activar el entorno virtual

# Ejecutar un pipeline específico
python -m Automation.Workflows.pipeline_hts
```
