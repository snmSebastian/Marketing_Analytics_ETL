# ❄️ Snowflake Connection: El Puente a la Nube

### ¿Para qué hice esto realmente?
Este módulo es el "enchufe universal". Lo creé para no tener que configurar credenciales, motores de SQLAlchemy o parámetros de red en cada script. Si mañana Snowflake cambia la forma de autenticarse, solo lo corrijo aquí y todo el ecosistema (Demand, Sales, Master Products) sigue funcionando sin enterarse. Es la única puerta de entrada a la data corporativa.

### Estado actual
**Estable y Operativo.** Actualmente es vital para el pipeline de **Demand** (QueryDemand) y para el enriquecimiento de **Master Products**. Maneja la autenticación y la creación del motor de datos para que Pandas pueda "succionar" la información directamente.

### Conexiones
- **Consumidores:** `Automation/Workflows/pipeline_QueryDemand.py` lo usa para traer la proyección de demanda.
- **Master_Products:** Se apoya aquí para la "Inyección de GPP" (Jerarquía oficial).
- **Destino:** Se comunica directamente con los warehouses de Snowflake de Stanley Black & Decker.

### Piedras en el zapato (Lo que siempre olvido)
- **La VPN es Obligatoria:** Si el script se queda "colgado" al inicio sin dar error, es 100% seguro que olvidé conectar la VPN de la empresa. Snowflake no responde fuera de la red segura.
- **Variables de Entorno:** Las credenciales no deben estar en el código (hardcoded). Si falla el login, revisa si las variables de entorno o el archivo de configuración local tienen el usuario/password correcto.
- **El Ampersand en las Rutas:** Al igual que en Automation, si este módulo intenta leer un archivo de configuración en una ruta con `&`, podría fallar si no se maneja con rutas literales.
- **Consumo de Créditos:** Recuerda que cada query "despierta" el warehouse de Snowflake. No lo uses para pruebas triviales que podrías hacer con un CSV local.

### Glosario en Lenguaje Humano (Basado en inferencias)
*Nota: Pendiente de auditoría con el código real.*
- **`QueryDemand`**: La instrucción específica (SQL) que le pide a Snowflake la data de proyección de demanda.
- **`Engine` / `Connection`**: El túnel de comunicación. El engine es la máquina y la conexión es el cable conectado.
- **`Snowflake-Connector`**: La librería que hace el trabajo sucio de traducir el lenguaje de Python al de la base de datos.
- **`Warehouse`**: Es el servidor en la nube que estamos alquilando para procesar los datos. Si no hay conexión, el warehouse ni siquiera se entera de que lo llamamos.

### Estructura sugerida
- **`Conection.py`**: El script que gestiona el login y el motor de conexión.
- **`Queries/`**: Carpeta (opcional) donde guardamos los archivos `.sql` para no ensuciar el código de Python con texto gigante.

---
*Última auditoría de memoria: Abril 2026 (Borrador basado en el flujo de Automation)*
