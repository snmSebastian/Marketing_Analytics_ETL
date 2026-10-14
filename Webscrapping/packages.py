'''
Se genera un archivo con todas los paquetes-librerias y modulos que seran usados
'''

# ---------------------------
# --- Paquetes necesarios ---
# ---------------------------
import pandas as pd         # Manipulación y análisis de datos en estructuras tipo DataFrame
import requests             # Realizar solicitudes HTTP para obtener contenido web
from bs4 import BeautifulSoup  # Parsear y extraer información de documentos HTML y XML
import time                 # Controlar pausas y medir tiempos de ejecución
import certifi              # Proporciona certificados raíz actualizados para conexiones seguras
import urllib3              # Manejo avanzado de conexiones HTTP, incluyendo soporte para HTTPS
from concurrent.futures import ThreadPoolExecutor, as_completed  # Ejecución concurrente de tareas (paralelismo)
from tqdm import tqdm       # Mostrar barras de progreso en bucles
from urllib.parse import urljoin  # Construir URLs absolutas a partir de relativas
import random               # Generar números aleatorios, útil para rotar user-agents
from typing import List     # Tipado estático para listas y otras estructuras
import re                   # Manejo y búsqueda de patrones con expresiones regulares