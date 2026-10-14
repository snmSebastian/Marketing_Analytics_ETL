'''
Concepto:
    * yield:  para entregar a Scrapy una nueva tarea (como visitar una página), permitiendo que el spider siga trabajando 
              y maneje muchas páginas al mismo tiempo, en vez de esperar a que termine una para empezar otra como pasaria con return.

    * callback: es una función que se llama cuando la solicitud se completa, permitiendo procesar la respuesta de esa solicitud.
                evita que el spider se detenga y espere a que se complete una solicitud antes de continuar con la siguiente.

    * self:permite llamar una función dentro de la misma clase

    *follow: es un método de Scrapy que se utiliza para seguir enlaces a otras páginas web.
            Sigue este enlace relativo o absoluto y, cuando descargues la página, llama a esta función”.
            Resuelve automáticamente URLs relativas (como /producto/123), usando la página actual como base.
    *meta: es un diccionario que se utiliza para pasar información adicional entre las solicitudes y las respuestas en Scrapy.
            se usara para rastrear el número de página actual y el número de errores de intentos por cada url base.
    
            '''
#==============
# Paquetes
#==============

import scrapy
from scrapy.crawler import CrawlerProcess
import logging
import pprint
import random
import pandas as pd
from multiprocessing import Process, Manager
from urllib.parse import urljoin

from scrapy.http import HtmlResponse

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import subprocess


class Description_Sku_Spider(scrapy.Spider):
    user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13.4; rv:126.0) Gecko/20100101 Firefox/126.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 OPR/109.0.0.0',
    'Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1'
    ]
 
    name = "sku_spider"
    custom_settings = {
        #'USER_AGENT': random.choice(user_agents),
        'DOWNLOAD_TIMEOUT': 20,
        "LOG_LEVEL": "CRITICAL",
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    }

    def __init__(self,country, name_page, information, secuencia, url_base,
                 xpath_link_sku, xpath_sku, xpath_brand,
                 xpath_name_product, xpath_description_product, xpath_image_product,
                 xpath_category_product, xpath_esp_tecnic_product_key, xpath_esp_tecnic_product_value,
                 xpath_price_product,
                resultados=None,**kwargs):
        ''' Inicializa el spider con los parámetros necesarios para el scraping,
        se colocan dentro de __init__ para que puedan ser pasado como argumentos a las otras funciones sin necesidad de
        definirlos nuevamente en cada una de ellas. 
        Se utiliza super().__init__(**kwargs) para inicializar la clase base de Scrapy
        '''
        super().__init__(**kwargs)
        self.country = country
        self.name_page = name_page
        self.information = information
        try:
            self.secuencia = int(secuencia)
        except Exception:
            self.secuencia = 1
        self.url_base = url_base
        self.xpath_link_sku = xpath_link_sku
        self.xpath_sku = xpath_sku
        self.xpath_brand = xpath_brand
        self.xpath_name_product = xpath_name_product
        self.xpath_description_product = xpath_description_product
        self.xpath_image_product = xpath_image_product
        self.xpath_category_product = xpath_category_product
        self.xpath_esp_tecnic_product_key = xpath_esp_tecnic_product_key
        self.xpath_esp_tecnic_product_value = xpath_esp_tecnic_product_value
        self.xpath_price_product = xpath_price_product
        self.resultados = resultados if resultados is not None else []
        self.intentos_url = 0  # Contador de intentos para la url_base

    def start_requests(self):
        ''' Método que se ejecuta al iniciar el spider, envía la primera solicitud a la url_base.
        Si la url_base contiene 'num_pag', se reemplaza por el número de página actual'''
        headers = {'User-Agent': random.choice(self.user_agents)}
        num_page =1 if 'num_pag' in self.url_base else None
        url = self.url_base.replace('num_pag', str(num_page)) if num_page is not None else self.url_base
        print(f"[SPIDER] Iniciando requests para url: {url}")
        
        try:
            yield scrapy.Request(
                url=url,
                callback=self.analyze_homepage,
                errback=self.handle_error,
                headers=headers,
                meta={'num_page': num_page, 'errores_intentos': 0}
            )
        except Exception as e:
            self.logger.error(f"Error al iniciar la solicitud para {url}: {e}")
            raise scrapy.exceptions.CloseSpider()

    def handle_error(self, failure):
        ''' Método que maneja los errores de las solicitudes, se ejecuta si una solicitud falla.
        Intenta reintentar la solicitud con un número de página incrementado o la misma url
        hasta un máximo de 3 intentos por url_base.
        Si se supera el número de intentos, se cierra el spider.'''
        meta = failure.request.meta
        errores_intentos = meta.get('errores_intentos', 0) + 1
        headers = {'User-Agent': random.choice(self.user_agents)}
        if errores_intentos < 3:
        # Intenta con la siguiente página
            next_num_page = meta['num_page'] + 1 if 'num_pag' in self.url_base else None
            next_url = self.url_base.replace('num_pag', str(next_num_page))
            self.logger.error(f"Error en la página {meta['num_page']}. Probando con la página {next_num_page} (Intento {errores_intentos})")
            yield scrapy.Request(
                url=next_url,
                callback=self.analyze_homepage,
                errback=self.handle_error,
                headers=headers,
                meta={'num_page': next_num_page, 'errores_intentos': errores_intentos}
            )
        else:
            self.logger.error(f"Demasiados intentos fallidos para la url {failure.request.url}. Deteniendo el spider.")
            raise scrapy.exceptions.CloseSpider()
    
    def analyze_homepage(self, response):
        ''' Método que analiza la página principal de la url_base.
        Extrae los enlaces de los productos utilizando el xpath proporcionado.
        Si no se encuentran productos, detiene la paginación.
        Si hay paginación, solicita la siguiente página y continúa el proceso.
        '''
        print(f"[SPIDER] Analizando página {response.meta.get('num_page', 'N/A')} de {self.url_base}")
        product_links = response.xpath(self.xpath_link_sku).getall()
        #print(f'[links encontrados]: {product_links}')
        if not product_links:
            print(f"No se encontraron productos en la página {response.meta.get('num_page', 'N/A')} de {self.url_base}.\n{'-'*80}")
            return  # No hay más productos, detén la paginación

        for url in product_links:
            #print(f"Enviando solicitud para el producto: {url}\n{'-'*80}")
            headers = {'User-Agent': random.choice(self.user_agents)}
            yield response.follow(url=url,
                                callback=self.analyze_product_detail,
                                headers=headers)

        # Si hay paginación, solicita la siguiente página
        if 'num_pag' in self.url_base and response.meta['num_page'] is not None:
            num_page = response.meta['num_page'] + self.secuencia
            next_url = self.url_base.replace('num_pag', str(num_page))
            print(f'[SPIDER] Solicitando siguiente página: {next_url}')
            headers = {'User-Agent': random.choice(self.user_agents)}
            if num_page<350:
                yield scrapy.Request(
                url=next_url,
                callback=self.analyze_homepage,
                errback=self.handle_error,
                headers=headers,
                meta={'num_page': num_page}
            )
        #print(f'[RESPONSE]: {response.text[:10000]}')

    def analyze_product_detail(self, response):
        ''' Método que analiza los detalles del producto.
        Extrae los datos del producto utilizando los xpaths proporcionados.
        Guarda los datos en un diccionario y lo agrega a la lista de resultados.
        '''
        print(f"[SPIDER] Extrayendo detalles de producto: {response.url}")
        # Extrae los datos del producto y los guarda en self.results
        def safe_xpath(xpath):
            try:
                value = response.xpath(xpath).get(default='N/A').strip()
                return value if value else "N/A"
            except Exception:
                return "N/A"
        def get_full_image_url(xpath):
            img_src = safe_xpath(xpath)
            if img_src.startswith("http"):
                return img_src
            elif img_src != "N/A":
                return urljoin(self.url_base, img_src)
            else:
                return "N/A"
            
        def get_full_espc_tecnic(xpath_key, xpath_value):
            try:
                keys = response.xpath(xpath_key).getall()
                values = response.xpath(xpath_value).getall()
                # Limpia y une los pares clave: valor
                pares = []
                for k, v in zip(keys, values):
                    k = k.strip()
                    v = v.strip()
                    if k and v:
                        pares.append(f"{k}: {v}")
                return " | ".join(pares) if pares else "N/A"
            except Exception:
                return "N/A"
        def get_full_info(xpath):
            try:
                keys = response.xpath(xpath).getall()
                # Limpia y une los pares clave: valor
                pares = []
                for k in keys:
                    k = k.strip()
                    if k and k != "N/A":
                        pares.append(f"{k}:")
                return " | ".join(pares) if pares else "N/A"
            except Exception:
                return "N/A"
        def get_full_price(xpath):
            try:
                keys = response.xpath(xpath).getall()
                # Limpia y une los pares clave: valor
                pares = []
                for k in keys:
                    k = k.strip()
                    if k and k != "N/A":
                        pares.append(f"{k}")
                return "".join(pares) if pares else "N/A"
            except Exception:
                return "N/A"    
        
        item = {
            'country': self.country,
            'url_base': self.url_base,
            'name_page': self.name_page,
            'information': self.information,
            'sku': get_full_info(self.xpath_sku),
            'name_product': safe_xpath(self.xpath_name_product),
            'description_product': get_full_info(self.xpath_description_product),
            'category_product': get_full_info(self.xpath_category_product),
            'esp_tecnic_product': get_full_espc_tecnic(self.xpath_esp_tecnic_product_key, self.xpath_esp_tecnic_product_value),
            #'esp_tecnic_product': get_full_info(self.xpath_esp_tecnic_product_key),
            
            'price_product': get_full_price(self.xpath_price_product),
            'brand': safe_xpath(self.xpath_brand),
            'link': response.url,
            'image_product': get_full_image_url(self.xpath_image_product)            
        }
        #para cada url_base se genera un diccionario con los datos de detalle de cada uno de los productos que se encuentran en cada una de 
        # las paginas de la url_base
        #print(f"Datos del producto extraídos: {item}\n{'-'*80}")
        self.resultados.append(item)
        #print(self.resultados)

    def closed(self, reason):
        ''' Método que se ejecuta al cerrar el spider.
        Guarda los resultados en el objeto crawler para poder acceder desde fuera
        '''
        # Guarda los resultados en el objeto crawler para poder acceder desde fuera
        print(f"[SPIDER] Spider cerrado para url_base: {self.url_base} - Motivo: {reason}")
        #self.crawler.stats.set_value('results', self.results)

def scrapear_productos(**kwargs):
    ''' Función que inicia el spider para scrapear los productos de una url_base.'''   
    resultados = []
    kwargs['resultados'] = resultados  # Inicializa la lista de resultados en kwargs
    process = CrawlerProcess(settings={
    "LOG_LEVEL": "CRITICAL",
    "CONCURRENT_REQUESTS": 32,  # Más solicitudes en paralelo
    "CONCURRENT_REQUESTS_PER_DOMAIN": 16,
    "CONCURRENT_REQUESTS_PER_IP": 16,
    "DOWNLOAD_DELAY": 0.1,      # Menor delay entre solicitudes
    })
    process.crawl(Description_Sku_Spider, **kwargs)
    process.start()
        
    if not resultados:
        return pd.DataFrame()
    return pd.DataFrame(resultados)

def run_spider(fila, resultados_compartidos):
    ''' Función que ejecuta el spider para una fila específica del DataFrame.
    tomando los parámetros de la fila y pasándolos al spider.'''
    print(f"\n[PROCESO] Iniciando spider para url_base: {fila['url base']}")
    df_result = scrapear_productos(
        url_base=fila['url base'],
        xpath_link_sku=fila['Ruta Xpath link'],
        xpath_sku=fila['Ruta Xpath sku'],
        xpath_brand=fila['Ruta Xpath brand'],
        xpath_name_product=fila['Ruta Xpath nombre producto'],
        xpath_description_product=fila['Ruta Xpath descripcion'],
        xpath_image_product=fila['Ruta Xpath imagen'],
        xpath_category_product=fila['Ruta Xpath categoria'],
        xpath_esp_tecnic_product_key=fila['Ruta Xpath esp tecnica key'],
        xpath_esp_tecnic_product_value=fila['Ruta Xpath esp tecnica value'],
        xpath_price_product=fila['Ruta Xpath precio'],
        country=fila['Country'],
        name_page=fila['Name'],
        information=fila['Information'],
        secuencia=fila['Secuencia de paginacion']
    )
    print(f"[PROCESO] Finalizó spider para url_base: {fila['url base']}. Productos extraídos: {len(df_result)}")
    # Convierte el DataFrame a dict y lo agrega a la lista compartida
    resultados_compartidos += df_result.to_dict(orient='records')

def scrapear_base_url_parallel(df_base_url, max_procesos=3):
    '''funcion que permite ejecutar hasta 3 spiders en paralelo para scrapear los productos de varias url_base.
    Utiliza multiprocessing para crear procesos independientes para cada url_base.
    Cada proceso ejecuta el spider y guarda los resultados en una lista compartida.'''
    manager = Manager()
    resultados_compartidos = manager.list()
    procesos = []
    for idx, fila in df_base_url.iterrows():
        print(f"\n[MAIN] Preparando proceso para url_base: {fila['url base']}")
        p = Process(target=run_spider, args=(fila, resultados_compartidos))
        procesos.append(p)
        p.start()
        # Limita el número de procesos simultáneos
        if len(procesos) >= max_procesos:
            print(f"[MAIN] Esperando a que terminen {len(procesos)} procesos activos...")
            for p in procesos:
                p.join()
            procesos = []
    # Espera a que terminen los procesos restantes
    for p in procesos:
        p.join()
    print(f"[MAIN] Todos los procesos han finalizado.")    
    # Une todos los resultados en un DataFrame
    return pd.DataFrame(list(resultados_compartidos))
