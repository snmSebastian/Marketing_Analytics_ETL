from webscraper import *
import pandas as pd
#source venv_webscrapping/bin/activate

ruta = '/home/sebastian/Documentos/programas/WebScraping/WebScraping Scrapy/all_links.xlsx'
df = pd.read_excel(ruta, sheet_name='urls')
df_base_url = df[(df['Scrapping'] == 'Scrapping')]

#print("Columnas encontradas en el archivo:", df.columns.tolist())
df_resultado = scrapear_base_url_parallel(df_base_url, max_procesos=3)
print("[MAIN] Scraping terminado. Total de productos extraídos:", len(df_resultado))
df_resultado.to_excel("results.xlsx", index=False)




