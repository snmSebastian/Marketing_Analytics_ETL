from Webscrapping.Selenium.webscraper import *
import pandas as pd
#source venv_webscrapping/bin/activate


def main():
    ruta = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace\Scripts\Webscrapping\all_links.xlsx'
    salida=r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace\Scripts\Webscrapping\results.xlsx'

    df = pd.read_excel(ruta, sheet_name='urls')
    df_base_url = df[(df['Scrapping'] == 'Scrapping')]

    #print("Columnas encontradas en el archivo:", df.columns.tolist())
    df_resultado = scrapear_base_url_parallel(df_base_url, max_procesos=3)
    print("[MAIN] Scraping terminado. Total de productos extraídos:", len(df_resultado))
    df_resultado.to_excel(salida, index=False)


if __name__ == "__main__":
    main()

