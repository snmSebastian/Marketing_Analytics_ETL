"""
Estandarización de nombres de clientes mediante similitud Jaro-Winkler.

Este script limpia nombres, identifica coincidencias aproximadas entre valores
únicos, agrupa variantes mediante un grafo y genera una columna con nombres
canónicos para facilitar la normalización de datos.
"""

import pandas as pd

import pandas as pd
import numpy as np
import re
import networkx as nx

from rapidfuzz.distance import JaroWinkler as jaro_winkler 

# --- 1. Preprocesamiento (sin cambios) ---

def clean_name(df,name_column:str):
    """
    Limpia y estandariza la columna del argumento de entrada eliminando caracteres no deseados.
    Solo permite: Letras (A-Z), Números (0-9), y los caracteres /, \, ., -
    """
    
    # El patrón define una "lista blanca":
    # ^ dentro de [] significa "todo lo que NO sea lo siguiente"
    # A-Z0-9: Letras y números
    # /: Barra inclinada
    # \\: Barra invertida (se usan dos para escapar el carácter en Python)
    # \.: Punto (se escapa porque en regex el punto significa "cualquier carácter")
    # \-: Guion (se pone al final para evitar que defina un rango)
    allowed_pattern = r'[^a-z0-9/\\.\- ]'
    df[name_column] = df[name_column].fillna('').astype(str)
    df['Sold-To Customer Name Clean'] = (
        df[name_column]
        .astype(str)          # Convierte a texto para evitar errores con nulos o números
        .str.lower()          # Convierte todo a mayúsculas
        # El replace elimina espacios y caracteres especiales en un solo paso:
        .str.replace(allowed_pattern, '', regex=True)
        .str.strip()
    )
    return df

def jaro_winkler_clustering(unique_names, threshold=95, batch_size=2000):
    """
    Encuentra pares de nombres similares usando Jaro-Winkler.

    Compara nombres únicos por lotes y devuelve los pares de índices cuya
    similitud supera el umbral definido.

    Parameters
    ----------
    unique_names : array-like
        Lista o arreglo de nombres únicos a comparar.
    threshold : int or float, optional
        Umbral mínimo de similitud en escala 0-100. Por defecto es 95.
    batch_size : int, optional
        Tamaño del lote usado en las comparaciones. Por defecto es 2000.

    Returns
    -------
    list[tuple[int, int]]
        Lista de pares de índices `(k, j)` considerados similares.
    """
    N = len(unique_names)
    matches = []

    for i in range(0, N, batch_size):
        end = min(i + batch_size, N)
        for j in range(i, N): 
            name_j = unique_names[j]
            for k in range(i, end): 
                if k >= j: 
                    continue
                name_k = unique_names[k]
                if  name_j.startswith(name_k[:round(len(name_k)*0.4)]) and not name_j.contains('/') and not name_k.contains('/'):
                    sim = jaro_winkler.normalized_similarity(name_k, name_j) 
                    # JaroWinkler.normalized_similarity retorna un valor entre 0 y 1. 
                    if (sim * 100) >= threshold:
                        matches.append((k, j))
        
        print(f"Procesado batch {i} a {end}. Matches encontrados: {len(matches)}")
        
    return matches

def standardize_names_jarowinkler(df, col_name='Sold-To Customer Name Clean', similarity_threshold=95):
    """
    Estandariza nombres similares en una columna de un DataFrame.

    Detecta nombres parecidos con Jaro-Winkler, agrupa coincidencias en un
    grafo y asigna un nombre canónico a cada grupo.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame que contiene la columna a procesar.
    col_name : str, optional
        Nombre de la columna con los nombres de clientes.
        Por defecto es 'Sold-To Customer Name'.
    similarity_threshold : int or float, optional
        Umbral mínimo de similitud en escala 0-100.
        Debe estar entre 90 y 100. Por defecto es 95.

    Returns
    -------
    pandas.DataFrame
        DataFrame con una nueva columna llamada 'Canonical Name'.

    Raises
    ------
    ValueError
        Si `similarity_threshold` no está entre 90 y 100.
    """
    
    print("1. Obteniendo nombres únicos...")
    unique_names = df[col_name].dropna().unique()
    
    if not 90 <= similarity_threshold <= 100:
        raise ValueError("El umbral Jaro-Winkler debe estar entre 90 y 100.")

    print(f"2. Calculando similitudes Jaro-Winkler por lotes (Umbral: {similarity_threshold})...")
    
    matched_pairs = jaro_winkler_clustering(
        unique_names, 
        threshold=similarity_threshold,
        batch_size=1000 
    )
    
    print(f"3. Construyendo grafo con {len(matched_pairs)} conexiones...")
    G = nx.Graph()
    G.add_nodes_from(range(len(unique_names)))
    G.add_edges_from(matched_pairs)
    
    canonical_map = {}
    print("4. Resolviendo clusters y seleccionando nombres canónicos...")
    
    for component in nx.connected_components(G):
        if len(component) > 1:
            names_in_group = [unique_names[i] for i in component]
            representative = max(names_in_group, key=len)
            
            for idx in component:
                canonical_map[unique_names[idx]] = representative
    
    print("5. Mapeando al dataframe original...")
    
    df['Canonical Name'] = df[col_name].map(canonical_map)
    df['Canonical Name'] = df['Canonical Name'].fillna(df[col_name])
    
    return df

def main():
    """
    Ejecuta el flujo completo de estandarización de nombres de clientes.

    Lee el archivo Excel de entrada desde la ruta configurada, carga los datos
    en un DataFrame, aplica el proceso de estandarización sobre la columna
    `Sold-To Customer Name` y exporta el resultado final a un nuevo archivo Excel.

    La función concentra la ejecución de las etapas principales del proceso:
    lectura de datos, transformación de nombres y escritura del archivo de salida.
    """
    from config_paths import MasterCustomersPaths
    path_md_customers=MasterCustomersPaths.OUTPUT_FILE_PROCESSED_MASTER_CUSTOMERS_FILE
    path_output_name=r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\LAG Analytics & Data Repository - Documents\Analytics_Workspace\Data\Processed-Dataflow\Master_Customers\Master_Customers_name.xlsx'
    df=pd.read_excel(path_md_customers, engine='openpyxl')

    df=clean_name(df,'Sold-To Customer Name')
    df_result = standardize_names_jarowinkler(df, 
                                          col_name='Sold-To Customer Name Clean', 
                                          similarity_threshold=97) 
    df_result.to_excel(path_output_name, index=False)

if __name__ == "__main__":
    main()