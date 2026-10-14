import pandas as pd
import numpy as np
from Fill_Rate.Process_ETL.Process_Files import clean_sku

def consolidate_files(df_1,df_2,lst_columns_extract):
    
    df_trades=pd.concat([df_1[lst_columns_extract],df_2[lst_columns_extract]])
    df_trades=clean_sku(df_trades,'SKU')
    

    return df_trades

def unpivot_trades(df_trades,df_md_products,lst_columns_unpivot):
    df_md_products=clean_sku(df_md_products,'SKU')
    df_trades=pd.merge(
        df_trades,
        df_md_products[['SKU','SKU Base','Brand','SBU Type']],
        on='SKU',
        how='left'
    )

    df_trades_unpivot=pd.melt(df_trades,
               id_vars=['SKU','SKU Base','Brand','SBU Type','USER', 'TRADE'],
               value_vars=lst_columns_unpivot,
               var_name='atributo', value_name='Value')
    
    
    df_trades_unpivot['Value'] = np.where(
        df_trades_unpivot['Value'].str.lower().str.strip().str.replace(' ','') == 'x', 
        df_trades_unpivot['atributo'].str.title().str.strip().str.replace(' ',''), 
        ''
    )
    df_trades_unpivot['USER']=df_trades_unpivot['USER'].str.title().str.strip()
    df_trades_unpivot['TRADE']=df_trades_unpivot['TRADE'].str.title().str.strip()
    
    mapeo_valores = {
        'Baseplate': 'BasePlate',
        'Mep': 'MEP',
        'Ironworker':'Iron Worker',
        'AutoMechanic':'Auto Mechanic',
        'Metalworking':'Metal Working'
    }

    # 2. Aplicas el reemplazo directamente en la columna
    df_trades_unpivot['Value'] = df_trades_unpivot['Value'].replace(mapeo_valores)
    df_trades_unpivot['SKU Base']=df_trades_unpivot['SKU Base'].fillna(df_trades_unpivot['SKU'])

    df_trades_unpivot['USER']=df_trades_unpivot['USER'].fillna('-')
    df_trades_unpivot['Brand']=df_trades_unpivot['Brand'].fillna('-')
    df_trades_unpivot['Sku_trade'] = (
        df_trades_unpivot['SKU Base'].astype(str).str.upper().str.strip() + 
        df_trades_unpivot['Value'].astype(str)
    )
    df_trades_unpivot.drop(columns='atributo', inplace=True)
    return df_trades_unpivot

def main():
    from config_paths import MasterProductsPaths
    paths_trades_dewalt=MasterProductsPaths.INPUT_PROCESSED_TRADES_DW_FILE
    paths_md_products=MasterProductsPaths.OUTPUT_PROCESSED_MASTER_PRODUCTS_FILE
    paths_output_trades_dewalt=MasterProductsPaths.OUTPUT_PROCESSED_TRADES_DEWALT_FILE
    
    df_wilson=pd.read_excel(paths_trades_dewalt,sheet_name='Info Wilson Pagano')
    df_franciso=pd.read_excel(paths_trades_dewalt,sheet_name='Info Franciso Costa')
    df_md_products=pd.read_excel(paths_md_products)
    lst_columns_extract=['SKU','USER','TRADE','METALWORKING','DEMOLITION',
                         'BASEPLATE','REBAR','MEP','IRONWORKER','ELETRICIAN','REMODELER',
                         'PLUMBER','AUTO MECHANIC','CARPENTER']
    lst_columns_unpivot = [col for col in lst_columns_extract if col not in ['SKU', 'USER', 'TRADE']]
    

    df_consolidated=consolidate_files(df_wilson,df_franciso,lst_columns_extract)
    df_trades_unpivot=unpivot_trades(df_consolidated,df_md_products,lst_columns_unpivot)
    df_trades_unpivot.to_excel(paths_output_trades_dewalt,index=False)

    print('proceso ejecutado correctamente')

if __name__ == "__main__":
    main()