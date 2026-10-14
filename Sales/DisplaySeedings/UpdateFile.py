import pandas as pd

def consolidated_files_xlsx(path_Display):
    df_bra=pd.read_excel(path_Display,sheet_name='BRA',header=1)
    df_arg=pd.read_excel(path_Display,sheet_name='ARG',header=1)
    df_chl=pd.read_excel(path_Display,sheet_name='CHL',header=1)
    df_per=pd.read_excel(path_Display,sheet_name='PER',header=1)
    df_mex=pd.read_excel(path_Display,sheet_name='MEX',header=1)
    df_col=pd.read_excel(path_Display,sheet_name='COL',header=1)
    df_cca=pd.read_excel(path_Display,sheet_name='CCA',header=1)
    df_ecu=pd.read_excel(path_Display,sheet_name='ECU',header=1)

    df=pd.concat([df_bra,df_arg,df_chl,df_per,df_mex,df_col,df_cca,df_ecu])
    lst_col=['Market','Key Focus Areas','Key Performance Indicator (KPI)','Brand','Scenario','Jan',
             'Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    df = df[lst_col]

    return df


def unpivot(df):
    lst_col = ['Market', 'SBU','Key Focus Areas', 'Key Performance Indicator (KPI)', 'Brand', 'Scenario']
    
    df = pd.melt(
        df,
        id_vars=lst_col,
        var_name='Month',
        value_name='Value'
    )
    
    return df


def  main():
    from config_paths import DisplayPaths
    path_Display=r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Shared Information\Display Seedings\Display Seeding Consolidation.xlsx'
    path_output_display=DisplayPaths.OUTPUT_PROCCESED_EXCEL_FILE

    df_raw=consolidated_files_xlsx(path_Display)
    print('consolida')
    print(df_raw.head())

    df_raw['SBU'] = df_raw['Key Performance Indicator (KPI)'].str.upper().str.replace('DISPLAY SEEDING ', '', regex=False)
    print('sbu')
    df_unpivot=unpivot(df_raw)
    print('unpivot')
    df_unpivot.to_excel(path_output_display,index=False)



if __name__ == "__main__":
    
    main()