from abc import ABC, abstractmethod
import pandas as pd
import logging
from pathlib import Path

class export_method(ABC):
    @abstractmethod
    def build_save_loc(self,path):
        raise NotImplementedError()

    @abstractmethod
    def export(self,df):
        raise NotImplementedError()

class Export_Manager:
    def __init__(self,path,save_method='csv',safe_save=True):
        save_method=save_method.lower()   

        if save_method=='excel' or save_method=='xlsx':
            self.exporter=export_as_excel(path,safe_save)
        if save_method=='csv':
            self.exporter=export_as_csv(path,safe_save)

    def export(self,dfs_to_export):
        export_dic=self.build_export_dic(dfs_to_export)
        for name,df in export_dic.items():
            self.exporter.export(name,df)

    def build_export_dic(self,dfs):
        df_names=set()
        if isinstance(dfs,dict):
            for name,df in dfs.items():
                if name in df_names:
                    raise ValueError(f'Duplicate dataframe name detected: {name}. Each dataframe must have a unique name for export.')
                df_names.add(name)
                if not isinstance(df,pd.DataFrame):
                    raise ValueError('Only pandas DataFrames can be exported.')
            return dfs
        elif isinstance(dfs,list):
            export_dic={}
            for i,df in enumerate(dfs):
                if not isinstance(df,pd.DataFrame):
                    raise ValueError('Only pandas DataFrames can be exported.')
                name=f'DataFrame_{i+1}'
                while name in df_names:
                    i+=1
                    name=f'DataFrame_{i+1}'
                df_names.add(name)
                export_dic[name]=df
            return export_dic
        elif isinstance(dfs,pd.DataFrame):
            return {'DataFrame_1':dfs}
        else:
            raise ValueError('Input must be a pandas DataFrame, a list of DataFrames, or a dictionary of DataFrames.')

class export_as_excel:
    def __init__(self,path,safe_save):
        self.build_save_loc(path,safe_save)
        self.writer=pd.ExcelWriter(self.path,mode='w')
    
    def build_save_loc(self, path, safe_save=True):
        if not path.endswith(('.xlsx', '.xls')):
            if safe_save:
                raise ValueError(
                    "When saving an Excel file, you must specify a specific file and append .xlsx or .xls to the file name."
                    "To auto-append, set safe_save to False."
                )
            logging.debug("Appending .xlsx to file path.")
            path += ".xlsx"

        self.path = Path(path)
        self.parent.mkdir(parents=True, exist_ok=True) 

    def export(self, name, df):
        df.to_excel(self.writer,sheet_name=name,index=False)

class export_as_csv:
    def __init__(self):
        pass

    def export(self, name, df):
        df.to_csv(f'{name}.csv',index=False)
    
    def validate_export(self,df):
        for col in df.columns:
            df[col]=df[col].str.replace(',', '/', regex=False)
        return df
    
    def build_save_loc(self, path, safe_save=True):
        self.path = Path(path)

        if not self.path.exists():
            logging.debug(f"Creating directory: {self.path}")
            self.path.mkdir(parents=True, exist_ok=True)