import pandas as pd
import hashlib
import numpy as np
import logging
from abc import abstractmethod, ABC

pd.set_option('future.no_silent_downcasting', True)

def sub_dim_id(fact_table,dim_table,join_map,id_col,replace_col):
    # join_map: {fact_col: dim_col}
    merged=fact_table.merge(
        dim_table[list(join_map.values())+[id_col]],
        left_on=list(join_map.keys()),
        right_on=list(join_map.values()),
        how='left'
    )

    merged[replace_col]=merged[id_col]

    return merged#.drop(columns=list(join_map.values())+[id_col])

def apply_alias(fact_df, alias_df, fact_col):
    # Merge fact table with aliases
    merged = fact_df.merge(
        alias_df,
        left_on=fact_col,
        right_on='alias_name',
        how='left'
    )
    # Replace original column with canonical name if exists
    merged[fact_col] = merged['canonical_name'].fillna(merged[fact_col])
    # Drop auxiliary columns
    return merged.drop(columns=['canonical_name','alias_name'])

class ExtractionFailed(Exception):
    pass

class TableNotFound(Exception):
    pass

class HTML_Extraction(ABC):
    """All classes that will interact with beautifulsoup must inherit this."""
    @property
    @abstractmethod
    def id(self):
        """HTML class identifier"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def expected_cols(self):
        """List or set of expected column names for conducting shapechecks"""
        raise NotImplementedError()
    
    @property
    @abstractmethod
    def cat(self):
        """List or set of expected column names for conducting shapechecks"""
        raise NotImplementedError()

class FactMeta(ABC):
    @property
    @abstractmethod
    def season_calcs(self):
        """HTML class identifier"""
        raise NotImplementedError()
    
    @property
    @abstractmethod
    def identifier(self):
        """HTML class identifier"""
        raise NotImplementedError()
    
    @property
    @abstractmethod
    def value_vars(self):
        """HTML class identifier"""
        raise NotImplementedError()
    
    @property
    @abstractmethod
    def stat_lookup(self):
        """HTML class identifier"""
        raise NotImplementedError()

class DimensionMeta(type):
    def __new__(mcls, name, bases, namespace):
        if "exportdf" in namespace or "export_df" in namespace:
            raise TypeError("Do not define 'exportdf' or 'export_df' manually.")

        # Create the class
        cls = super().__new__(mcls, name, bases, namespace)

        # ----- Inject validate_df -----
        def validate_df(self):
            df = self.df

            # Auto-set primary key if not defined
            if not getattr(self, "primary_key", None):
                logging.warning('No primary key defined — defaulting to first column.')
                self.primary_key = df.columns[0]

            pk = self.primary_key

            duplicates_df = df[df.duplicated(subset=pk, keep=False)]

            if not duplicates_df.empty:
                logging.critical('Dimension table contains duplicate values in primary key.')
                logging.debug(f'Duplicate rows:\n\n{duplicates_df}')
                raise TypeError('Dimension table primary keys cannot have duplicate values.')

        cls.validate_df = validate_df

        def exportcreate_(self):
            self.validate_df()
            self.export_df=self.df

        cls.validate_df = validate_df

        return cls

class BaseClasses:
    fact=FactMeta
    dimension=DimensionMeta
    html=HTML_Extraction

def ExtractRows(soup,id,strip_text):
    james = soup.find('table', id=id)
    if james is None:
        raise TableNotFound
    table = james.find('tbody')
    rows = table.find_all('tr')
    thead = james.find('thead')
    if thead:
        header_rows = thead.find_all('tr')
        if header_rows:
            headers = [th.get_text(strip=True) for th in header_rows[-1].find_all('th')]
    else:
        headers = None
    return rows, headers

def ExtractTable(soup,id,strip_text):
    rows, headers = ExtractRows(soup,id,strip_text)
    table_data = []
    for row in rows:
        cells = row.find_all(['td', 'th'])
        row_data = [cell.get_text(strip=strip_text) for cell in cells]
        table_data.append(row_data)
    df = pd.DataFrame(table_data, columns=headers)
    return df

class DIM_Players_Mixin:
    def generate_player_id(self, name_col, birth_col):
        self.df['normalized_name'] = self.normalize_names_column(name_col)
        self.df['Name'] = self.df['Player']
        self.df['Player'] = self.generate_hash(self.df['normalized_name'], birth_col)
        self.df['Player_ID'] = self.df['Player'] + f'_{self.year}'
        self.df.drop(columns=[c for c in ['normalized_name', 'Birthdate_str'] if c in self.df.columns], inplace=True)
        cols=['Player_ID','Player','Name']+[c for c in self.df.columns if c not in ['Player_ID','Player','Name']]
        self.df=self.df[cols]
        
    @staticmethod
    def normalize_names_column(col:pd.Series)->pd.Series:
        suffixes=['jr','sr','iii','ii','iv']
        col_clean=col.str.replace('-',' ',regex=False)
        col_split=col_clean.str.split()
        col_filtered=col_split.apply(lambda parts:[p for p in parts if p.lower().rstrip('.') not in suffixes])
        return col_filtered.apply(lambda parts:parts[0]+parts[-1] if len(parts)>=2 else parts[0])

    @staticmethod
    def generate_hash(name_col,birth_col):
        combined=(name_col.str.lower()+birth_col.str.replace('/','',regex=False)).values.astype('U')
        def vectorized_sha256(arr):
            return np.array([hashlib.sha256(s.encode('utf-8')).hexdigest()[:8] for s in arr])
        return pd.Series(vectorized_sha256(combined),index=name_col.index)

class MissingCols(Exception):
    pass

class Table: # note for review- this class should never be directly inherited.
    def __init__(self,category,soup,validate=True,strip_text=True):
        logging.debug(f'\nCreating dataframe for {category.cat}')
        for k,v in category.__dict__.items():
            if not k.startswith('__'):
                setattr(self,k,v)
        self.df = ExtractTable(soup,self.id,strip_text).fillna(0).replace('',0).infer_objects(copy=False)
        if validate==True:
            self.shapecheck()

    def shapecheck(self):
        logging.debug('Conducting shapecheck.')
        actual_cols=set(self.df.columns)
        expected=set(self.expected_cols.keys())
        self.missing_cols=expected-actual_cols
        if self.missing_cols:
            logging.critical(f'Shapecheck failed. The table is missing the following columns: {self.missing_cols}.')
            raise MissingCols
        
        leftover_cols=actual_cols-expected
        
        if leftover_cols:
            logging.warning(f'Shapecheck succeeded, however there are more columns than expected. Unexpected columns: {leftover_cols}. These will be retained.')

    def typecheck(self):
        for col in self.df.columns:
            expectedtype=self.expected_cols[col]
            actualtype=self.df[col].dtypes
            if expectedtype==actualtype:
                logging.debug('Typecheck succeeded')
                continue
            else:
                logging.debug(f'{col} failed typecheck. Expected type: {expectedtype}. Actual type: {actualtype}. Attempting conversion...')
                try:
                    self.df[col]=self.df[col].astype(expectedtype)
                    logging.debug('Successfully converted to expected type.')
                except Exception as e:
                    logging.error(f'Unable to convert{col}- {e}')

    def clean_table(self):
        for col, rules in self.cleaning.items():
            for rule in rules:
                dirtychar = rule['target']
                replacement = rule['replace_with']
                self.df[col] = self.df[col].str.replace(dirtychar, replacement, regex=False)

class Fact(Table):
    def summerge(self,merged_df): #pass a pre-merged df into this. Does not have merging logic since it's very context-dependent
        calc_cols=self.convert_col_names(merged_df)
        for col in calc_cols:
            merged_df[col]=merged_df[f'{col}_x'].astype(int)+merged_df[f'{col}_y'].astype(int)
            merged_df.drop(columns=[f'{col}_x',f'{col}_y'],inplace=True)
        return merged_df 

    def convert_col_names(self,df):
        cols=df.columns
        cols=[col for col in cols if '_x' in col or '_y' in col]
        col_count=len(cols)
        if col_count%2==1:
            raise TypeError('An unmatched value column was passed into summerge.')
        if col_count==0:
            logging.warning('No value columns passed into summerge.')
        clean_cols=[]
        for col in cols:
            col=col.replace('_x','').replace('_y','')
            if col not in clean_cols:
                clean_cols.append(col)
        return clean_cols
     
    def calculate_values(self):
        for calc in self.category.calc_columns:
            dicref=self.category.calc_columns[calc]
            for col in dicref:
                nestref=dicref[col]
                if calc=='avg':
                    self.df[col]=self.df[nestref[0]]/self.df[nestref[1]]
                if calc=='pct':
                    self.df[col]=(self.df[nestref[0]]*100)/self.df[nestref[1]]
                if calc=='tot':
                    self.df[col]=self.df[nestref[0]]*self.df[nestref[1]]
                if calc=='sum':
                    self.df[col]=self.df[nestref[0]]+self.df[nestref[1]]
        logging.debug(f'After calculating, this is the table:\n\n{self.df}')
        self.df=self.df[self.category.col_order]

    def long_now(self):
        logging.debug(f'Before lengthening, this is the dataframe:\n\n{self.df}')
        self.df=self.df.melt(id_vars=['Player','Tm'],value_vars=self.value_vars,var_name='Stat',value_name='Value')

class Dim_Check(ABC):
    @property
    @abstractmethod
    def primary_key(self):
        """HTML class identifier"""
        raise NotImplementedError()

class Dimension(Table,Dim_Check):
    def validate_export(self):
        df=self.df
        dup_mask=df[self.primary_key].duplicated(keep=False)

        if not dup_mask.any():
            logging.debug('No duplicates found.')
            return

        self.dup_df=df[dup_mask].sort_values(self.primary_key)
        raise TypeError

class ExtractionFailed(Exception):
    pass

class MissingCols(Exception):
    pass

class Exporter:
    def __init__(self):
        export_df_objects=[]
        validated_dfs=[]

        for obj in export_df_objects:
            obj.validate_export()
            validated_dfs.append(obj.df)
