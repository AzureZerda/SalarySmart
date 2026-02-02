import sys
from pathlib import Path
import pandas as pd

# Add the parent directory (main folder) to sys.path
sys.path.append(str(Path(__file__).parent.parent))
teams=None
import scraping
from bs4 import BeautifulSoup
import extractor
import numpy as np
import json
from abc import ABCMeta
import copy

#scraper=scraping.Scrape_HTML()

with open("C:\\Users\\19495\\OneDrive\\Documents\\Python\\SalarySmart\\MLB\\teams.json") as f:
    teams = json.load(f)
    teams_df = pd.DataFrame.from_dict(teams, orient='index').reset_index()

def run_pipeline(year):
    soup_kitchen=Soup_Kitchen()
    htmls=HTML_Layer(year,soup_kitchen)
    obj=Season(soup_kitchen)

class Soup_Kitchen:
    def __init__(self):
        self.pots={
            'team_schedules':[],
            'team_rosters':[],
            'games':[]
        }

    def cook(self,html):
        soup=BeautifulSoup(html,'html.parser')
        return soup

class HTML_Layer:
    def __init__(self,year,soup_kitchen):
        file_name = "C:\\Users\\19495\\OneDrive\\Documents\\Python\\SalarySmart\\MLB\\ARI_2025.html"
        with open(file_name, "r", encoding="utf-8") as f:
            html = f.read()
        self.game_links=[]
        self.team_games=[html]
        for html in self.team_games:
            soup=soup_kitchen.cook(html)
            soup_kitchen.pots['team_schedules'].append(soup)
            self.extract_game_links(soup)
        
        for game in self.game_links:
            continue
        file_name = "C:\\Users\\19495\\OneDrive\\Documents\\Python\\SalarySmart\\MLB\\ARI_Game.html"
        with open(file_name, "r", encoding="utf-8") as f:
            html = f.read()
        soup=soup_kitchen.cook(html)
        soup_kitchen.pots['games'].append(soup)

    def extract_game_links(self,soup):
        table=extractor.Dimension(Team_Schedule,soup)
        body=table.table.find('tbody')
        rows=body.find_all('tr')
        links=[]
        for row in rows:
            link_box=row.find('td',{'data-stat':'boxscore'})
            if link_box is None:
                continue
            link_sec=link_box.find('a')
            link=link_sec['href']
            game_link=f'https://www.baseball-reference.com{link}'
            links.append(game_link)
        self.game_links.extend(links)

class Season:
    def __init__(self,soup_kitchen):
        self.game_links=set()
        games=Dim_Games(soup_kitchen.pots['team_schedules'])
        stats=FACT_Stats(soup_kitchen.pots['games'])

class FACT_Stats:
    def __init__(self,soups):
        for soup in soups:
            game_stats=Game_Stats(soup)

class Game_Dets:
    def __init__(self,soup):
        self.teams={}
        scorebox=soup.find('div',class_='scorebox')
        team_divs=scorebox.find_all('div',recursive=False)
        team_divs=team_divs[:2]
        for div in team_divs:
            team_name=div.find('strong').text.strip()
            self.teams[team_name]={}
            box_tag=team_name.replace(' ','')
            self.teams[team_name]['abbr']=teams[team_name]['Abbrev']
            self.teams[team_name]['box_tag']=box_tag

class Game_Stats(extractor.Fact):
    def __init__(self,soup):
        dets=Game_Dets(soup)
        for stat in Stat_Cat.registry:
            base_id=stat.id
            dfs=[]
            for team in dets.teams.keys():
                box_tag=dets.teams[team]['box_tag']
                stat.id=f"{box_tag}{base_id}"
                super().__init__(stat,soup)
                if hasattr(stat, 'filter_col'):
                    self.df=self.df[self.df[stat.filter_col]!=0]
                if hasattr(stat, 'cleaning'):
                    self.clean_table()
                self.df
                dfs.append(self.df)
            self.full_df=pd.concat(dfs,ignore_index=True)
            self.full_df.fillna(0, inplace=True)
            if stat.cat=='batting':
                self.parse_details(self.full_df)
            try:
                self.full_df=self.full_df.drop(columns=['Details'])
            except KeyError:
                pass
            stat.cols=stat.cols|stat.extra_cols if hasattr(stat,'extra_cols') else stat.cols
            for col in self.full_df:
                try:
                    self.full_df.rename(columns={col:stat.cols[col]['ID']},inplace=True)
                except KeyError:
                    continue
            print(self.full_df)

    def parse_details(self,df):
        df=df[['Batting','Details']]
        detail_vals=['HR','2B','3B','GDP','HBP','SF']
        for val in detail_vals:
            self.full_df[val]=0

            mask=self.full_df["Details"].astype(str).str.contains(val, na=False)

            self.full_df.loc[mask,val]=(
                self.full_df.loc[mask,"Details"]
                    .str.extract(rf'(\d+)·{val}|(?<!·)\b{val}\b')[0]
                    .fillna(1)
                    .astype(int)
            )

class Stat_Cat(ABCMeta): # any flat class used to define a statistical category must inherit this
    registry = []

    def __new__(cls, name, bases, attrs):
        new_cls = super().__new__(cls, name, bases, attrs)

        if not attrs.get('__abstractmethods__', False):
            required_attrs = ['id', 'cols']#, 'cat',  'value_vars', 'identifier', 'stat_lookup']
            for attr in required_attrs:
                if not hasattr(new_cls, attr):
                    raise TypeError(f"Class {name} must define '{attr}'")

            Stat_Cat.registry.append(new_cls)

        return new_cls

class Batting(metaclass=Stat_Cat):
    id='batting'
    cols = {
    "AB": {"type": pd.Int64Dtype(), "ID": "H1"},
    "R": {"type": pd.Int64Dtype(), "ID": "H2"},
    "H": {"type": pd.Int64Dtype(), "ID": "H3"},
    "RBI": {"type": pd.Int64Dtype(), "ID": "H4"},
    "BB": {"type": pd.Int64Dtype(), "ID": "H5"},
    "SO": {"type": pd.Int64Dtype(), "ID": "H6"},
    "PA": {"type": pd.Int64Dtype(), "ID": "H7"},
    "BA": {"type": pd.Float64Dtype(), "ID": "H8"},
    "OBP": {"type": pd.Float64Dtype(), "ID": "H9"},
    "SLG": {"type": pd.Float64Dtype(), "ID": "H10"},
    "OPS": {"type": pd.Float64Dtype(), "ID": "H11"},
    "Pit": {"type": pd.Int64Dtype(), "ID": "H12"},
    "Str": {"type": pd.Int64Dtype(), "ID": "H13"},
    "WPA": {"type": pd.Float64Dtype(), "ID": "H14"},
    "aLI": {"type": pd.Float64Dtype(), "ID": "H15"},
    "WPA+": {"type": pd.Float64Dtype(), "ID": "H16"},
    "WPA-": {"type": pd.Float64Dtype(), "ID": "H17"},
    "cWPA": {"type": pd.Float64Dtype(), "ID": "H18"},
    "acLI": {"type": pd.Float64Dtype(), "ID": "H19"},
    "RE24": {"type": pd.Float64Dtype(), "ID": "H20"},
    "PO": {"type": pd.Int64Dtype(), "ID": "H21"},
    "A": {"type": pd.Int64Dtype(), "ID": "H22"}}
    extra_cols={
    'HR': {"type": pd.Int64Dtype(), "ID": "H23"},
    '2B': {"type": pd.Int64Dtype(), "ID": "H24"},
    '3B': {"type": pd.Int64Dtype(), "ID": "H25"},
    'GDP': {"type": pd.Int64Dtype(), "ID": "H26"},
    'HBP': {"type": pd.Int64Dtype(), "ID": "H27"},
    'SF': {"type": pd.Int64Dtype(), "ID": "H28"}
    }
    cat='batting'
    filter_col='AB'
    cleaning={
        'cWPA':[{'target':'%','replace_with':''}]
    }

class Pitching(metaclass=Stat_Cat):
    id='pitching'
    cols={
    "IP": {"type": np.float64, "ID": "P1"},
    "H": {"type": np.int64, "ID": "P2"},
    "R": {"type": np.int64, "ID": "P3"},
    "ER": {"type": np.int64, "ID": "P4"},
    "BB": {"type": np.int64, "ID": "P5"},
    "SO": {"type": np.int64, "ID": "P6"},
    "HR": {"type": np.int64, "ID": "P7"},
    "ERA": {"type": np.float64, "ID": "P8"},
    "BF": {"type": np.int64, "ID": "P9"},
    "Pit": {"type": np.int64, "ID": "P10"},
    "Str": {"type": np.int64, "ID": "P11"},
    "Ctct": {"type": np.int64, "ID": "P12"},
    "StS": {"type": np.int64, "ID": "P13"},
    "StL": {"type": np.int64, "ID": "P14"},
    "GB": {"type": np.int64, "ID": "P15"},
    "FB": {"type": np.int64, "ID": "P16"},
    "LD": {"type": np.int64, "ID": "P17"},
    "Unk": {"type": np.int64, "ID": "P18"},
    "GSc": {"type": np.float64, "ID": "P19"},
    "IR": {"type": np.int64, "ID": "P20"},
    "IS": {"type": np.int64, "ID": "P21"},
    "WPA": {"type": np.float64, "ID": "P22"},
    "aLI": {"type": np.float64, "ID": "P23"},
    "cWPA": {"type": np.float64, "ID": "P24"},
    "acLI": {"type": np.float64, "ID": "P25"},
    "RE24": {"type": np.float64, "ID": "P26"}
    }
    cat='pitching'
    cleaning={
        'cWPA':[{'target':'%','replace_with':''}]
    }
    
class Game(extractor.Dimension):
    def __init__(self,row):
        if row._5=='@':
            game_name=f"{row.Tm} at {row.Opp} {row.Date}"
        else:    
            game_name=f"{row.Opp} at {row.Tm} {row.Date}"
        self.streak=self.parse_streak(
            row.Streak
        )
        self.game_name=game_name
        self.result=row.W_L
        self.R=row.R
        self.RA=row.RA
        self.GB=row.GB
        self.TOD=row.D_N
        self.attendance=row.Attendance
        self.cLI=row.cLI
        self.row=[self.game_name,self.streak,self.result,self.R,self.RA,self.GB,self.TOD,self.attendance,self.cLI]
    
    def parse_streak(self,streak_det):
        streak=''
        length=str(len(streak_det))
        if streak_det[0]=='+':
            streak+='W'
        else:
            streak+='L'
        streak+=length
        return streak

class Team_Schedule(extractor.HTML_Extraction):
    id='team_schedule'
    expected_cols={'Gm#':np.int64,'Date':str,'':str,'Tm':str,'':str,'Opp':str,'W/L':np.int64,'R':np.int64,'RA':str,'Inn':str,'W-L':str,'Rank':str,'GB':str,'Win':str,'Loss':str,'Save':np.int64}
    cat='schedule'
    cleaning={
        'attendance':[{'target':',','replace_with':''}]}

class Dim_Games(extractor.Dimension):
    def __init__(self,soups):
        for soup in soups:
            table=extractor.Dimension(Team_Schedule,soup)
            df=table.df
            df=df.rename(columns={'W/L':'W_L','D/N':'D_N'})
            rows=[]
            for row in df.itertuples():
                if row.Tm=='Tm':
                    continue
                game=Game(row)
                rows.append(game.row)
            games_df=pd.DataFrame(rows,columns=['game_name','streak','result','R','RA','GB','TOD','attendance','cLI'])
            games_df['attendance']=games_df['attendance'].str.replace(',','').astype(np.int64)
            games_df=extractor.Dimension.generate_id_from_column(games_df,['game_name'],'game_ID')

run_pipeline(2025)