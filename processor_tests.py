from NFL import Players_Table, Season, run_pipeline
from bs4 import BeautifulSoup
import pandas as pd
import ast
import json

#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

class HTML_Injection:
      def __init__(self,teamhtml=None,rosterhtml=None,weekhtmls=None):
            pass
            self.team_htmls=teamhtml
            self.roster_htmls=rosterhtml
            self.week_htmls=weekhtmls

class game_test_1_pipeline_settings:
    start_week=1
    end_week=1
    year=2024
    scrape_rosters=True
    scrape_teams=True
    scrape_games=True

class Game_Extraction_Test_1:
    def __init__(self):
        with open('fact_test_html_1.txt','r',encoding='utf-8') as f:
                    html=f.read()
        with open('full_week_test_rosters.txt', 'r', encoding='utf-8') as f:
            roster_dict = json.load(f)
        with open('full_week_test_teams.txt', 'r', encoding='utf-8') as f:
            team_dict = json.load(f)
        weekhtml={1:[html]}
        html=HTML_Injection(weekhtmls=weekhtml,teamhtml=team_dict,rosterhtml=roster_dict)
        obj=Season(html,game_test_1_pipeline_settings)

class game_test_2_pipeline_settings:
    start_week=1
    end_week=1
    year=2024
    scrape_rosters=True
    scrape_teams=True
    scrape_games=True

class Game_Extraction_Test_2: #pull all games from a specific week
    def __init__(self):
        with open("full_week_test_games.txt", "r", encoding="utf-8") as f:
            obj = ast.literal_eval(f.read())
        with open('full_week_test_rosters.txt', 'r', encoding='utf-8') as f:
            roster_dict = json.load(f)
        with open('full_week_test_teams.txt', 'r', encoding='utf-8') as f:
            team_dict = json.load(f)
        weekhtml={1:obj}
        html=HTML_Injection(weekhtmls=weekhtml,rosterhtml=roster_dict,teamhtml=team_dict)
        obj=Season(html,game_test_2_pipeline_settings)

class game_test_3_pipeline_settings:
    start_week=1
    end_week=2
    year=2024
    scrape_rosters=True
    scrape_teams=True
    scrape_games=True

class Game_Extraction_Test_3: #pull all games from a specific week
    def __init__(self):
        with open("full_week_test_games_3.txt", "r", encoding="utf-8") as f:
            weekhtml = ast.literal_eval(f.read())
        with open('full_week_test_rosters.txt', 'r', encoding='utf-8') as f:
            roster_dict = json.load(f)
        with open('full_week_test_teams.txt', 'r', encoding='utf-8') as f:
            team_dict = json.load(f)
        html=HTML_Injection(weekhtmls=weekhtml,rosterhtml=roster_dict,teamhtml=team_dict)
        obj=Season(html,game_test_3_pipeline_settings)

class Full_Season_pipeline_settings:
    start_week=1
    end_week=18
    year=2024
    scrape_rosters=True
    scrape_teams=True
    scrape_games=True

class Full_Season_Test:
    def __init__(self):
        with open("HTMLs\\full_season_test_games.txt", "r", encoding="utf-8") as f:
            weeks_dict = ast.literal_eval(f.read())
        with open('HTMLs\\full_week_test_teams.txt', 'r', encoding='utf-8') as f:
            teams_dict = json.load(f)
        with open('HTMLs\\full_week_test_rosters.txt', 'r', encoding='utf-8') as f:
            roster_dict = json.load(f)  
        self.html = HTML_Injection(weekhtmls=weeks_dict, rosterhtml=roster_dict, teamhtml=teams_dict)
        self.obj = Season(self.html, Full_Season_pipeline_settings)

class Test_NFL_Rosters:
    def __init__(self):
        with open('roster_test_html_2.txt','r',encoding='utf-8') as f:
            html=f.read()

        self.soup=BeautifulSoup(html,'html.parser')

        roster_obj=Players_Table(self.soup,2024)

class Cases:
    def __init__(self,test):
        if test==1:
            Game_Extraction_Test_1()
        elif test==2:
            Game_Extraction_Test_2()
        elif test==3:
            Game_Extraction_Test_3()
        elif test==4:
            Full_Season_Test()

Full_Season_Test()