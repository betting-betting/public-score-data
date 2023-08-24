import requests
import pandas as pd
import time
from log_notify import Logger,notify

from sql import df_to_sql
from datetime import datetime

class score_downloader:
    
    tennis_url = 'https://api.sofascore.com/api/v1/sport/tennis/events/live'
    
    soccer_url = 'https://api.sofascore.com/api/v1/sport/football/events/live'

    headers : dict = {
      'authority' : 'api.sofascore.com',
      'accept': '*/*',
      'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
      'cache-control' : 'max-age=0',
      'if-none-match' : 'W/^\^"36f4dd3abe^\^"', 
      'origin' : 'https://www.sofascore.com',
      'referer' : 'https://www.sofascore.com/',
      'sec-ch-ua' : '^\^"Chromium^\^";v=^\^"110^\^", ^\^"Not A(Brand^\^";v=^\^"24^\^", ^\^"Google Chrome^\^";v=^\^"110^\^"',
      'sec-ch-ua-mobile' : '?0',
      'sec-ch-ua-platform' : '^\^"Windows^\^"',
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode' : 'cors',
      'sec-fetch-site' : 'same-site',
      'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
        
        }

    
    def __init__(self,sport):
        self.sport = sport
        self.log = Logger('logs.log')
        self.notify = notify()
        
        
    
    
    def request(self) -> dict:
        if self.sport == 'tennis':
            url = self.tennis_url
        elif self.sport == 'soccer':
            url = self.soccer_url
            self.headers['if_none-match'] = 'W/^\^"7d2fbe8889^\^"'
        else:
            print('sport not set up')
            
        try:
            req = requests.get(url, headers = self.headers) 
            response : dict = req.json()
            return response['events']
        except Exception as error:
            print(f'Error occured: {error}')
            return response['error']
        
    
    def tennis_request_flattener(self) -> pd.DataFrame:
        event_data : list = []
        data = self.request()
        for event in data:
            tournament_organiser = event['tournament']['category']['name']
            tournament = event['tournament']['name']
            event_name = event['slug']
            home = event['homeTeam']['slug']
            away = event['awayTeam']['slug']
            try:
                court_type = event['groundType']
            except:
                court_type = 'Unknown'
            start_time = datetime.fromtimestamp(event['startTimestamp'])
            current_set = event['status']['description']
            inplay = event['status']['type']
            try:
                round_info = event['roundInfo']['slug']   #can add tie breaks into the bit below if you want
            except:
                round_info = 'Unknown'
            
            
            try:
                home_set1 = event['homeScore']['period1']
            except:
                home_set1 = 0
            try:
                home_set2 = event['homeScore']['period2']
            except:
                home_set2 = 0
            try:
                home_set3 = event['homeScore']['period3']
            except:
                home_set3 = 0
            try:
                home_set4 = event['homeScore']['period4']
            except:
                home_set4 = 0
            try:
                home_set5 = event['homeScore']['period5']
            except:
                home_set5 = 0
               
            try:
                home_current_game_points = str(event['homeScore']['point'])
            except:
                home_current_game_points = str(999)
                
            try:
                away_set1 = event['awayScore']['period1']
            except:
                away_set1 = 0
            try:
                away_set2 = event['awayScore']['period2']
            except:
                away_set2 = 0
            try:
                away_set3 = event['awayScore']['period3']
            except:
                away_set3 = 0
            try:
                away_set4 = event['awayScore']['period4']
            except:
                away_set4 = 0
            try:
                away_set5 = event['awayScore']['period5']
            except:
                away_set5 = 0
            
            try:
                away_current_game_points = str(event['awayScore']['point'])
            except:
                away_current_game_points = '999'
            
            
            try:
                firstserver = (lambda x: home if x == 1  else away )(event['firstToServe'])
            except:
                firstserver = None


            created_ts = datetime.now()
            
            event_data.append([tournament_organiser,tournament,event_name,home,away,court_type,start_time,inplay,
                               round_info,firstserver,current_set,home_set1,home_set2,home_set3,home_set4,home_set5,home_current_game_points,
                               away_set1,away_set2,away_set3,away_set4,away_set5,away_current_game_points,created_ts])
        flattened = pd.DataFrame(event_data)
        flattened.columns = ['TOURNAMENT_ORGANISER','TOURNAMENT','EVENT_NAME', 'HOME', 'AWAY', 'COURT_TYPE', 'START_TIME', 
                              'INPLAY', 'ROUND_INFO','CURRENT_SERVER','CURRENT_SET', 'HOME_SET1', 'HOME_SET2', 
                             'HOME_SET3', 'HOME_SET4', 'HOME_SET5', 'HOME_CURRENT_GAME_POINTS', 
                             'AWAY_SET1', 'AWAY_SET2', 'AWAY_SET3', 'AWAY_SET4', 'AWAY_SET5', 
                             'AWAY_CURRENT_GAME_POINTS', 'CREATED_TS']
        
        return flattened
    
    
    def soccer_request_flattener(self) -> pd.DataFrame:
        event_data : list = []
        data = self.request()
        for event in data:
            tournament = event['tournament']['name']
            event_name = event['slug']
            home = event['homeTeam']['slug']
            away = event['awayTeam']['slug']
            try:
                home_red_cards = event['homeRedCards']
            except:
                home_red_cards = -999
            try:
                away_red_cards = event['awayRedCards']
            except:
                away_red_cards = -999
            start_time = datetime.fromtimestamp(event['startTimestamp'])
            status = event['status']['description']
            inplay = event['status']['type']
            
            try:
                home_score = event['homeScore']['current']
            except:
                home_score = -999
            
                
            try:
                away_score = event['awayScore']['current']
            except:
                away_score = 0
            
                    
                    
                
            created_ts = datetime.now()
            
            event_data.append([tournament,event_name,home,away,start_time,status,inplay,
                               home_red_cards,away_red_cards,home_score,away_score,created_ts])
        flattened = pd.DataFrame(event_data)
        flattened.columns = ['TOURNAMENT','EVENT_NAME', 'HOME', 'AWAY', 'START_TIME', 'MATCH_STATUS',
                              'INPLAY', 'HOME_RED_CARDS','AWAY_RED_CARDS','HOME_SCORE','AWAY_SCORE','CREATED_TS']
        
        return flattened
           
    
    def inserter(self,sleep):
        if self.sport == 'tennis':
            while True:
                self.log.start()
                data : pd.DataFrame = self.tennis_request_flattener()
                df_to_sql('tennis_score_data', data)
                time.sleep(sleep)
                self.log.stop()
        elif self.sport == 'soccer':
            while True:
                data : pd.DataFrame = self.soccer_request_flattener()
                df_to_sql('soccer_score_data', data)
                time.sleep(sleep)
        else:
            print('Sport not handled for')
     

    
if __name__ == '__main__':
    sport = 'tennis'
    delay = 5
    score_downloader = score_downloader(sport)
    try: 
        score_downloader.inserter(delay)
    except Exception as e:
        score_downloader.log.start()
        print(f'Script Ended Error: {e}')
        score_downloader.log.stop()
        score_downloader.notify.send_message(e,'score_downloader')

    
    
        
        
        
        
        
        