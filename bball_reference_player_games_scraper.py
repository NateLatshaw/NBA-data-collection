import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from requests import get
import time
import gc
gc.collect()

# define function to scrape player game logs from Basketball Reference by month
# input season and month of the year to be scraped
# these dates will be placed in the output file name as well
# provide the path where the resulting data will be saved

def player_logs_scraper(season_, month_, out_path_):
    # set parameters
    url_season = season_ + 1
    counter = 0
    
    # loop through all pages of 100 rows, break when table finishes
    data_all = []
    print('PROGRESS')
    print(0)
    while counter < 100000:
        # set url
        url = 'https://www.basketball-reference.com/play-index/pgl_finder.cgi?request=1&match=game&' + \
              'year_min=' + str(url_season) + '&year_max=' + str(url_season) + \
              '&is_playoffs=N&age_min=0&age_max=99&game_month=' + str(month_) + '&season_start=1&' + \
              'season_end=-1&pos_is_g=Y&pos_is_gf=Y&pos_is_f=Y&pos_is_fg=Y&pos_is_fc=Y&pos_is_c=Y&pos_is_cf=Y&' + \
              'order_by=date_game&order_by_asc=Y&offset=' + str(counter)
        # some months have different URLs
        if (season_ == 2013 & (month_ == 11 | month_ == 3)) | \
        (season_ == 2014 & (month_ == 3)) | \
        (season_ == 2017 & (month_ == 3 | month_ == 4)) | \
        (season_ == 2016 & (month_ == 11 | month_ == 12)) | \
        (season_ == 2018 & (month_ == 1) | \
        (season_ == 2010 & (month_ == 1 | month_ == 2))):
            url = 'https://www.basketball-reference.com/play-index/pgl_finder.cgi?request=1&player_id=&' + \
            'match=game&year_min=' + str(url_season) + '&year_max=' + str(url_season) + \
            '&age_min=0&age_max=99&team_id=&opp_id=&season_start=1&season_end=-1&is_playoffs=N&draft_year=' + \
            '&round_id=&game_num_type=&game_num_min=&game_num_max=&game_month=' + str(month_) + \
            '&game_day=&game_location=&game_result=&is_starter=&is_active=&is_hof=&pos_is_g=Y&pos_is_gf=Y&' + \
            'pos_is_f=Y&pos_is_fg=Y&pos_is_fc=Y&pos_is_c=Y&pos_is_cf=Y&c1stat=&c1comp=&c1val=&' + \
            'c1val_orig=&c2stat=&c2comp=&c2val=&c2val_orig=&c3stat=&c3comp=&c3val=&c3val_orig=&c4stat=&' + \
            'c4comp=&c4val=&c4val_orig=&is_dbl_dbl=&is_trp_dbl=&order_by=date_game&order_by_asc=Y&offset=' + \
            str(counter)
        # parse page, break loop if table is NoneType
        data = []
        response = get(url)
        html_soup = BeautifulSoup(response.text, 'html.parser')
        table = html_soup.find('div', attrs = {'class': 'table_outer_container'})
        if table is None:
            break
        table_body = table.find('tbody')
        rows = table_body.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele])
        # append scraped data to dataframe
        data_all = data_all + data
        # add 100 to counter
        counter += 100
        print(counter)
        # wait 15 seconds to avoid aggressively scraping
        # this is 5x the crawl delay in their robots.txt file
        time.sleep(15)
    
    # create dataframe
    df = pd.DataFrame(data_all, columns = ['PLAYERNAME', 'AGE', 'POSITION', 'GAMEDATE', 'TEAM', \
                                           'OPPONENT', 'WL', 'STARTER', 'MIN', 'FGM', 'FGA', 'FGPERC', 'MADE2S', \
                                           'ATTEMPT2S', 'PERC2S', 'MADE3S', 'ATTEMPT3S', 'PERC3S', 'FTM', 'FTA', \
                                           'FTPERC', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', \
                                           'PTS', 'GAMESCORE', 'EXTRA1'])
    
    # FIX DATAFRAME
    df
    # remove rows with missing player names
    df = df.loc[df.PLAYERNAME.notnull()]
    check_rows = df.shape[0]
    
    # fix @ in between TEAM and OPPONENT
    df['HOME'] = df['OPPONENT'] != '@'
    df['HOME'] = df['HOME'].astype(str)
    df['EXTRA2'] = 'extra2'
    df_clean = df.loc[df.HOME == 'True']
    df_broken = df.loc[df.HOME == 'False']
    mask = ~(df_broken.columns.isin(['PLAYERNAME', 'AGE', 'POSITION', 'GAMEDATE', 'TEAM', 'HOME']))
    cols_to_shift = df_broken.columns[mask]
    df_broken[cols_to_shift] = df_broken.loc[:,mask].shift(periods = -1, axis = 1)
    df = pd.concat([df_clean, df_broken], axis = 0)
    
    # fix missing FGPERC for those who did not attempt a shot
    df['EXTRA2'] = 'extra2'
    df_clean = df.loc[df.FGA != '0']
    df_broken = df.loc[df.FGA == '0']
    mask = ~(df_broken.columns.isin(['PLAYERNAME', 'AGE', 'POSITION', 'GAMEDATE', 'TEAM', \
                                     'OPPONENT', 'WL', 'STARTER', 'MIN', 'FGM', 'FGA', 'HOME']))
    cols_to_shift = df_broken.columns[mask]
    df_broken[cols_to_shift] = df_broken.loc[:,mask].shift(periods = 1, axis = 1)
    df_broken['FGPERC'] = np.nan
    df = pd.concat([df_clean, df_broken], axis = 0)
    
    # fix missing PERC2S for those who did not attempt a 2 pointer
    df['EXTRA2'] = 'extra2'
    df_clean = df.loc[df.ATTEMPT2S != '0']
    df_broken = df.loc[df.ATTEMPT2S == '0']
    mask = ~(df_broken.columns.isin(['PLAYERNAME', 'AGE', 'POSITION', 'GAMEDATE', 'TEAM', \
                                     'OPPONENT', 'WL', 'STARTER', 'MIN', 'FGM', 'FGA', 'FGPERC', \
                                     'MADE2S', 'ATTEMPT2S', 'HOME']))
    cols_to_shift = df_broken.columns[mask]
    df_broken[cols_to_shift] = df_broken.loc[:,mask].shift(periods = 1, axis = 1)
    df_broken['PERC2S'] = np.nan
    df = pd.concat([df_clean, df_broken], axis = 0)
    
    # fix missing PERC3S for those who did not attempt a 3 pointer
    df['EXTRA2'] = 'extra2'
    df_clean = df.loc[df.ATTEMPT3S != '0']
    df_broken = df.loc[df.ATTEMPT3S == '0']
    mask = ~(df_broken.columns.isin(['PLAYERNAME', 'AGE', 'POSITION', 'GAMEDATE', 'TEAM', \
                                     'OPPONENT', 'WL', 'STARTER', 'MIN', 'FGM', 'FGA', 'FGPERC', \
                                     'MADE2S', 'ATTEMPT2S', 'PERC2S', 'MADE3S', 'ATTEMPT3S', 'HOME']))
    cols_to_shift = df_broken.columns[mask]
    df_broken[cols_to_shift] = df_broken.loc[:,mask].shift(periods = 1, axis = 1)
    df_broken['PERC3S'] = np.nan
    df = pd.concat([df_clean, df_broken], axis = 0)
    
    # fix missing FTPERC for those who did not attempt a free throw
    df['EXTRA2'] = 'extra2'
    df_clean = df.loc[df.FTA != '0']
    df_broken = df.loc[df.FTA == '0']
    mask = ~(df_broken.columns.isin(['PLAYERNAME', 'AGE', 'POSITION', 'GAMEDATE', 'TEAM', \
                                     'OPPONENT', 'WL', 'STARTER', 'MIN', 'FGM', 'FGA', 'FGPERC', \
                                     'MADE2S', 'ATTEMPT2S', 'PERC2S', 'MADE3S', 'ATTEMPT3S', \
                                     'PERC3S', 'FTM', 'FTA', 'HOME']))
    cols_to_shift = df_broken.columns[mask]
    df_broken[cols_to_shift] = df_broken.loc[:,mask].shift(periods = 1, axis = 1)
    df_broken['FTPERC'] = np.nan
    df = pd.concat([df_clean, df_broken], axis = 0)
    
    # fill missing GAMESCORE values
    df.loc[df.GAMESCORE.isnull(), 'GAMESCORE'] = 9999
    
    # drop extra columns, make sure all non % columns have 0 NAs
    df.drop(['EXTRA1', 'EXTRA2'], axis = 1, inplace = True)
    mask = ~(df.columns.isin(['FGPERC', 'PERC2S', 'PERC3S', 'FTPERC']))
    cols = df.columns[mask]
    assert sum(df[cols].isnull().sum() > 0) == 0, 'A column has an NA when it should not'
    assert df.shape[0] == check_rows, 'Shape of dataframe changed incorrectly'
    
    # write to csv
    if(month_ < 7):
        season_ += 1
    df.to_csv(out_path_ + 'player_games_' + str(season_) + str(month_) + '.csv', index = False)
    print('DATA FORMATTED AND SAVED. SHAPE: ', df.shape)


