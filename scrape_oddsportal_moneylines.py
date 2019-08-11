import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import gc
gc.collect()

output_path = 'E:/NBA data collection\Data\OddsPortal NBA Money Lines/'
# example url: https://www.oddsportal.com/basketball/usa/nba-2008-2009/results/#/page/1/
base_url = 'https://www.oddsportal.com/basketball/usa/nba-'

def Scraper(year_, page_, base_url_ = base_url):
    # format URL
    url = base_url_ + str(year_) + '-' + str(year_ + 1) + '/results/#/page/' + str(page_) + '/'
    # open browser
    browser = webdriver.Chrome(executable_path = 'C:/Drivers/chromedriver.exe')
    browser.get(url)
    soup = BeautifulSoup(browser.page_source, 'lxml')
    # collect data
    teams = soup.find_all('td', {'class':'name table-participant'})
    scores = soup.find_all('td', {'class':'center bold table-odds table-score'})
    mlL = soup.find_all(lambda tag: tag.name == 'td' and tag.get('class') == ['odds-nowrp'])
    mlW = soup.find_all('td', {'class':'result-ok odds-nowrp'})
    # pass data to dataframe
    df = pd.DataFrame()
    for i in range(0, len(teams)):
        if len(mlL) == len(mlW):
            df = df.append(pd.DataFrame([[teams[i].text, scores[i].text, mlL[i].text, mlW[i].text]]))
        else:
            # sometimes OddsPortal's HTML is messed up - this is a hack to not break the code
            # money lines of 0 can be hand corrected later
            df = df.append(pd.DataFrame([[teams[i].text, scores[i].text, 0, 0]]))
    browser.quit()
    return(df)

def RunScraper(year_, df_):
    print('Page Number:')
    for page in range(1, 100):
        print(page)
        # run Scraper
        tmp = Scraper(year_ = year, page_ = page)
        # break if no data is scraped
        if tmp.shape[0] == 0:
            break
        df_ = df_.append(tmp)
        time.sleep(8)
    df_.columns = ['Teams', 'Scores', 'ML1', 'ML2']
    return(df_)

##########################################################################################################

# scrape seasons 2008-2018
for year in range(2008, 2019):
    print(year)
    all_data = pd.DataFrame()
    all_data = RunScraper(year_ = year, df_ = all_data)
    all_data.to_csv(output_path + 'OddsPortal_NBA_MoneyLines_' + str(year) + '_' + str(year + 1) + '.csv', \
                    index = False, encoding = 'utf-8')

##########################################################################################################



