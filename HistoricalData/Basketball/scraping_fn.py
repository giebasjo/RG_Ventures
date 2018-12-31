"""

Author: Giebas, Jordan
Date: 30/09/2018

Description:

    1. Scrape
        a. nba.com/stats
        b. espn.com
        c. basketball-reference.com
    2. Merge all data together as master_df
    3. Create Directory ./{today's date}
        a. Store individual data sources to ./{today's date}/individual_srcs
        b. Store master dataframe to ./{today's date}/main

"""

# Import necessary modules
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns; sns.set()
from selenium import webdriver
from datetime import datetime
import time
import os

from multiprocessing import current_process

"""
Create directory to store data in 
"""

td = datetime.today().strftime('%m_%d_%Y'); path = "./" + td + "/"
os.system( "mkdir {}".format(td) )

"""
NBA.COM/STATS SCRAPE
"""

print("\n ====== NBA.com/stats SCRAPING ====== \n")

# Define dictionary for all url parsing
nba_stats_info = {"http://stats.nba.com/players/usage/": \
                  [["/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[2]",\
                   "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[2]/div/div/label/select/option[2]",\
                   "/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]"],\
                  "nba-stat-table__overflow",\
                  [8,10] + [i for i in range(12,20)] + [23],\
                  ['%FGA','%3PA','%FTA','%OREB','%DREB','%REB','%AST','%TOV','%STL','%BLK','%PTS'],\
                  1,\
                  3],\
                  "http://stats.nba.com/players/rebounding/": \
                  [["/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[1]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[2]/div/div/label/select/option[2]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[3]/div/div/label/select/option[2]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]"],\
                   "table",\
                   [6,7,8,9],\
                   ["Consested_REB", "Consested_REB%", "REB_changes", "REB_changes%"],\
                   8,\
                   2],\
                  "http://stats.nba.com/players/opponent/": \
                  [["/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[2]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[2]/div/div/label/select/option[2]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[3]/div/div/label/select/option[3]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]"],\
                   "nba-stat-table__overflow",\
                   [6,7,9,10,12] + [x for x in range(14,21)] + [24,25],\
                   ["opp_FGA/100p","opp_FG%","opp_3PA/100p","opp_3P%","opp_FTA/100p",\
                    "opp_OREB/100p","opp_DREB/100p","opp_REB/100p", "opp_AST/100p", \
                    "opp_TOV/100p", "opp_STL/100p", "opp_BLK/100p", "opp_PTS/100p", "opp_plusminus/100p"],\
                   40,\
                   3],\
                  "http://stats.nba.com/players/defense/": \
                  [["/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[2]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[2]/div/div/label/select/option[2]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[3]/div/div/label/select/option[3]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]"],\
                   "nba-stat-table__overflow",\
                   [-1],\
                   ["Def_WS/100p"],\
                   10,\
                   3],\
                  "http://stats.nba.com/players/advanced/": \
                  [["/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[2]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[2]/div/div/label/select/option[2]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]"],\
                   "nba-stat-table__overflow",\
                   [15,11,19,20],\
                   ["TO_ratio", "AST_ratio", "Pace", "PIE"],\
                   1,\
                   3],\
                  "http://stats.nba.com/players/touches/": \
                  [["/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[1]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[2]/div/div/label/select/option[2]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[3]/div/div/label/select/option[2]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]"],\
                   "nba-stat-table__overflow",\
                   [6,11],\
                   ["Touches/game", "PTS/Touch"],\
                   12,\
                   2],\
                  "http://stats.nba.com/players/speed-distance/": \
                  [["/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[1]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[2]/div/div/label/select/option[2]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[3]/div/div/label/select/option[2]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]"],\
                   "nba-stat-table__overflow",\
                   [y for y in range(6,12)],\
                   ["Dist_miles/g", "Dist_miles_off/g", "Dist_miles_def/g", "avg_speed/g", \
                    "avg_speed_off/g", "avg_speed_def/g"],\
                   1,\
                   2],\
                  "http://stats.nba.com/players/defense-dash-overall/": \
                  [["/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[1]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[2]/div/div/label/select/option[1]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[3]/div/div/label/select/option[2]",\
                    "/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]"],\
                   "nba-stat-table__overflow",\
                   [y for y in range(7,11)],\
                   ["DFGA","DFG%","FG%","DIFF%"],\
                   1,\
                   2] }


# Define function to scrape nba stats
def scrape_nba_stats(url):
    print("URL: ", url)

    def populate_2(data, stat_args, player_names, player_stats):

        for idx, elm in enumerate(data):
            if idx % 2 == 0:
                player_names.append(elm)
            else:
                player_stats.append(np.array(elm.split(" "))[stat_args])

        return player_names, player_stats

    def populate_3(data, stat_args, player_names, player_stats):

        for idx, elm in enumerate(data):
            if idx % 3 == 1:
                player_names.append(elm)
            elif idx % 3 == 2:
                player_stats.append(np.array(elm.split(" "))[stat_args])

        return player_names, player_stats

    ## Unpack data
    xpaths = nba_stats_info[url][0]
    class_name = nba_stats_info[url][1]
    stats_args = nba_stats_info[url][2]
    cols = nba_stats_info[url][3]
    dp = nba_stats_info[url][4]
    fn = nba_stats_info[url][5]

    ## Set up chrome driver, open browser
    path_to_chromedriver = "./web_scraping/chromedriver"
    browser = webdriver.Chrome(executable_path=path_to_chromedriver)
    browser.get(url);
    time.sleep(4)  ## make sure the browswer loads before executing xpaths
    
    ## Use the xpaths to click the necessary browswer buttons
    for xpath in xpaths:
        browser.find_element_by_xpath(xpath).click();
        time.sleep(4)  # Sleep in between
        
    print( "current process: ", current_process(), "Selected all XPATHs" )

    ## Populate containers
    table = browser.find_element_by_class_name(class_name)

    # Containers
    player_names = [];
    player_stats = []
    data = table.text.split("\n")[dp:]

    if fn == 2:
        player_names, player_stats = populate_2(data, stats_args, player_names, player_stats)
    else:
        player_names, player_stats = populate_3(data, stats_args, player_names, player_stats)

    # Create dataframe
    df = pd.DataFrame(columns=["PLAYER"] + cols)
    for i, col in enumerate(df.columns):
        if col == "PLAYER":
            df[col] = player_names
        else:
            df[col] = [j[i - 1] for j in player_stats]

    browser.quit()

    return df