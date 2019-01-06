"""

Author: Rosenbaum, Richard
Date: 28/12/2018

Description:

    1. Scrape
        a. sportsbookreview.com/betting-odds/nba-basketball/money-line
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
from datetime import date, timedelta

"""
Create directory to store data in 
"""

td = datetime.today().strftime('%m_%d_%Y'); path = "./" + td + "/"
os.system( "mkdir {}".format(td) )

print("\n ====== sportsbooksreview.com/betting-odds/nba-basketball SCRAPING ====== \n")

# Define dictionary for all url parsing
# select full game
# select money-line
nba_stats_info = {"https://www.sportsbookreview.com/betting-odds/nba-basketball/": \
                  [["/html/body/div/div/div/div/section/div/div[3]/div[1]/div[1]/ol/li[1]", \
                    "/html/body/div/div/div/div/section/div/div[3]/div[1]/div[2]/ol/li[2]"],\
                  "bettingOddsGridContainer",\
                  [8,10] + [i for i in range(12,20)] + [23],\
                  ['%FGA','%3PA','%FTA','%OREB','%DREB','%REB','%AST','%TOV','%STL','%BLK','%PTS'],\
                  18,\
                  3,\
					"/html/body/div/div/div/div/section/div/div[3]/div[2]/div[3]/div[3]/div[1]/div/div",\
					"html/body/div/div/div/div/section/div/div[3]/div[2]/div[3]/div[2]/div/div/div[2]"]}

# calendar
# /html/body/div/div/div/div/section/div/div[2]/div/div[1]/span[5]

# body of calendar
# /html/body/div[3]/div/div/section/div/div/div[2]/div[2]/table

# Define function to scrape nba stats
def scrape_nba_odds(url, days_xpaths):
	df_final = pd.DataFrame()

	def populate(data, colnames, teamNames):
		game_id = 0
		team_count = 0
		odds_count = 0
		bookie_count = 0
		dt = data[0]
		
		for idx, elm in enumerate(data):

			# if element is a valid team name
			if elm in teamNames:
				colnames[1].append(dt)
				colnames[2].append(elm)
				colnames[0].append(game_id)
				team_count += 1

				# increment game_id once two teams are selected
				if team_count % 2 == 0:
					game_id += 1
					odds_count = 0
					bookie_count = 0

			# to account for overtime games, increment to the "wager" field
			# and then count backward to get total points
			if elm[-1] == "%" and data[idx-1][-1] == "%":
				colnames[3].append(data[idx-3])
				colnames[3].append(data[idx-2])
		
			if elm[-1] == "%":
				colnames[4].append(elm)
			
			# start appending odds
			if elm[0] == '+' or elm[0] == '-':
				colnames[5+bookie_count].append(elm)
				odds_count += 1
		
				if odds_count % 2 == 0 and bookie_count < 4:
					bookie_count +=1

		print('Scraping', dt, '................')
				
		return colnames


	def populate_after_click(data, colnames,teamNames):
		odds_count = 0
		bookie_count = 0
		team_count = 0

		for idx, elm in enumerate(data):

			# if element is a valid team name
			if elm in teamNames:
				team_count += 1

				# increment game_id once two teams are selected
				if team_count % 2 == 0:
					odds_count = 0
					bookie_count = 0

			if elm[0] == '+' or elm[0] == '-':
				colnames[bookie_count].append(elm)
				odds_count += 1
		        
				if odds_count % 2 == 0 and bookie_count < 4:
					bookie_count +=1

		return colnames

    
	## Unpack data
	#url = "https://www.sportsbookreview.com/betting-odds/nba-basketball/"
	xpaths = nba_stats_info[url][0]
	object_id = nba_stats_info[url][1]
	stats_args = nba_stats_info[url][2]
	cols = nba_stats_info[url][3]
	dp = nba_stats_info[url][4]
	fn = nba_stats_info[url][5]
	click_arrow = nba_stats_info[url][6]
	click_arrow2 = nba_stats_info[url][7]

	## Set up chrome driver, open browser
	path_to_chromedriver = "./web_scraping/chromedriver"
	browser = webdriver.Chrome(executable_path=path_to_chromedriver)
	browser.get(url);
	time.sleep(4)  ## make sure the browswer loads before executing xpaths

	# select calendar	
	cal = '/html/body/div/div/div/div/section/div/div[2]/div/div/span'
	switch_month = '/html/body/div[5]/div/div/section/div/div/div[2]/div[1]/div/a[2]'


	except_ind = 0
	switch_month_ind = 0

	for day in days_xpaths:

		if switch_month_ind == 1:
			browser.find_element_by_xpath(switch_month).click();
			time.sleep(2)
		# t1 = time.process_time()
		# some dates not available due to no games. cannot select in browser
		try:
			# select calendar			
			if except_ind == 0:
				browser.find_element_by_xpath(cal).click();
			time.sleep(2)
		
			# select date
			browser.find_element_by_xpath(day).click();
			time.sleep(1)

			## Use the xpaths to click the necessary browswer buttons
			for xpath in xpaths:
				browser.find_element_by_xpath(xpath).click();
				time.sleep(2)  # Sleep in between



			## Populate containers
			table = browser.find_element_by_id(object_id)

			# lists
			teamNames = ['Phoenix','Orlando','Washington','Detroit','Indiana','Atlanta',
						'Toronto','Miami','Charlotte','Brooklyn','Cleveland','Memphis','Minnesota',
						'Chicago','New Orleans','Dallas','Denver','San Antonio','Sacramento','L.A. Clippers',
						'L.A. Lakers','Philadelphia','Boston', 'New York','Houston','Oklahoma City',
						'Golden State','Utah','Portland','Milwaukee']

			# Containers
			GameID = []
			Date = []
			Teams = []
			Points = []
			Wagers = []
			Opener = []
			Pinnacle = []
			Fivedimes = []
			Bookmaker = []
			BetOnline = []
			Bovada = []
			Heritage = []
			Intertops = []
			Youwager = []
			Opener2 = []
			Opener3 = []
			Justbet = []
			Sportsbetting = []
			Gtbets = []
			Sportbet = []
			Opener4 = []
			Nitrogensports = []
			Betphoenix = []
			Betmania = []
			Skybook = []
			Opener5 = []
			Mybookie = []
			Abcislands = []
			Jazz = []
			Sportsinteraction = []
			Opener6 = []
			Betcris = []
			Sbr = []
			Wagerweb = []
			Thegreek = []
			Opener7 = []
			Bodog = []
			Matchbook = []
			Looselines = []
			Bet365 = []

			# grab data and run populate function
			colnames = [GameID, Date, Teams, Points, Wagers, Opener, Pinnacle, Fivedimes, Bookmaker, BetOnline]
			data = table.text.split("\n")[dp:]
			pop = populate(data,colnames,teamNames)
			
			if ' 01, ' in data[0]:
				switch_month_ind = 1
			else:
				switch_month_ind = 0

			# Create dataframe
			dfout = pd.DataFrame(pop,
								index= ['GameID', 'Date', 'Teams', 'Points', 'Wagers',
										'Opener', 'Pinnacle', 'Fivedimes', 'Bookmaker', 'BetOnline']).T

			# ------------------------------------
			# redo process for next set of bookies
			browser.find_element_by_xpath(click_arrow).click();
			time.sleep(1)
			table_after = browser.find_element_by_id(object_id)

			# grab data and populate
			colnames2 = [Opener2, Bovada, Heritage, Intertops, Youwager]
			data_after = table_after.text.split("\n")
			pop_after = populate_after_click(data_after,colnames2,teamNames)

			# create dataframe
			dfout_after = pd.DataFrame(pop_after,
						    index= ['Opener2','Bovada', 'Heritage', 'Intertops', 'Youwager']).T

			# concatenate to form master df
			dfout = pd.concat([dfout, dfout_after], axis=1)
			# ------------------------------------


			# ------------------------------------
			# redo process for next set of bookies
			browser.find_element_by_xpath(click_arrow2).click();
			time.sleep(1)
			table_after2 = browser.find_element_by_id(object_id)

			colnames3 = [Opener3, Justbet, Sportsbetting, Gtbets, Sportbet]
			data_after2 = table_after2.text.split("\n")
			pop_after2 = populate_after_click(data_after2,colnames3,teamNames)

			# dataframe
			dfout_after2 = pd.DataFrame(pop_after2,
							index= ['Opener3','Justbet', 'Sportsbetting', 'Gtbets', 'Sportbet']).T

			dfout = pd.concat([dfout, dfout_after2], axis=1)
			# ------------------------------------


			# ------------------------------------
			# redo process for next set of bookies
			browser.find_element_by_xpath(click_arrow2).click();
			time.sleep(1)
			table_after3 = browser.find_element_by_id(object_id)

			colnames4 = [Opener4, Nitrogensports, Betphoenix, Betmania, Skybook]
			data_after3 = table_after3.text.split("\n")
			pop_after3 = populate_after_click(data_after3,colnames4,teamNames)

			# dataframe
			dfout_after3 = pd.DataFrame(pop_after3,
							index= ['Opener4','Nitrogensports', 'Betphoenix', 'Betmania', 'Skybook']).T

			dfout = pd.concat([dfout, dfout_after3], axis=1)
			# ------------------------------------


			# ------------------------------------
			# redo process for next set of bookies
			browser.find_element_by_xpath(click_arrow2).click();
			time.sleep(1)
			table_after4 = browser.find_element_by_id(object_id)

			colnames5 = [Opener5, Mybookie, Abcislands, Jazz, Sportsinteraction]
			data_after4 = table_after4.text.split("\n")
			pop_after4 = populate_after_click(data_after4,colnames5,teamNames)

			# dataframe
			dfout_after4 = pd.DataFrame(pop_after4,
							index= ['Opener5','Mybookie', 'Abcislands', 'Jazz', 'Sportsinteraction']).T

			dfout = pd.concat([dfout, dfout_after4], axis=1)
			# ------------------------------------


			# ------------------------------------
			# redo process for next set of bookies
			browser.find_element_by_xpath(click_arrow2).click();
			time.sleep(1)
			table_after5 = browser.find_element_by_id(object_id)

			colnames6 = [Opener6, Betcris, Sbr, Wagerweb, Thegreek]
			data_after5 = table_after5.text.split("\n")
			pop_after5 = populate_after_click(data_after5,colnames6,teamNames)

			# dataframe
			dfout_after5 = pd.DataFrame(pop_after5,
							index= ['Opener6','Betcris', 'Sbr', 'Wagerweb', 'Thegreek']).T

			dfout = pd.concat([dfout, dfout_after5], axis=1)
			# ------------------------------------

				# ------------------------------------
			# redo process for next set of bookies
			browser.find_element_by_xpath(click_arrow2).click();
			time.sleep(1)
			table_after6 = browser.find_element_by_id(object_id)

			colnames7 = [Opener7, Bodog, Matchbook, Looselines, Bet365]
			data_after6 = table_after6.text.split("\n")
			pop_after6 = populate_after_click(data_after6,colnames7,teamNames)

			# dataframe
			dfout_after6 = pd.DataFrame(pop_after6,
							index= ['Opener7','Bodog', 'Matchbook', 'Looselines', 'Bet365']).T

			dfout = pd.concat([dfout, dfout_after6], axis=1)
			# ------------------------------------

			df_final = df_final.append(dfout)
			except_ind = 0
		
			# doesn't count sleep time - pretty fast if we can lower sleep time
			# t2 = time.process_time()
			# print('Time Elapsed:', t2-t1, 'seconds')
		except:
			print('ERROR loading data')
			except_ind = 1
			time.sleep(1)


	browser.quit()

	return df_final









