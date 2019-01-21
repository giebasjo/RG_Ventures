"""

Author: Rosenbaum, Richard
Date: 21/1/2019

Description:

    1. Scrape
        a. sportsbookreview.com/betting-odds/nba-basketball/money-line
		b. other sports
    2. Merge all data together as master_df - create "historical database"

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
import logging
import math
import scipy.stats
from sklearn.linear_model import LinearRegression

from multiprocessing import current_process
from datetime import date, timedelta, datetime

td = datetime.today().strftime('%m_%d_%Y'); path = "./" + td + "/"
os.system( "mkdir {}".format(td) )


#urls = ["https://www.sportsbookreview.com/betting-odds/nba-basketball/money-line",
#		"https://www.sportsbookreview.com/betting-odds/ncaa-basketball/money-line/"]


# --------------- INPUTS --------------- 
urls = ["https://www.sportsbookreview.com/betting-odds/nba-basketball/money-line"]
tables = ["bettingOddsGridContainer"]
alpha = 0.05
# --------------- INPUTS ---------------

begin_index = 18

def scrape(urls, alpha):

	# initial populate function
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
			# control for non-reporting of wager % before March 12, 2014
			if elm[-1] == "-" and data[idx-1][-1] == "-":
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

		logging.info('Scraping %s.....', dt)
				
		return colnames

	# populate df after first arrow click
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

	# scrape page for respective url
	def scrape_one(url):
		df_final = pd.DataFrame()
		browser.get(url);
		time.sleep(1)
		table = browser.find_element_by_id(tables[0])
		teamNames = ['Phoenix','Orlando','Washington','Detroit','Indiana','Atlanta',
					'Toronto','Miami','Charlotte','Brooklyn','Cleveland','Memphis','Minnesota',
					'Chicago','New Orleans','Dallas','Denver','San Antonio','Sacramento','L.A. Clippers',
					'L.A. Lakers','Philadelphia','Boston', 'New York','Houston','Oklahoma City',
					'Golden State','Utah','Portland','Milwaukee']
		
		click_arrow = "/html/body/div/div/div/div/section/div/div[3]/div[2]/div[3]/div[3]/div[1]/div/div"
		click_arrow2 = "/html/body/div/div/div/div/section/div/div[3]/div[2]/div[3]/div[2]/div/div/div[2]"

		# if games have already started, need to go one more row down
		click_arrow_after_start = "/html/body/div/div/div/div/section/div/div[3]/div[2]/div[4]/div[3]/div/div/div"
		click_arrow_after_start2 = "/html/body/div/div/div/div/section/div/div[3]/div[2]/div[4]/div[2]/div/div/div[2]"



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
		data = table.text.split("\n")[begin_index:]
		pop = populate(data,colnames,teamNames)

		# Create dataframe
		dfout = pd.DataFrame(pop,
							index= ['GameID', 'Date', 'Teams', 'Points', 'Wagers',
									'Opener', 'Pinnacle', 'Fivedimes', 'Bookmaker', 'BetOnline']).T

		# ------------------------------------
		# redo process for next set of bookies
		try:
			browser.find_element_by_xpath(click_arrow).click();
		except:
			browser.find_element_by_xpath(click_arrow_after_start).click();

		time.sleep(1)
		table_after = browser.find_element_by_id(tables[0])

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
		try:
			browser.find_element_by_xpath(click_arrow2).click();
		except:
			browser.find_element_by_xpath(click_arrow_after_start2).click();

		time.sleep(1)
		table_after2 = browser.find_element_by_id(tables[0])

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
		try:
			browser.find_element_by_xpath(click_arrow2).click();
		except:
			browser.find_element_by_xpath(click_arrow_after_start2).click();

		time.sleep(1)
		table_after3 = browser.find_element_by_id(tables[0])

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
		try:
			browser.find_element_by_xpath(click_arrow2).click();
		except:
			browser.find_element_by_xpath(click_arrow_after_start2).click();

		time.sleep(1)
		table_after4 = browser.find_element_by_id(tables[0])

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
		try:
			browser.find_element_by_xpath(click_arrow2).click();
		except:
			browser.find_element_by_xpath(click_arrow_after_start2).click();

		time.sleep(1)
		table_after5 = browser.find_element_by_id(tables[0])

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
		try:
			browser.find_element_by_xpath(click_arrow2).click();
		except:
			browser.find_element_by_xpath(click_arrow_after_start2).click();

		time.sleep(1)
		table_after6 = browser.find_element_by_id(tables[0])

		colnames7 = [Opener7, Bodog, Matchbook, Looselines, Bet365]
		data_after6 = table_after6.text.split("\n")
		pop_after6 = populate_after_click(data_after6,colnames7,teamNames)

		# dataframe
		dfout_after6 = pd.DataFrame(pop_after6,
						index= ['Opener7','Bodog', 'Matchbook', 'Looselines', 'Bet365']).T

		dfout = pd.concat([dfout, dfout_after6], axis=1)
		# ------------------------------------

		df_final = df_final.append(dfout)
		browser.quit()
		return df_final


	## Set up chrome driver, open browser
	path_to_chromedriver = "./web_scraping/chromedriver"
	browser = webdriver.Chrome(executable_path=path_to_chromedriver)
	
	
	for url in urls:
		data = scrape_one(url)

		# ---------------- TAKE ALL THE BELOW CODE FROM THE CLEAN DATA NOTEBOOK ----------------
		# CLEAN DATA

		# make sure each row has a date, teams, and points
		include = data['Date'].notnull() & data['Teams'].notnull() & data['Points'].notnull()
		
		# filter data to remove issues
		data = data[include]

		# fill NaN values with dash so they are not counted in odds calculation
		data = data.fillna(value='-')

		# remove unnamed column
		data = data.drop(['Opener2','Opener3', 'Opener4','Opener5','Opener6','Opener7'],axis=1)
		

		# calculate expectations and signals
		data['Consensus_Minus_Alpha'] = data['Average_Probs'] - alpha
		
		filter_signal = data['Max_Implied_Prob'] < data['Consensus_Minus_Alpha']
		data['Signal'] = [int(x) for x in filter_signal]
		
		# add payout for each bet if successful
		payout = [-100/x if int(x) < 0 else x/100 for x in data['Max_Odds']]
		data['Payout_if_Win'] = payout

		# add expected value of each bet - this will be numerator of kelly
		expectation = [data.iloc[i,43]*data.iloc[i,41]+ (-1)*(1-data.iloc[i,41]) for i in range(len(data))]
		data['Expected_Value'] = expectation

		# export cleaned data
		now = datetime.now()
		data.to_csv(path + str(now) + '.csv', sep=',')

		
		# IF WE PUT ON A POSITION
		# subset where signal = 1 so we put on a position
		bets_subset = data.iloc[np.where(data['Signal'] == 1)[0],:]
		
		# calculate kelly bet sizes
		kelly = np.array(bets_subset['Expected_Value']) / np.array(bets_subset['Payout_if_Win'])
		bets_subset['Kelly'] = kelly
		kelly_bet_size = np.array(bets_subset['Kelly']) * 100 
		kelly_bet_winnings = kelly_bet_size * np.array(bets_subset['Payout_if_Win'])

		if len(bets_subset) > 0:
			print('URGENT')
			print('*********************** ENTER INTO POSITION NOW ***********************')

		bets_subset.to_csv(path + '/Positive_Signals/' + str(now) + '.csv', sep=',')

			## ^^^^^^^^^^^^^^^ better way to do this ^^^^^^^^^^^^^^^^^^



if __name__ == '__main__':
	scrape(urls, alpha)







