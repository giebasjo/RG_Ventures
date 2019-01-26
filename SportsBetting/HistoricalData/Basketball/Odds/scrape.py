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
import schedule
import warnings

from multiprocessing import current_process
from datetime import date, timedelta, datetime

td = datetime.today().strftime('%m_%d_%Y'); path = "./" + td + "/"

try:
	os.system( "mkdir {}".format('./Data_Files/'+td) )
	os.system( "mkdir {}".format('./Data_Files/'+td+'/NBA') )
	os.system( "mkdir {}".format('./Data_Files/'+td+'/NCAAM') )
	os.system( "mkdir {}".format('./Data_Files/' + td + '/Positive_Signals'))
except:
	pass

# get rid of warning when appending to df
#warnings.filterwarnings("ignore", category=SettingWithCopyWarning)


# --------------- INPUTS --------------- 
path_to_chromedriver = "./web_scraping/chromedriver"

url_nba = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/money-line'
teams_nba = pd.read_csv('nba_teams.csv',header=None)
teams_nba = [str(x) for x in list(teams_nba.iloc[:,0])]

url_ncaam = 'https://www.sportsbookreview.com/betting-odds/ncaa-basketball/money-line/'
teams_ncaam = pd.read_csv('ncaam_teams.csv',header=None)
teams_ncaam = [str(x).replace('\xa0','') for x in list(teams_ncaam.iloc[:,0])]

tables = ["bettingOddsGridContainer"]

# DONT CHANGE
begin_index_nba = 18
begin_index_ncaam = 14

# bookie "edge"
alpha = 0.04

# minimum number of bookies supplying odds. fewer odds than the number below will be excluded
min_number_odds = 12

# how many outliers to remove (i.e. 2 means remove highest 2 and lowest 2 odds supplied)
num_outliers = 0

# when to start and end scrape
start_scrape = 6 #11am
end_scrape = 23	#11pm
# --------------- INPUTS ---------------


def scrape(url, alpha, min_number_odds, num_outliers):

	# --------------------- DEFINE FUNCTIONS START ---------------------
	# initial populate function
	def populate(data, colnames, team_names):
		game_id = 0
		team_count = 0
		odds_count = 0
		bookie_count = 0
		dt = data[0]
		days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
		
		for idx, elm in enumerate(data):

			# if element is a valid team name - need to adjust for rankings in ncaa bball
			dt_l = [1 if x in elm else 0 for x in days]
			if sum(dt_l) > 0: dt = elm

			if elm in team_names or elm[4:] in team_names or elm[5:] in team_names:
				colnames[1].append(dt)
				colnames[2].append(elm)
				colnames[0].append(game_id)
				team_count += 1

				# increment game_id once two teams are selected
				if team_count % 2 == 0:
					game_id += 1
					odds_count = 0
					bookie_count = 0
			
			# games that haven't started will not have points			
			colnames[3].append(0)

			# append wager %
			#try:
			if elm[-1] == "%":
				colnames[4].append(elm)
			#	elif (data[idx] == '-' and data[idx-1] == '-'):
			#		colnames[4].append('-')
			#	elif (data[idx] == '-' and data[idx-2] in team_names):
			#		colnames[4].append('-')
			#except:
			#	colnames[4].append('-')

			# start appending odds
			if elm[0] == '+' or elm[0] == '-':
				colnames[5+bookie_count].append(elm)
				odds_count += 1
		
				if odds_count % 2 == 0 and bookie_count < 4:
					bookie_count +=1

				
		return colnames

	# populate df after first arrow click
	def populate_after_click(data, colnames,team_names):
		odds_count = 0
		bookie_count = 0
		team_count = 0

		for idx, elm in enumerate(data):

			# if element is a valid team name
			if elm in team_names:
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

		if 'nba-basketball' in url:
			team_names = teams_nba
		else:
			team_names = teams_ncaam

		
		#click_arrow = "/html/body/div/div/div/div/section/div/div[3]/div[2]/div[3]/div[3]/div[1]/div/div"
		click_arrow = '/html/body/div/div/div/div/section/div/div[3]/div[2]/div[3]/div[2]/div/div/div'
		click_arrow2 = '/html/body/div/div/div/div/section/div/div[3]/div[2]/div[3]/div[2]/div/div/div[2]'
		
		click_morning = '/html/body/div/div/div/div/section/div/div[3]/div[2]/div[3]/div[3]/div/div/div'
		click_morning2 = '/html/body/div/div/div/div/section/div/div[3]/div[2]/div[3]/div[2]/div/div/div[2]'

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
		if 'nba-basketball' in url:
			begin_index = begin_index_nba
		else:
			begin_index = begin_index_ncaam

		colnames = [GameID, Date, Teams, Points, Wagers, Opener, Pinnacle, Fivedimes, Bookmaker, BetOnline]
		data = table.text.split("\n")[begin_index:]
		pop = populate(data,colnames,team_names)

		# Create dataframe
		dfout = pd.DataFrame(pop,
							index= ['GameID', 'Date', 'Teams', 'Points', 'Wagers',
									'Opener', 'Pinnacle', 'Fivedimes', 'Bookmaker', 'BetOnline']).T

		# ------------------------------------
		# redo process for next set of bookies
		try:
			browser.find_element_by_xpath(click_morning).click();
		except:
			try: 
				browser.find_element_by_xpath(click_arrow).click();
			except:
				browser.find_element_by_xpath(click_arrow_after_start).click();

		time.sleep(1)
		table_after = browser.find_element_by_id(tables[0])

		# grab data and populate
		colnames2 = [Opener2, Bovada, Heritage, Intertops, Youwager]
		data_after = table_after.text.split("\n")
		pop_after = populate_after_click(data_after,colnames2,team_names)

		# create dataframe
		dfout_after = pd.DataFrame(pop_after,
						index= ['Opener2','Bovada', 'Heritage', 'Intertops', 'Youwager']).T

		# concatenate to form master df
		dfout = pd.concat([dfout, dfout_after], axis=1)
		# ------------------------------------


		# ------------------------------------
		# redo process for next set of bookies
		try:
			browser.find_element_by_xpath(click_morning2).click();
		except:
			try:
				browser.find_element_by_xpath(click_arrow2).click();
			except:
				browser.find_element_by_xpath(click_arrow_after_start2).click();

		time.sleep(1)
		table_after2 = browser.find_element_by_id(tables[0])

		colnames3 = [Opener3, Justbet, Sportsbetting, Gtbets, Sportbet]
		data_after2 = table_after2.text.split("\n")
		pop_after2 = populate_after_click(data_after2,colnames3,team_names)

		dfout_after2 = pd.DataFrame(pop_after2,
						index= ['Opener3','Justbet', 'Sportsbetting', 'Gtbets', 'Sportbet']).T

		dfout = pd.concat([dfout, dfout_after2], axis=1)
		# ------------------------------------


		# ------------------------------------
		# redo process for next set of bookies
		try:
			browser.find_element_by_xpath(click_morning2).click();
		except:
			try:
				browser.find_element_by_xpath(click_arrow2).click();
			except:
				browser.find_element_by_xpath(click_arrow_after_start2).click();

		time.sleep(1)
		table_after3 = browser.find_element_by_id(tables[0])

		colnames4 = [Opener4, Nitrogensports, Betphoenix, Betmania, Skybook]
		data_after3 = table_after3.text.split("\n")
		pop_after3 = populate_after_click(data_after3,colnames4,team_names)

		dfout_after3 = pd.DataFrame(pop_after3,
						index= ['Opener4','Nitrogensports', 'Betphoenix', 'Betmania', 'Skybook']).T

		dfout = pd.concat([dfout, dfout_after3], axis=1)
		# ------------------------------------


		# ------------------------------------
		# redo process for next set of bookies
		try:
			browser.find_element_by_xpath(click_morning2).click();
		except:
			try:
				browser.find_element_by_xpath(click_arrow2).click();
			except:
				browser.find_element_by_xpath(click_arrow_after_start2).click();

		time.sleep(1)
		table_after4 = browser.find_element_by_id(tables[0])

		colnames5 = [Opener5, Mybookie, Abcislands, Jazz, Sportsinteraction]
		data_after4 = table_after4.text.split("\n")
		pop_after4 = populate_after_click(data_after4,colnames5,team_names)

		dfout_after4 = pd.DataFrame(pop_after4,
						index= ['Opener5','Mybookie', 'Abcislands', 'Jazz', 'Sportsinteraction']).T

		dfout = pd.concat([dfout, dfout_after4], axis=1)
		# ------------------------------------


		# ------------------------------------
		# redo process for next set of bookies
		try:
			browser.find_element_by_xpath(click_morning2).click();
		except:
			try:
				browser.find_element_by_xpath(click_arrow2).click();
			except:
				browser.find_element_by_xpath(click_arrow_after_start2).click();

		time.sleep(1)
		table_after5 = browser.find_element_by_id(tables[0])

		colnames6 = [Opener6, Betcris, Sbr, Wagerweb, Thegreek]
		data_after5 = table_after5.text.split("\n")
		pop_after5 = populate_after_click(data_after5,colnames6,team_names)

		dfout_after5 = pd.DataFrame(pop_after5,
						index= ['Opener6','Betcris', 'Sbr', 'Wagerweb', 'Thegreek']).T

		dfout = pd.concat([dfout, dfout_after5], axis=1)
		# ------------------------------------

		# ------------------------------------
		# redo process for next set of bookies
		try:
			browser.find_element_by_xpath(click_morning2).click();
		except:
			try:
				browser.find_element_by_xpath(click_arrow2).click();
			except:
				browser.find_element_by_xpath(click_arrow_after_start2).click();

		time.sleep(1)
		table_after6 = browser.find_element_by_id(tables[0])

		colnames7 = [Opener7, Bodog, Matchbook, Looselines, Bet365]
		data_after6 = table_after6.text.split("\n")
		pop_after6 = populate_after_click(data_after6,colnames7,team_names)

		dfout_after6 = pd.DataFrame(pop_after6,
						index= ['Opener7','Bodog', 'Matchbook', 'Looselines', 'Bet365']).T

		dfout = pd.concat([dfout, dfout_after6], axis=1)
		# ------------------------------------

		df_final = df_final.append(dfout)
		browser.quit()
		return df_final

	# --------------------- DEFINE FUNCTIONS END ---------------------

	# --------------------- CALL SCRAPE FUNCTION AND MANIPULATE DATA --------------------- 
	# set up chrome driver and open browser
	browser = webdriver.Chrome(executable_path=path_to_chromedriver)
	
	#for url in urls:
	data = scrape_one(url)

	# CLEAN DATA
	# make sure each row has a date, teams, and points
	include = data['Date'].notnull() & data['Teams'].notnull() & data['Points'].notnull()
	
	# filter data to remove issues
	data = data[include]

	# fill NaN values with dash so they are not counted in odds calculation
	data = data.fillna(value='-')

	# remove unnamed column
	data = data.drop(['Opener2','Opener3', 'Opener4','Opener5','Opener6','Opener7'],axis=1)
	
	
	# calculate average odds of all bookies
	avg_odds_l = []
	avg_prob_l = []
	max_odds_l = []
	max_prob_l = []
	min_odds_l = []
	min_prob_l = []

	for i in range(len(data)):
	
		# sort odds in each row
		sl = np.sort([int(x) for x in data.iloc[i,6:34].values if x != '-'])[::-1]

		# remove top 2 and bottom 2 values (avoid outliers and data issues)
		sl_sub = sl[num_outliers:len(sl)-num_outliers]

		# calculate average odds of 24 remaining bookies
		# only append new odds/probs if x bookies give odds
		if len(sl_sub) > min_number_odds:
			avg_odds = np.mean(sl_sub)
			max_odds = np.max(sl_sub)
			#min_odds = np.min(sl_sub)

			#if max(sl_sub) - min(sl_sub) < 260:
			probs_tmp = []
			for odds in sl_sub:
			    # calculate implied probabilities using odds
			    if int(odds) > 0:
			        probs_tmp.append(1/(1 + int(odds)/100))
			    else:
			        probs_tmp.append((-int(odds)/100)/(1-int(odds)/100))

			# calculate max prob
			if int(max_odds) > 0:
			    max_prob_l.append(1/(1 + int(max_odds)/100))
			else:
			    max_prob_l.append((-int(max_odds)/100)/(1-int(max_odds)/100))

			# calculate min prob
			#if int(min_odds) > 0:
			#    min_prob_l.append(1/(1 + int(min_odds)/100))
			#else:
			#    min_prob_l.append((-int(min_odds)/100)/(1-int(min_odds)/100))

			avg_odds_l.append(avg_odds)
			avg_prob_l.append(np.mean(probs_tmp))
			max_odds_l.append(max_odds)
			#min_odds_l.append(min_odds)

		else:
			avg_odds_l.append(0)
			avg_prob_l.append(0)
			max_odds_l.append(0)
			max_prob_l.append(0)
			#min_odds_l.append(0)
			#min_prob_l.append(0)
	
	# append computed odds and probabilities to dataframe
	data['Consensus_Odds'] = avg_odds_l
	data['Consensus_Probs'] = avg_prob_l

	# calculate expectations and signals
	data['Consensus_Minus_Alpha'] = data['Consensus_Probs'] - alpha

	data['Max_Odds'] = max_odds_l
	data['Max_Implied_Prob'] = max_prob_l
	#data['Min_Odds'] = min_odds_l
	#data['Min_Implied_Prob'] = min_prob_l

	filter_signal_long = data['Max_Implied_Prob'] < data['Consensus_Minus_Alpha']
	data['Signal_Long'] = [int(x) for x in filter_signal_long]

	filter_signal_short = data['Max_Implied_Prob'] > data['Consensus_Minus_Alpha']
	data['Signal_Short'] = [int(x) for x in filter_signal_short]
	
	# add payout for each bet if successful
	#payout_long ='
	data['Payout_if_Win_Long'] =  [-100/x if int(x) < 0 else x/100 for x in data['Max_Odds']]
	data['Payout_if_Lose_Long'] = [-1 for x in data['Max_Odds']]

	#payout_short = [100/x if int(x) < 0 else -x/100 for x in data['Max_Odds']]
	data['Payout_if_Win_Short'] = [1 for x in data['Max_Odds']]
	data['Payout_if_Lose_Short'] = [100/x if int(x) < 0 else -x/100 for x in data['Max_Odds']]

	# add expected value of each bet - this will be numerator of kelly
	expectation_long = np.array(data['Consensus_Minus_Alpha']) * np.array(data['Payout_if_Win_Long']) +\
					(np.array(1) - np.array(data['Consensus_Minus_Alpha'])) * np.array(data['Payout_if_Lose_Long'])
	data['Expected_Value_Long'] = expectation_long

	expectation_short = (np.array(1) - np.array(data['Consensus_Minus_Alpha'])) * np.array(data['Payout_if_Win_Short']) +\
					np.array(data['Consensus_Minus_Alpha']) * np.array(data['Payout_if_Lose_Short'])
	data['Expected_Value_Short'] = expectation_short

	# breakeven odds
	breakeven_payout = (np.array(1) - np.array(data['Consensus_Minus_Alpha'])) / np.array(data['Consensus_Minus_Alpha'])
	breakeven_payout_net_comm = [1.02*x for x in breakeven_payout]
	data['Breakeven_Odds_Gross'] = [-100/x if int(x) < 1 else 100*x for x in breakeven_payout]
	data['Breakeven_Odds_Net_Comm'] = [-100/x if int(x) < 1 else 100*x for x in breakeven_payout_net_comm]

	# export cleaned data
	now = datetime.now()
	if 'nba-basketball' in url:
		data.to_csv('./Data_Files/'+ td + '/NBA/' + 'nba_' + str(now.hour) + '_' + str(now.minute) + '.csv', sep=',')
	else:
		data.to_csv('./Data_Files/'+ td + '/NCAAM/' + 'ncaam_' + str(now.hour) + '_' + str(now.minute) + '.csv', sep=',')
	
	# IF WE PUT ON A POSITION
	# subset where signal = 1 so we put on a position
	#bets_subset = data.iloc[np.where((data['Signal_Long'] == 1) or (data['Signal_Short'] == 1))[0],:]
	bets_subset = data.iloc[np.where(data['Signal_Long'] == 1)[0],:]

	# calculate kelly bet sizes
	kelly_long = np.array(bets_subset['Expected_Value_Long']) / np.array(bets_subset['Payout_if_Win_Long'])
	bets_subset.loc[:,'Kelly_Long'] = kelly_long

	kelly_short = np.array(bets_subset['Expected_Value_Short']) / np.array(bets_subset['Payout_if_Win_Short'])
	bets_subset.loc[:,'Kelly_Short'] = kelly_short
	
	#kelly_bet_size_long = np.array(bets_subset['Kelly_Long']) * 100 
	#kelly_bet_winnings_long = kelly_bet_size * np.array(bets_subset['Payout_if_Win_Long'])

	if len(bets_subset) > 0:
		if 'nba-basketball' in url:
			print(str(now.hour) + '_' + str(now.minute) + ': NBA - ENTER POSITION')
			bets_subset.to_csv('./Data_Files/' + td + '/Positive_Signals/' + 'nba_' + str(now.hour) + '_' + str(now.minute) + '.csv', sep=',')
		else:
			print(str(now.hour) + '_' + str(now.minute) + ': NCAAM - ENTER POSITION')
			bets_subset.to_csv('./Data_Files/' + td + '/Positive_Signals/' + 'ncaam_' + str(now.hour) + '_' + str(now.minute) + '.csv', sep=',')
	else:
		if 'nba-basketball' in url:
			print(str(now.hour) + '_' + str(now.minute) + ': NBA - no signal')
		else:
			print(str(now.hour) + '_' + str(now.minute) + ': NCAAM - no signal')

		

			## ^^^^^^^^^^^^^^^ better way to do this ^^^^^^^^^^^^^^^^^^

	
def check_time(now):
	if now.hour > start_scrape and now.hour < end_scrape: return 1
	return 0

if __name__ == '__main__':

	# schedule script runs
	schedule.every(20).seconds.do(scrape, url_nba, alpha, min_number_odds, num_outliers)
	#schedule.every(20).seconds.do(scrape, url_ncaam, alpha, min_number_odds, num_outliers)

	# check time to see if after noon
	now = datetime.now()
	cur_time = check_time(now)
	
	# keep running until 
	while cur_time == 1:
		schedule.run_pending()
		time.sleep(20)

		now = datetime.now()
		cur_time = check_time(now)








