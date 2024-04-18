from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

'''********************************************************************************************
 Author: Justin Pearson

 This file has 6 total functions that implement datascraping of NFL player data
 from pro-football-reference.com.
 functions in this file:
 - get_stats(year)
 - def get_pass_stats(year)
 - def get_rush_stats(year)
 - def get_rec_stats(year)
 - def get_scrim_stats(year)
 - def get_fantasy_stats(year)
********************************************************************************************'''

# implements all the stat functions and combines the data from a given year
def get_stats(year):
	fantasy_stats = get_fantasy_stats(year)
	pass_stats = get_pass_stats(year)
	rush_stats = get_rush_stats(year)
	rec_stats = get_rec_stats(year)
	scrim_stats = get_scrim_stats(year)

	# combine all data
	stats = pd.merge(fantasy_stats, pass_stats, on='Player', how='outer')
	stats = pd.merge(stats, rush_stats, on='Player', how='outer')
	stats = pd.merge(stats, rec_stats, on='Player', how='outer')
	stats = pd.merge(stats, scrim_stats, on='Player', how='outer')

	# convert all cells that should be numbers from strings to numbers
	columns_to_convert = stats.columns[3:]  # Adjusted index to start from column 3

	# convert selected columns to numeric type
	for col in columns_to_convert:
		stats[col] = pd.to_numeric(stats[col], errors='coerce')

	# fix duplicated players
	stats.drop_duplicates('Player', inplace=True)

	# data manipulation
	stats = stats.replace(r'', 0, regex=True) # replace empty columns with zero
	stats = stats.fillna(0)
	stats = stats.replace(r'\+|\*', '', regex=True) # remove trailing + and * from names

	return stats

# gets the passing stats from a given year
def get_pass_stats(year):
	# create beautifulSoup object for passing
	url = "https://www.pro-football-reference.com/years/{}/passing.htm".format(year)
	html = urlopen(url)
	soup = BeautifulSoup(html, features="html.parser")

	# get header data
	headers = [th.getText() for th in soup.findAll('tr')[0].findAll('th')]
	headers = headers[1:]


	# get table row data
	rows = soup.findAll('tr', class_ = lambda table_rows: table_rows != "thead")
	player_stats = [[td.getText() for td in rows[i].findAll('td')]
					for i in range(len(rows))]
	player_stats = player_stats[2:]

	# create pandas DataFrame object
	pass_stats = pd.DataFrame(player_stats, columns = headers)

	# drop unnecessary columns
	pass_stats = pass_stats.drop(['QBrec', '1D', 'Lng', 'Succ%'], axis=1)
	pass_stats = pass_stats.drop(pass_stats.columns[-4:], axis=1) # drop last 4 columns
	pass_stats = pass_stats.drop(pass_stats.columns[-2], axis=1) # drop yards lost to sack column
	pass_stats = pass_stats.drop(pass_stats.columns[1:6], axis=1) # drop all repeating stats but name

	# adjust column headers to be more descriptive
	pass_stats = pass_stats.rename(columns={'Att' : 'PassAtt'   , 
											'Yds' : 'PassYds'   , 
											'TD'  : 'PassTD'    ,
											'Y/A' : 'PassYds/A' ,
											'Y/G' : 'PassYds/G'  })

	return pass_stats

# gets the rushing stats from a given year
def get_rush_stats(year):
	# create beautifulSoup object for rushing
	url = "https://www.pro-football-reference.com/years/{}/rushing.htm".format(year)
	html = urlopen(url)
	soup = BeautifulSoup(html, features="html.parser")

	# get header data
	headers = [th.getText() for th in soup.findAll('tr')[1].findAll('th')]
	headers = headers[1:]
	# adjust column headers to be more descriptive

	# get table row data
	rows = soup.findAll('tr', class_ = lambda table_rows: table_rows != "thead")
	player_stats = [[td.getText() for td in rows[i].findAll('td')]
					for i in range(len(rows))]
	player_stats = player_stats[2:]

	# create pandas DataFrame object
	rush_stats = pd.DataFrame(player_stats, columns = headers)

	# drop unnecessary columns
	rush_stats = rush_stats.drop(['1D', 'Lng', 'Succ%'], axis=1)
	rush_stats = rush_stats.drop(rush_stats.columns[1:6], axis=1) # drop all repeating stats but name
	rush_stats = rush_stats.drop(rush_stats.columns[-1], axis=1) #remove fumbles

	# adjust column headers to be more descriptive
	rush_stats = rush_stats.rename(columns={'Att' : 'RushAtt'   , 
											'Yds' : 'RushYds'   , 
											'TD'  : 'RushTD'    ,
											'Y/A' : 'RushYds/A' ,
											'Y/G' : 'RushYds/G'  })

	return rush_stats

# gets the receiving stats from a given year
def get_rec_stats(year):
	# create beautifulSoup object for receiving
	url = "https://www.pro-football-reference.com/years/{}/receiving.htm".format(year)
	html = urlopen(url)
	soup = BeautifulSoup(html, features="html.parser")

	# get header data
	headers = [th.getText() for th in soup.findAll('tr')[0].findAll('th')]
	headers = headers[1:]
	# adjust column headers to be more descriptive

	# get table row data
	rows = soup.findAll('tr', class_ = lambda table_rows: table_rows != "thead")
	player_stats = [[td.getText() for td in rows[i].findAll('td')]
					for i in range(len(rows))]
	player_stats = player_stats[2:]

	# create pandas DataFrame object
	rec_stats = pd.DataFrame(player_stats, columns = headers)

	# drop unnecessary columns
	rec_stats = rec_stats.drop(['1D', 'Lng', 'Succ%'], axis=1)
	rec_stats = rec_stats.drop(rec_stats.columns[1:6], axis=1) # drop all repeating stats but name
	rec_stats = rec_stats.drop(rec_stats.columns[-1], axis=1) #remove fumbles

	# adjust column headers to be more descriptive
	rec_stats = rec_stats.rename(columns={'Yds' : 'RecYds'   , 
											'TD'  : 'RecTD'    ,
											'Y/G' : 'RecYds/G'  })

	return rec_stats

# gets the scrimmage stats from a given year
def get_scrim_stats(year):
	# create beautifulSoup object for total scrimmage yards
	url = "https://www.pro-football-reference.com/years/{}/scrimmage.htm".format(year)
	html = urlopen(url)
	soup = BeautifulSoup(html, features="html.parser")

	# get header data
	headers = [th.getText() for th in soup.findAll('tr')[1].findAll('th')]
	headers = headers[1:]
	# adjust column headers to be more descriptive

	# get table row data
	rows = soup.findAll('tr', class_ = lambda table_rows: table_rows != "thead")
	player_stats = [[td.getText() for td in rows[i].findAll('td')]
					for i in range(len(rows))]
	player_stats = player_stats[2:]

	# create pandas DataFrame object
	scrim_stats = pd.DataFrame(player_stats, columns = headers)

	# drop unnecessary columns
	scrim_stats = scrim_stats.drop(['Succ%'], axis=1)
	scrim_stats = scrim_stats.drop(scrim_stats.columns[1:-6], axis=1) #take first and last few columns
	scrim_stats = scrim_stats.drop(scrim_stats.columns[-1], axis=1) #remove fumbles

	return scrim_stats

# gets the fantasy stats from a given year
def get_fantasy_stats(year):
	# create beautifulSoup object for fantasy stats
	url = "https://www.pro-football-reference.com/years/{}/fantasy.htm".format(year)
	html = urlopen(url)
	soup = BeautifulSoup(html, features="html.parser")

	# get header data
	headers = [th.getText() for th in soup.findAll('tr')[1].findAll('th')]
	headers = headers[1:]
	# adjust column headers to be more descriptive

	# get table row data
	rows = soup.findAll('tr', class_ = lambda table_rows: table_rows != "thead")
	player_stats = [[td.getText() for td in rows[i].findAll('td')]
					for i in range(len(rows))]
	player_stats = player_stats[2:]

	# create pandas DataFrame object
	fantasy_stats = pd.DataFrame(player_stats, columns = headers)

	# drop unnecessary columns
	fantasy_stats = fantasy_stats.drop(fantasy_stats.columns[4:-7], axis=1) #take first and last few columns

	# adjust column headers to be more descriptive
	fantasy_stats = fantasy_stats.rename(columns={'FantPos' : 'Pos'})

	return fantasy_stats