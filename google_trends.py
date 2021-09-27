from datetime import datetime, timedelta
from pytrends.request import TrendReq
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Initialize the API, set the keyword, and specify the region
pytrends = TrendReq()
kw = ['花粉']
region = 'JP'

# Google Trends weekly data is aggregated from Sunday to Saturday i.e. the first
# observation in the weekly_2019_2020 dataframe is for 2018-12-30 and corresponds
# to the range Sunday 2018-12-30 to Saturday 2019-01-05. I therefore set the date
# range so that the first day is a Sunday and the last day is a Saturday so that the
# daily and weekly data can be properly compared

################################
### Retreive the Weekly Data ###
################################

# Build the payload for the weekly data between January 2019 and December 2020
pytrends.build_payload(kw, timeframe='2018-12-30 2021-01-02', geo=str(region))
# Request the interest over time data for the time interval specified above
weekly_2019_2020 = pytrends.interest_over_time()

# Drop the unneeded column
weekly_2019_2020 = weekly_2019_2020.drop('isPartial', 1)

# Rename the Pollen column to calc_weekly
weekly_2019_2020.columns.values[0] = "raw_weekly"

##################################################################
### Retreive the Daily Data and build the daily data dataframe ###
##################################################################

# Build the payload for the daily data between January 2019 and June 2019
pytrends.build_payload(kw, timeframe='2018-12-30 2019-07-06', geo=str(region))
# Request the interest over time data for the time interval specified above
daily_jan_jun_2019 = pytrends.interest_over_time()

# Build the payload for the daily data between July 2019 and December 2019
pytrends.build_payload(kw, timeframe='2019-07-07 2020-01-04', geo=str(region))
# Request the interest over time data for the time interval specified above
daily_jul_dec_2019 = pytrends.interest_over_time()

# Build the payload for the daily data between January 2020 and June 2020
pytrends.build_payload(kw, timeframe='2020-01-05 2020-07-04', geo=str(region))
# Request the interest over time data for the time interval specified above
daily_jan_jun_2020 = pytrends.interest_over_time()

# Build the payload for the daily data between July 2020 and December 2020
pytrends.build_payload(kw, timeframe='2020-07-05 2021-01-02', geo=str(region))
# Request the interest over time data for the time interval specified above
daily_jul_dec_2020 = pytrends.interest_over_time()

# Build the dataframe of daily data 
daily_2019_2020 = daily_jan_jun_2019.append([daily_jul_dec_2019, daily_jan_jun_2020, daily_jul_dec_2020])

# Drop the unneeded column
daily_2019_2020 = daily_2019_2020.drop('isPartial', 1)

# Rename the Pollen column to raw_daily. This is the raw data from Google Trends
daily_2019_2020.columns.values[0] = "raw_daily"

#############################################################
### Calculate the weekly averages from the raw daily data ###
#############################################################

# Use the resample function to find the weekly averages
weekly_calc_jan_jun_2019 = daily_jan_jun_2019.resample('W-SAT').mean()
weekly_calc_jul_dec_2019 = daily_jul_dec_2019.resample('W-SAT').mean()
weekly_calc_jan_jun_2020 = daily_jan_jun_2020.resample('W-SAT').mean()
weekly_calc_jul_dec_2020 = daily_jul_dec_2020.resample('W-SAT').mean()

# Append the 4 dataframes together
weekly_calc_2019_2020 = weekly_calc_jan_jun_2019.append([weekly_calc_jul_dec_2019, weekly_calc_jan_jun_2020, weekly_calc_jul_dec_2020])

# Drop the unneeded column
weekly_calc_2019_2020 = weekly_calc_2019_2020.drop('isPartial', 1)

# Rename the Pollen column to calc_weekly
weekly_calc_2019_2020.columns.values[0] = "calc_weekly"

# Offset the averages by a week since the Google Trends data uses the beginning of
# the time interval as its index whereas the resample function uses the end of the
# time interval
weekly_calc_2019_2020.index = weekly_calc_2019_2020.index - pd.Timedelta(6, unit='d')

###################################################################
### Merge the raw daily, raw weekly, and calculated weekly data ###
###################################################################

trends_data = daily_2019_2020.merge(weekly_calc_2019_2020, how = 'left', left_index = True, right_index = True)
trends_data = trends_data.merge(weekly_2019_2020, how = 'left', left_index = True, right_index = True)

##################################################################
### Calculate the normalized weighted daily Google Trends Data ###
##################################################################

# Forward fill the weekly data
trends_data = trends_data.fillna(method = 'ffill')

# Calculate the weights
trends_data['weight'] = trends_data['raw_weekly'] / trends_data['calc_weekly']

# Drop any infinity weights and replace with zero
trends_data.replace([np.inf, -np.inf], np.nan, inplace=True)
trends_data = trends_data.fillna(0)

# Calculate the weighted daily values
trends_data['weighted_daily'] = trends_data['raw_daily'] * trends_data['weight']

# Find the maximum daily weighted value
weighted_daily_column = trends_data['weighted_daily']
max_weighted_daily = weighted_daily_column.max()

# Normalize the daily weighted values
trends_data['normalized_weighted_daily'] = (trends_data['weighted_daily'] / max_weighted_daily) * 100

#######################
### Export the data ###
#######################

trends_data.to_csv('./' + str(kw[0]) + '_trends_data.csv')

######################
### Graph the data ###
######################

x = trends_data.index
y = trends_data['normalized_weighted_daily'] 
z = trends_data['raw_weekly'] 
plt.plot(x,y)
plt.plot(x,z)
plt.show()