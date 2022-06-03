import pandas as pd
from pytrends.request import TrendReq

# Initialize the API, set the keyword, and specify the region
pytrends = TrendReq()
kw = ['アレジオン+アレグラ+クラリチン']
region = 'JP'

# Google Trends weekly data is aggregated from Sunday to Saturday i.e. the first
# observation in the weekly_2019_2020 dataframe is for 2018-12-30 and corresponds
# to the range Sunday 2018-12-30 to Saturday 2019-01-05. I therefore set the date
# range so that the first day is a Sunday and the last day is a Saturday so that the
# daily and weekly data can be properly compared

################################
### Retreive the Weekly Data ###
################################

# Build the payload for the weekly data between January 2018 and December 2021
pytrends.build_payload(kw, timeframe='2017-12-31 2022-01-01', geo=str(region))
# Request the interest over time data for the time interval specified above
weekly_2018_2021 = pytrends.interest_over_time()

# Drop the unneeded column and rename the trend data column
weekly_2018_2021 = weekly_2018_2021.drop('isPartial', 1)
weekly_2018_2021.columns.values[0] = "raw_weekly"

##################################################################
### Retreive the Daily Data and build the daily data dataframe ###
##################################################################

# Build the payload for the daily data between January 2018 and June 2018
pytrends.build_payload(kw, timeframe='2017-12-31 2018-06-30', geo=str(region))
# Request the interest over time data for the time interval specified above
daily_jan_jun_2018 = pytrends.interest_over_time()

# Build the payload for the daily data between July 2018 and December 2018
pytrends.build_payload(kw, timeframe='2018-07-01 2018-12-29', geo=str(region))
# Request the interest over time data for the time interval specified above
daily_jul_dec_2018 = pytrends.interest_over_time()

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

# Build the payload for the daily data between January 2021 and June 2021
pytrends.build_payload(kw, timeframe='2021-01-03 2021-07-03', geo=str(region))
# Request the interest over time data for the time interval specified above
daily_jan_jun_2021 = pytrends.interest_over_time()

# Build the payload for the daily data between January 2021 and June 2021
pytrends.build_payload(kw, timeframe='2021-07-04 2022-01-01', geo=str(region))
# Request the interest over time data for the time interval specified above
daily_jul_dec_2021 = pytrends.interest_over_time()

# Build the dataframe of daily data 
daily_2018_2021 = daily_jan_jun_2018.append([daily_jul_dec_2018, daily_jan_jun_2019, daily_jul_dec_2019, daily_jan_jun_2020, daily_jul_dec_2020, daily_jan_jun_2021, daily_jul_dec_2021])

# Drop the unneeded column and rename the trend data column
daily_2018_2021 = daily_2018_2021.drop('isPartial', 1)
daily_2018_2021.columns.values[0] = "raw_daily"

# Merge the weekly and daily trends data
trends_data = pd.merge(daily_2018_2021, weekly_2018_2021, how = 'left', left_index = True, right_index = True)

# Forward fill the raw_weekly data
trends_data['raw_weekly'] = trends_data['raw_weekly'].fillna(method = 'ffill')

# Convert the raw_daily data to a float to match the raw weekly
trends_data['raw_daily'] = trends_data['raw_daily'].astype(float)

# Turn the date index into a column
trends_data = trends_data.reset_index()

# Output to csv
trends_data.to_csv('./country_level/medication/medication_trends_data.csv', index = False)