import pandas as pd
import time
from pytrends.request import TrendReq

# Initialize the API, set the keyword, specify the region, and create the prefecture list
pytrends = TrendReq()
kw = ['鬱']
region = 'JP'

prefectures = {'Hokkaido': '01', 'Aomori': '02', 'Iwate': '03', 'Miyagi': '04', 'Akita': '05', 'Yamagata': '06',
'Fukushima': '07', 'Ibaraki': '08', 'Tochigi': '09', 'Gunma': '10', 'Saitama': '11', 'Chiba': '12', 'Tokyo': '13',
'Kanagawa': '14', 'Niigata': '15', 'Toyama': '16', 'Ishikawa': '17', 'Fukui': '18', 'Yamanashi': '19', 'Nagano': '20',
'Gifu': '21', 'Shizuoka': '22', 'Aichi': '23', 'Mie': '24', 'Shiga': '25', 'Kyoto': '26', 'Osaka': '27', 'Hyogo': '28',
'Nara': '29', 'Wakayama': '30', 'Tottori': '31', 'Shimane': '32', 'Okayama': '33', 'Hiroshima': '34', 'Yamaguchi': '35',
'Tokushima': '36', 'Kagawa': '37', 'Ehime': '38', 'Kochi': '39', 'Fukuoka': '40', 'Saga': '41', 'Nagasaki': '42', 
'Kumamoto': '43', 'Oita': '44', 'Miyazaki': '45', 'Kagoshima': '46', 'Okinawa': '47'}

# Google Trends weekly data is aggregated from Sunday to Saturday i.e. the first
# observation in the weekly_2019_2020 dataframe is for 2018-12-30 and corresponds
# to the range Sunday 2018-12-30 to Saturday 2019-01-05. I therefore set the date
# range so that the first day is a Sunday and the last day is a Saturday so that the
# daily and weekly data can be properly compared

################################################################
### Create the for loop to collect the prefecture level data ###
################################################################

# First create an empty dataframe to append the prefecture level data to
trends_data_prefecture = pd.DataFrame()

for x in prefectures:
    try:
        ################################
        ### Retreive the Weekly Data ###
        ################################

        # Build the payload for the weekly data between January 2016 and December 2019
        pytrends.build_payload(kw, timeframe='2015-12-27 2020-01-04', geo = str(region) + '-' + str(prefectures[x]))
        # Request the interest over time data for the time interval specified above
        weekly_2016_2019 = pytrends.interest_over_time()

        # Drop the unneeded column and rename the trend data column
        weekly_2016_2019 = weekly_2016_2019.drop('isPartial', 1)
        weekly_2016_2019.columns.values[0] = "raw_weekly"

        ##################################################################
        ### Retreive the Daily Data and build the daily data dataframe ###
        ##################################################################

        # Build the payload for the daily data between January 2016 and June 2016
        pytrends.build_payload(kw, timeframe='2015-12-27 2016-07-02', geo = str(region) + '-' + str(prefectures[x]))
        # Request the interest over time data for the time interval specified above
        daily_jan_jun_2016 = pytrends.interest_over_time()

        # Build the payload for the daily data between July 2016 and December 2016
        pytrends.build_payload(kw, timeframe='2016-07-03 2016-12-31', geo = str(region) + '-' + str(prefectures[x]))
        # Request the interest over time data for the time interval specified above
        daily_jul_dec_2016 = pytrends.interest_over_time()

        # Build the payload for the daily data between January 2017 and June 2017
        pytrends.build_payload(kw, timeframe='2017-01-01 2017-07-01', geo = str(region) + '-' + str(prefectures[x]))
        # Request the interest over time data for the time interval specified above
        daily_jan_jun_2017 = pytrends.interest_over_time()

        # Build the payload for the daily data between July 2017 and December 2017
        pytrends.build_payload(kw, timeframe='2017-07-02 2017-12-30', geo = str(region) + '-' + str(prefectures[x]))
        # Request the interest over time data for the time interval specified above
        daily_jul_dec_2017 = pytrends.interest_over_time()

        # Build the payload for the daily data between January 2018 and June 2018
        pytrends.build_payload(kw, timeframe='2017-12-31 2018-06-30', geo = str(region) + '-' + str(prefectures[x]))
        # Request the interest over time data for the time interval specified above
        daily_jan_jun_2018 = pytrends.interest_over_time()

        # Build the payload for the daily data between July 2018 and December 2018
        pytrends.build_payload(kw, timeframe='2018-07-01 2018-12-29', geo = str(region) + '-' + str(prefectures[x]))
        # Request the interest over time data for the time interval specified above
        daily_jul_dec_2018 = pytrends.interest_over_time()

        # Build the payload for the daily data between January 2019 and June 2019
        pytrends.build_payload(kw, timeframe='2018-12-30 2019-07-06', geo = str(region) + '-' + str(prefectures[x]))
        # Request the interest over time data for the time interval specified above
        daily_jan_jun_2019 = pytrends.interest_over_time()

        # Build the payload for the daily data between July 2019 and December 2019
        pytrends.build_payload(kw, timeframe='2019-07-07 2020-01-04', geo = str(region) + '-' + str(prefectures[x]))
        # Request the interest over time data for the time interval specified above
        daily_jul_dec_2019 = pytrends.interest_over_time()

        # Build the dataframe of daily data 
        daily_2016_2019 = daily_jan_jun_2016.append([daily_jul_dec_2016, daily_jan_jun_2017, daily_jul_dec_2017, daily_jan_jun_2018, daily_jul_dec_2018, daily_jan_jun_2019, daily_jul_dec_2019])

        # Drop the unneeded column and rename the trend data column
        daily_2016_2019 = daily_2016_2019.drop('isPartial', 1)
        daily_2016_2019.columns.values[0] = "raw_daily"

        # Merge the weekly and daily trends data
        trends_data = pd.merge(daily_2016_2019, weekly_2016_2019, how = 'left', on = 'date')

        # Forward fill the raw_weekly data
        trends_data['raw_weekly'] = trends_data['raw_weekly'].fillna(method = 'ffill')

        # Convert the raw_daily data to a float to match the raw weekly
        trends_data['raw_daily'] = trends_data['raw_daily'].astype(float)

        # Turn the date index into a column
        trends_data = trends_data.reset_index()

        # Remove duplicate rows
        trends_data = trends_data.drop_duplicates(subset = ['date'], keep = 'last')

        # Assign the prefecture name to the column 'prefecture'
        trends_data['prefecture'] = x

        # Append the trends_data dataframe to the trends_data_prefecture dataframe
        trends_data_prefecture = trends_data_prefecture.append(trends_data)

        # Print to the console that the prefecture has been run
        print(str(x)+" is complete")

        # Pause for 10 seconds to stop Google from blocking requests
        time.sleep(10)
    except:
        print(str(x)+" failed!")
        pass

# Export the data
trends_data_prefecture.to_csv('./prefecture_level/depression1_2016_2019/depression1_trends_data.csv', index = False)