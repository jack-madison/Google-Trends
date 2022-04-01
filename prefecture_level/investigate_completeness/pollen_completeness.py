import pandas as pd
import time
from pytrends.request import TrendReq

# Initialize the API, set the keyword, specify the region, and create the prefecture list
pytrends = TrendReq()
kw = ['花粉+花粉症+スギ花粉']
region = 'JP'

prefectures = {'Hokkaido': '01', 'Aomori': '02', 'Iwate': '03', 'Miyagi': '04', 'Akita': '05', 'Yamagata': '06',
'Fukushima': '07', 'Ibaraki': '08', 'Tochigi': '09', 'Gunma': '10', 'Saitama': '11', 'Chiba': '12', 'Tokyo': '13',
'Kanagawa': '14', 'Niigata': '15', 'Toyama': '16', 'Ishikawa': '17', 'Fukui': '18', 'Yamanashi': '19', 'Nagano': '20',
'Gifu': '21', 'Shizuoka': '22', 'Aichi': '23', 'Mie': '24', 'Shiga': '25', 'Kyoto': '26', 'Osaka': '27', 'Hyogo': '28',
'Nara': '29', 'Wakayama': '30', 'Tottori': '31', 'Shimane': '32', 'Okayama': '33', 'Hiroshima': '34', 'Yamaguchi': '35',
'Tokushima': '36', 'Kagawa': '37', 'Ehime': '38', 'Kochi': '39', 'Fukuoka': '40', 'Saga': '41', 'Nagasaki': '42', 
'Kumamoto': '43', 'Oita': '44', 'Miyazaki': '45', 'Kagoshima': '46', 'Okinawa': '47'}

# Import dates from CSV
date_cols = ["date"]
dates = pd.read_csv('./prefecture_level/investigate_completeness/dates.csv', parse_dates=date_cols)

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
    ##################################################################
    ### Retreive the Daily Data and build the daily data dataframe ###
    ##################################################################

    # Build the payload for the daily data between January 2004 and June 2004
    pytrends.build_payload(kw, timeframe='2004-01-01 2004-07-03', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2004 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2004 and December 2004
    pytrends.build_payload(kw, timeframe='2004-07-04 2005-01-01', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2004 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2005 and June 2005
    pytrends.build_payload(kw, timeframe='2005-01-02 2005-07-02', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2005 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2005 and December 2005
    pytrends.build_payload(kw, timeframe='2005-07-03 2005-12-31', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2005 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2006 and June 2006
    pytrends.build_payload(kw, timeframe='2006-01-01 2006-07-01', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2006 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2006 and December 2006
    pytrends.build_payload(kw, timeframe='2006-07-02 2007-01-06', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2006 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2007 and June 2007
    pytrends.build_payload(kw, timeframe='2007-01-07 2007-06-30', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2007 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2007 and December 2007
    pytrends.build_payload(kw, timeframe='2007-07-01 2008-01-05', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2007 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2008 and June 2008
    pytrends.build_payload(kw, timeframe='2008-01-06 2008-07-05', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2008 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2008 and December 2008
    pytrends.build_payload(kw, timeframe='2008-07-06 2009-01-03', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2008 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2009 and June 2009
    pytrends.build_payload(kw, timeframe='2009-01-04 2009-07-04', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2009 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2009 and December 2009
    pytrends.build_payload(kw, timeframe='2009-07-05 2010-01-02', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2009 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2010 and June 2010
    pytrends.build_payload(kw, timeframe='2010-01-03 2010-07-03', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2010 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2010 and December 2010
    pytrends.build_payload(kw, timeframe='2010-07-04 2011-01-01', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2010 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2011 and June 2011
    pytrends.build_payload(kw, timeframe='2011-01-02 2011-07-02', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2011 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2011 and December 2011
    pytrends.build_payload(kw, timeframe='2011-07-03 2011-12-31', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2011 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2012 and June 2012
    pytrends.build_payload(kw, timeframe='2012-01-01 2012-06-30', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2012 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2012 and December 2012
    pytrends.build_payload(kw, timeframe='2012-07-01 2013-01-05', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2012 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2013 and June 2013
    pytrends.build_payload(kw, timeframe='2013-01-06 2013-07-06', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2013 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2013 and December 2013
    pytrends.build_payload(kw, timeframe='2013-07-07 2014-01-04', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2013 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2014 and June 2014
    pytrends.build_payload(kw, timeframe='2014-01-05 2014-07-05', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2014 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2014 and December 2014
    pytrends.build_payload(kw, timeframe='2014-07-06 2014-12-27', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2014 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2015 and June 2015
    pytrends.build_payload(kw, timeframe='2014-12-28 2015-07-04', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2015 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2015 and December 2015
    pytrends.build_payload(kw, timeframe='2015-07-05 2016-01-02', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2015 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2016 and June 2016
    pytrends.build_payload(kw, timeframe='2016-01-03 2017-07-02', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2016 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2016 and December 2016
    pytrends.build_payload(kw, timeframe='2017-07-03 2017-12-31', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2016 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2017 and June 2017
    pytrends.build_payload(kw, timeframe='2017-01-01 2017-07-01', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2017 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2017 and December 2017
    pytrends.build_payload(kw, timeframe='2017-07-02 2017-12-30', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2017 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2018 and June 2018
    pytrends.build_payload(kw, timeframe='2017-12-31 2018-06-30', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2018 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2018 and December 2018
    pytrends.build_payload(kw, timeframe='2018-07-01 2018-12-29', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2018 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2019 and June 2019
    pytrends.build_payload(kw, timeframe='2018-12-30 2019-07-06', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2019 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2019 and December 2019
    pytrends.build_payload(kw, timeframe='2019-07-07 2020-01-04', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2019 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2020 and June 2020
    pytrends.build_payload(kw, timeframe='2020-01-05 2020-07-04', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2020 = pytrends.interest_over_time()

    # Build the payload for the daily data between July 2020 and December 2020
    pytrends.build_payload(kw, timeframe='2020-07-05 2021-01-02', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2020 = pytrends.interest_over_time()

    # Pause for 5 seconds to stop Google from blocking requests
    time.sleep(5)

    # Build the payload for the daily data between January 2021 and June 2021
    pytrends.build_payload(kw, timeframe='2021-01-03 2021-07-03', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jan_jun_2021 = pytrends.interest_over_time()

    # Build the payload for the daily data between January 2021 and June 2021
    pytrends.build_payload(kw, timeframe='2021-07-04 2022-01-01', geo = str(region) + '-' + str(prefectures[x]))
    # Request the interest over time data for the time interval specified above
    daily_jul_dec_2021 = pytrends.interest_over_time()

    # Build the dataframe of daily data 
    daily_2004_2021 = daily_jan_jun_2004.append([
        daily_jul_dec_2004, daily_jan_jun_2005, daily_jul_dec_2005, daily_jan_jun_2006, daily_jul_dec_2006, daily_jan_jun_2007, 
        daily_jul_dec_2007, daily_jan_jun_2008, daily_jul_dec_2008, daily_jan_jun_2009, daily_jul_dec_2009, daily_jan_jun_2010, 
        daily_jul_dec_2010, daily_jan_jun_2011, daily_jul_dec_2011, daily_jan_jun_2012, daily_jul_dec_2012, daily_jan_jun_2013, 
        daily_jul_dec_2013, daily_jan_jun_2014, daily_jul_dec_2014, daily_jan_jun_2015, daily_jul_dec_2015, daily_jan_jun_2016, 
        daily_jul_dec_2016, daily_jan_jun_2017, daily_jul_dec_2017, daily_jan_jun_2018, daily_jul_dec_2018, daily_jan_jun_2019, 
        daily_jul_dec_2019, daily_jan_jun_2020, daily_jul_dec_2020, daily_jan_jun_2021, daily_jul_dec_2021])

    # Drop the unneeded column and rename the trend data column
    daily_2004_2021 = daily_2004_2021.drop('isPartial', 1)
    daily_2004_2021.columns.values[0] = "raw_daily"

    # Merge the weekly and daily trends data
    trends_data = daily_2004_2021

    # Convert the raw_daily data to a float to match the raw weekly
    trends_data['raw_daily'] = trends_data['raw_daily'].astype(float)

    # Turn the date index into a column
    trends_data = trends_data.reset_index()

    # Assign the prefecture name to the column 'prefecture'
    trends_data['prefecture'] = x

    # Merge the trends data with the dates df to eliinate any missing dates and fill nan with 0
    trends_data_prefecture = pd.merge(dates, trends_data, how = 'left', on = 'date')
    trends_data_prefecture['raw_daily'] = trends_data_prefecture['raw_daily'].fillna(0)

    # Append the trends_data dataframe to the trends_data_prefecture dataframe
    trends_data_prefecture = trends_data_prefecture.append(trends_data)

    # Print to the console that the prefecture has been run
    print(str(x)+" is complete")

    # Pause for 2 seconds to stop Google from blocking requests
    time.sleep(10)

# Export the data
trends_data_prefecture.to_csv('./prefecture_level/investigate_completeness/pollen_completeness.csv', index = False)