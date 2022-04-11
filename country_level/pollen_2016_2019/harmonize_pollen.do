/* Imort the raw data collected in python */
import delimited "C:\Users\jack-\OneDrive\Documents\GitHub\Google-Trends\country_level\pollen_2016_2019\pollen_trends_data.csv", delimiter(comma) varnames(1)

/* Extract the year, month, and day values from the date variable */
gen year=real(substr(date,1,4))
gen month=real(substr(date,6,2))
gen day=real(substr(date,9,2))

/* Drop the date variable and then generate a new date variable in STATA format */
drop date
gen date=mdy(month,day,year)
format date %tdCCYY.NN.DD
drop year month day

/* Create a week_of variable to use later to calculate the weekly average of the raw_daily data */
gen week_of = cond(dow(date) == 0, date, date - dow(date))
format week_of %tdCCYY.NN.DD

/* The following calculations follow from Brodeur et at. 2021 Section 2.2 */

/* Calculate the weekly average of the raw_daily data using the week_of variable */
bysort week_of: egen calc_weekly = mean(raw_daily)

/* Calculate the weights to be applied to the daily data */
gen weight = raw_weekly / calc_weekly

/* Apply the weights to the daily data */
gen weighted_daily = raw_daily * weight

/* Find the maximum of the weighted daily data */
egen max_weighted_daily = max(weighted_daily)

/* Scale the weighted daily data by the maximum of the weighted daily data*/
gen pollen_harmonized_daily = (weighted_daily / max_weighted_daily) * 100

/* Clean up the unnessesary variables and reorder the variables */
drop week_of calc_weekly weight weighted_daily max_weighted_daily
order date

/* Export the data */
export delimited "C:\Users\jack-\OneDrive\Documents\GitHub\Google-Trends\country_level\pollen_2016_2019\pollen_trends_data_harmonized.csv"