# Google-Trends

This repository contains python code used to collect daily Google Trends data on a specific keyword between 2019 and 2021. Since daily Google Trends data is not available for time periods longer than 9 months, I had to rescale the daily data using the weekly data in order to make the different years comparable. I use two different methods to achieve this goal. The first method is outlined in Brodeur et al. (2021) and the second is outlined in Tseng (2019).

## References

Brodeur, A., Clark, A. E., Fleche, S., & Powdthavee, N.(2021).Covid-19, lockdowns and well-being: Evidence from google trends. Journal of Public Economics, 193, 104346. doi: 10.1016/j.jpubeco.2020.104346

Tseng, Q. (2019, 11). Reconstruct google trends daily data for extended period. Retrieved from https://towardsdatascience.com/reconstruct-google-trends-daily-data-for-extended-period-75b6ca1d3420
