This project is used to predict the impact of weather conditions on accidents in New York state. A file containing accident data from counties of NY state from 2014-2016 is used for the analysis. While this has weather conditions, it does not have the specific weather parameters (in units) at the time of the accident.

The scripts here aim to extract information from Wunderground.com. Every row in the dataset opens a firefox window through selenium and obtains the exact weather conditions at the hour of the accident. 

This process could have been faster with the use of requests or beautifulsoup. However, that was not possible as wunderground.com used a mechanism to publish data using javascript and not html. If one visited a historical data webpage, they will notice that html text saying "No Summary, No Records" is loaded first before javascript kicks in after a second to load the data. Any request to the url using beautiful soup returns "No summary, No Results" as the data.

Motorcrash.csv is the file that contains all the accidents.
Filtered_mvc.csv contains data records of municipalities where more than 10000 accidents occured.

Weather_municipality1.csv,Weather_municipality3.csv,Weather_municipality2.csv was produced after the extraction process.
This script was run on multiple systems to speeden up the process of data scraping.

geckodriver.exe is the selenium webdriver for firefox.

merge_thread.py was written after several iterations. This helped us 70% of the data scraped. We used threads to speeden up the process and handle certain python exceptions. Earlier version would crash as soon as an error occured. Selenium errors closed the webdriver which crashed the program. Earliest iterations gave out of memory errors. 

merge.py generates filtered_mvc.csv which contains data from municipalities that have over 10k accidents and weather_municipality.csv which contains weather data. It also cleans and preps the data before loading them into dataframes for analysis. It uses an SVM classifier to predict accidents based on weather conditions. The nominal attributes were converted using one hot encoding. The attributes that finally went into the SVM classifier were decided using Weka. 5 fold cross validation was run to obtain accuracy of 69%.

Influence_of_Weather_on_Accidents.pdf is the paper report.

