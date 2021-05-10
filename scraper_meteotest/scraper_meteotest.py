from datetime import datetime
from datetime import timedelta
import logging
import numpy as np
import pandas as pd
import random
import time
# Selenium
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
# Selenium exception handling inspired from:
# https://www.pingshiuanchua.com/blog/post/error-handling-in-selenium-on-python
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import WebDriverException

'''
This script is designed to scrape weather forecasts from meteotest.ch. It can be run in simulation mode or in continuous 
mode.
For CONTINUOUS MODE (will scrape meteotest.ch ever morning at 11:00 am):
1. in "scraper_start.sh" set the path of the virtual environment
2. copy "scraper_start.sh", "scraper_meteotest.py" and folder "scraped" to the desired location
3. open terminal in that very location and:
    chmod -x scraper_start.sh
    chmod -x scraper_meteotest.py
4. execute "scraper_start.sh"

CONFIGURABLE:
- PATH_DRIVER (chrome driver for Selenium)
- LOCATIONS (any city in Switzerland can be added)
- SIMULATION (1: instantly execute scraping
              0: run in loop, only execute scraping at 11:00 am)

OUTPUT (in folder scraped):
- Daily files:
    - meteotest_wind_DATE.csv
    - meteotest_weather_DATE.csv
    - meteotest_clean_DATE.csv
- Combined values over (will be appended forever)
    - meteotest_combined_clean.csv (USE THIS FILE FOR FURTHER PROCESSING)
    - meteotest_combined_weather.csv
    - meteotest_combined_wind.csv (hourly wind forecasts)
'''

# define variables
PATH_DRIVER = '/usr/lib/chromium-browser/chromedriver'
WEBPAGE_WEATHER = 'https://meteotest.ch/wetter/ortswetter/'
WEBPAGE_WIND = 'https://meteotest.ch/wetter/wind/'
LOCATIONS = ['Bern', 'Zürich', 'Luzern', 'Fribourg', 'Jungfraujoch']
SIMULATION = 1  # 1:ON, 0:OFF
if SIMULATION:
    PATH = '/home/student/Cloud/Owncloud/Private/SyncVM/cip02-fs21/cip02-weather/scraper_meteotest/'
    # create log file
    logging.basicConfig(filename=PATH + 'meteotest_scraper' + '.log',
                        level=logging.INFO)
else:
    PATH = '/home/student/Cloud/Owncloud/Private/SyncVM/cip02-fs21/Project/'
    # create log file
    logging.basicConfig(filename=PATH + 'meteotest_scraper' + '.log',
                        level=logging.INFO)  # for long uptimes set level to WARNING or ERROR


# Main scraper function, in simulation mode it takes about 5 min to scrape the data
def scraper_weather():
    # Start job within 5 minutes
    if not SIMULATION:
        time.sleep(random.randint(1, 600))
    # create empty pandas dataframes
    df_weather_data = pd.DataFrame()
    df_wind_data = pd.DataFrame()
    # set user-agent
    opts = Options()
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36")
    try:
        # create Chrome web driver object
        driver = webdriver.Chrome(executable_path=PATH_DRIVER, options=opts)
    except WebDriverException:
        # create error / warning log message
        logging.warning(
            msg=str(datetime.now()).rsplit(".")[0] + ' Exception web driver (e.g. bad web driver path)')
        return None
    # iterate over all locations
    for location in LOCATIONS:
        # WIND FORECAST HOURLY
        # open webpage - wind
        driver.get(WEBPAGE_WIND + location)
        # select the division with the forecast
        delay = 5  # max time to load page in seconds
        try:
            # wait until the class with the forecast is loaded
            wind = WebDriverWait(driver, delay).until(ec.presence_of_element_located((
                By.CSS_SELECTOR, '.container > .weatherPanel > .windTable')))
            # create info log message
            logging.info(msg=str(datetime.now()).rsplit(".")[0] + ' wind page load ok for location: ' + location)
        except TimeoutException:
            # create error / warning log message
            logging.warning(
                msg=str(datetime.now()).rsplit(".")[0] + ' Exception wind page load for location: ' + location)
            return None
        # wait before clicking
        time.sleep(15)
        # iterate over 5 days
        for day in range(0, 5):
            # click on day i
            try:
                driver.find_elements_by_css_selector('.tablistContainer > .nav.nav-tabs span[class="hidden-xs"]')[
                    day].click()
            except ElementClickInterceptedException:
                logging.warning(
                    msg=str(datetime.now()).rsplit(".")[0] +
                    ' Exception wind page clicking for location: ' + location)
            except NoSuchElementException:
                logging.warning(
                    msg=str(datetime.now()).rsplit(".")[0] +
                    ' Exception wind page clicking (element not found) for location: ' + location)
            # wait for page to load
            time.sleep(2)
            try:
                # load division with wind forecast table per day
                # (hourly data, 24 entries, 2 tables of 12 entries, both with header)
                forecast = wind.find_elements_by_css_selector(".weatherPanelHalf > .stundenPrognoseRow")
                # remove second header (from table of second half of the day)
                del forecast[13]
                # ignore first header ()
                for d in forecast[1:]:
                    data = {"date_scraped": str(datetime.now()).rsplit(".")[0],
                            "website_scraped": 'meteotest',
                            "date_forecast": str(datetime.now() + timedelta(days=day)).rsplit(" ")[0],
                            "location": location,
                            "time": d.text.split("\n")[0],
                            "wind_dir": d.text.split("\n")[1],
                            "wind_mean": d.find_elements_by_css_selector('.stundenPrognoseCol.beaufortValues')[
                                0].text,
                            "wind_peak": d.find_elements_by_css_selector('.stundenPrognoseCol.beaufortValues')[
                                1].text,
                            "temperature":
                                d.find_elements_by_css_selector(".stundenPrognoseCol.data > span[class='celsius']")[
                                    0].text,
                            "percipation_mm":
                                d.find_elements_by_css_selector(".stundenPrognoseCol.data > span[class='mm']")[
                                    0].text}
                    df_wind_data = df_wind_data.append(data, ignore_index=True)

            except NoSuchElementException:
                logging.warning(
                    msg=str(datetime.now()).rsplit(".")[0] +
                    ' Exception wind page extracting (element not found) for location: ' + location)
        # sleep for a random amount of time
        time.sleep(random.randint(3, 9))

        # WEATHER FORECAST DAILY
        # open webpage
        driver.get(WEBPAGE_WEATHER + location)
        # select the division with the forecast
        delay = 5  # max time to load page in seconds
        try:
            # wait until the class with the forecast is loaded
            forecast = WebDriverWait(driver, delay).until(ec.presence_of_element_located((
                By.CSS_SELECTOR, '.fivedaysforecast > .container > .row > .tablistContainer')))
            # create info log message
            logging.info(msg=str(datetime.now()).rsplit(".")[0] + ' page load ok for location: ' + location)
        except TimeoutException:
            # create error / warning log message
            logging.warning(
                msg=str(datetime.now()).rsplit(".")[0] + ' Exception page load for location: ' + location)
            return None

        try:
            # get the sections with the corresponding information
            dates = forecast.find_elements_by_css_selector(".registerDateTime")  # ok
            temps_min = forecast.find_elements_by_css_selector("span[class='celsius min']")  # ok
            temps_max = forecast.find_elements_by_css_selector("span[class='celsius max']")  # ok
            icons = forecast.find_elements_by_css_selector(".registerIcon img")  # ok
            rain_percent = forecast.find_elements_by_css_selector(".tabRain span[class='percent']")  # ok
            rain_mm = forecast.find_elements_by_css_selector(".tabRain span[class='mm']")  # ok
            # iterate over five days - extract data by day
            for day in range(0, 5):
                data = {"date_scraped": str(datetime.now()).rsplit(".")[0],
                        "website_scraped": 'meteotest',
                        "date_forecast": dates[day].text.rsplit(' ')[1],
                        "location": location,
                        "temp_min_celsius": temps_min[day].text,
                        "temp_max_celsius": temps_max[day].text,
                        "precipitation_percent": rain_percent[day].text,
                        "precipitation_mm": rain_mm[day].text,
                        "weather_forecast": icons[day].get_attribute('title')}

                # store dataframe into pandas dataframe
                df_weather_data = df_weather_data.append(data, ignore_index=True)

        except NoSuchElementException:
            logging.warning(
                msg=str(datetime.now()).rsplit(".")[0] +
                ' Exception weather page (element not found) for location: ' + location)

        time.sleep(random.randint(3, 9))
    # write .csv files for backup
    writer(df_wind_data, info='wind', path=PATH)
    writer(df_weather_data, info='weather', path=PATH)
    # Data wrangling: wind forecast df
    df_wind_data = clean_wind(df=df_wind_data)
    # Data wrangling: weather forecast df
    df_weather_data = clean_weather(df=df_weather_data)
    # merge df with weather and wind
    df_weather_data = pd.merge(df_weather_data, df_wind_data, how='left', on=['location', 'date_forecast'])
    # close Chrome window
    driver.quit()
    # return merged df with weather and wind
    return df_weather_data


def clean_wind(df):
    # remove rows from wind forecast
    df.drop(columns=['temperature', 'percipation_mm', 'time', 'wind_dir'], axis=1, inplace=True)
    # set data types
    mask = df.columns.isin(['wind_mean', 'wind_peak'])
    df.loc[:, mask] = df.loc[:, mask].astype('int')
    df['date_forecast'] = pd.to_datetime(df['date_forecast'], format='%Y-%m-%d')
    df['date_scraped'] = pd.to_datetime(df['date_scraped'], format='%Y-%m-%d %H:%M:%S')

    df_wind_processed = df.groupby(['location', 'date_forecast'], as_index=False)['wind_peak'].agg('max')
    # df_wind_data_.groupby(['location', 'date_forecast'], as_index=False)['wind_mean'].agg('mean') # nok
    # --> könnte mit dem zusammenhangen: https://github.com/pandas-dev/pandas/issues/33515
    df_wind_processed = pd.merge(df_wind_processed,
                                 df.groupby(['location', 'date_forecast'], as_index=False)['wind_mean'].agg(
                                     lambda x: np.sum(x) / 24),
                                 how='left', on=['location', 'date_forecast'])
    return df_wind_processed


def clean_weather(df):
    # get time today in format YYYY:MM:DD HH:MM:SS
    today = str(datetime.now()).rsplit(".")[0]
    # set date from dd.m --> yyyy-mm-dd
    df['date_forecast'] = str(pd.to_datetime(today).year) + '-' + \
        df['date_forecast'].str.split('.').apply(lambda x: str(x[1]).zfill(2)) + '-' + \
        df['date_forecast'].str.split('.').apply(lambda x: str(x[0]).zfill(2))
    # make 'date_forecast' of type datetime
    df['date_forecast'] = pd.to_datetime(df['date_forecast'], format='%Y-%m-%d')
    # Create dataframe to convert forecast name to number
    df_enum = pd.DataFrame({'forecast': ['schön',
                                         'leicht bewölkt',
                                         'bewölkt',
                                         'stark bewölkt',
                                         'Wärmegewitter',
                                         'starker Regen',
                                         'Schneefall',
                                         'Nebel',
                                         'Schneeregen',
                                         'Regenschauer',
                                         'leichter Regen',
                                         'Schneeschauer',
                                         'Gerwitter',
                                         'Hochnebel',
                                         'Schneeregenschauer']})

    # Make forecast strings defined above lowercase
    df_enum['forecast'] = df_enum['forecast'].str.lower()
    # set index from 1 upwards
    df_enum = df_enum.set_index(df_enum.index + 1)
    # create dictionary from dataframe enum
    dict_forecast = df_enum.to_dict()
    # invert the key-value pairs
    dict_forecast = {v: k for k, v in dict_forecast['forecast'].items()}
    # add a new row with a numeric forecast
    df['weather_forecast_numeric'] = df['weather_forecast'].str.lower().replace(dict_forecast)
    return df


def writer(df, info='no_info', path=''):
    # get time today in format YYYY:MM:DD HH:MM:SS
    today = str(datetime.now()).rsplit(".")[0]
    # define file name
    filename = path + "scraped/meteotest_" + info + '_' + today.rsplit(' ')[0] + ".csv"
    filename_combined = path + "scraped/meteotest_combined_" + info + ".csv"
    # write data with pandas
    df.to_csv(filename, header=True, index=False)
    df.to_csv(filename_combined, mode='a', header=False, index=False)


if __name__ == "__main__":
    while not SIMULATION:
        if (datetime.now().hour == 11) and (datetime.now().minute == 0):
            df_forecast = scraper_weather()
            writer(df_forecast, info='clean', path=PATH)
        time.sleep(30)
    # execute this once when simulation active:
    df_forecast = scraper_weather()
    writer(df_forecast, info='clean', path=PATH)
