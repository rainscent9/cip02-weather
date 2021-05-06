import logging
import pandas as pd
import time
from datetime import datetime
import random
# Selenium
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# define variables
PATH_DRIVER = '/usr/lib/chromium-browser/chromedriver'
WEBPAGE = 'https://www.hydrodaten.admin.ch/de/'
LOCATIONS = {'Bern': '2135.html', 'Thun': '2030.html'}  # (stations)
SIMULATION = 1  # 1:ON, 0:OFF

# create log file
if SIMULATION:
    PATH = '/home/student/Cloud/Owncloud/Private/SyncVM/cip02-fs21/cip02-weather/scraper_meteotest/'
    # create log file
    logging.basicConfig(filename=PATH + 'hydrodata_scraper' + '.log',
                        level=logging.INFO)
else:
    PATH = '/home/student/Cloud/Owncloud/Private/SyncVM/cip02-fs21/Project/'
    # create log file
    logging.basicConfig(filename=PATH + 'hydrodata_scraper' + '.log',
                        level=logging.ERROR)


def scraper_weather():
    # Start cronjob within 5 minutes
    if not SIMULATION:
        time.sleep(random.randint(1, 600))

    # create empty pandas dataframe
    df_water_data = pd.DataFrame()
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

    for location in LOCATIONS:
        # WIND FORECAST HOURLY
        # open webpage - wind
        driver.get(WEBPAGE + LOCATIONS[location])
        # select the division with the forecast
        delay = 5  # max time to load page in seconds
        try:
            # wait until the class with the forecast is loaded
            wind = WebDriverWait(driver, delay).until(ec.presence_of_element_located((
                By.CSS_SELECTOR, '.container.container-main > .container-fluid > .horizontal-scroll-wrapper > '
                                 '.table.table-bordered.table-narrow')))
            # create info log message
            logging.info(msg=str(datetime.now()).rsplit(".")[0] +
                             ' water temperature page load ok for location: ' + location)
        except TimeoutException:
            # create error / warning log message
            logging.warning(
                msg=str(datetime.now()).rsplit(".")[0] + ' water temperature load error for location: ' + location)
            return None
        # wait before clicking
        time.sleep(5)

        data = wind.text.split('\n')

        # set dates from %d.%m.%Y %H:%M to %Y-%m-%d %H:%M:%S
        for measurement in range(1, 7, 2):
            date = data[measurement].split(' ')[0].split('.')
            data[measurement] = date[2] + '-' + date[1] + '-' + date[0] + ' ' + data[measurement].split(' ')[1] + ':00'
        # put data into dict
        data = {'date_scraped': str(datetime.now()).rsplit(".")[0],
                'website_scraped': 'hydrodaten admin',
                'station': location,
                'flow_date': str(pd.to_datetime(data[1], format='%Y-%m-%d %H:%M:%S')),
                'flow_value_last': data[7].split(' ')[2],
                'flow_value_mean_24': data[8].split(' ')[2],
                'flow_value_max_24': data[9].split(' ')[2],
                'height_date': str(pd.to_datetime(data[3], format='%Y-%m-%d %H:%M:%S')),
                'height_value_last': data[7].split(' ')[3],
                'height_value_mean_24': data[8].split(' ')[3],
                'height_value_max_24': data[9].split(' ')[3],
                'temp_date': str(pd.to_datetime(data[5], format='%Y-%m-%d %H:%M:%S')),
                'temp_value_last': data[7].split(' ')[4],
                'temp_value_mean_24': data[8].split(' ')[4],
                'temp_value_max_24': data[9].split(' ')[4],
                }
        df_water_data = df_water_data.append(data, ignore_index=True)
        time.sleep(random.randint(3, 9))

    # write .csv files for backup
    writer(df_water_data, info='water', path=PATH)

    # close Chrome window
    driver.quit()

    # return merged df with weather and wind
    return df_water_data


def writer(df, info='no_info', path=''):
    # get time today in format YYYY:MM:DD HH:MM:SS
    today = str(datetime.now()).rsplit(".")[0]
    # define file name
    filename = path + "scraped/hydrodata_" + info + '_' + today.rsplit(' ')[0] + ".csv"
    filename_combined = path + "scraped/hydrodata_combined_" + info + ".csv"
    # write data with pandas
    df.to_csv(filename, header=True, index=False)
    df.to_csv(filename_combined, mode='a', header=False, index=False)


while not SIMULATION:
    if datetime.now().minute == 0:
        df_forecast = scraper_weather()
        writer(df_forecast, info='clean', path=PATH)
    time.sleep(50)
# execute this once when simulation active:
df_water = scraper_weather()
writer(df_water, info='clean', path=PATH)
