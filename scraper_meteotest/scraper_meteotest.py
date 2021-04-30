import pandas as pd
import time
from datetime import datetime
from selenium import webdriver
import random
from selenium.webdriver.chrome.options import Options

# Time out
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

# set locations to be scraped
locations = ['Bern', 'Zuerich', 'Luzern', 'Fribourg', 'Jungfraujoch']


def scraper():
    # Start cronjob within 5 minutes
    time.sleep(random.randint(1, 600))

    # create empty pandas dataframe
    df = pd.DataFrame()
    try:
        opts = Options()
        opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36")
        # open chrome
        driver = webdriver.Chrome(executable_path='/usr/lib/chromium-browser/chromedriver', chrome_options=opts)
        for location in locations:

            # open webpage
            driver.get('https://meteotest.ch/wetter/ortswetter/' + location)

            # give the webpage some time to load
            time.sleep(5)

            delay = 3  # seconds

            try:
                myElem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.ID, 'IdOfMyElement')))
                print
                "Page is ready!"
            except TimeoutException:
                print
                "Loading took too much time!"

            # select the devision with the forecast
            forecast = driver.find_element_by_css_selector(
                '.fivedaysforecast > .container > .row > .tablistContainer')  # ok

            # get the sections with the corresponding information
            dates = forecast.find_elements_by_css_selector(".registerDateTime")  # ok
            temps_min = forecast.find_elements_by_css_selector("span[class='celsius min']")  # ok
            temps_max = forecast.find_elements_by_css_selector("span[class='celsius max']")  # ok
            icons = forecast.find_elements_by_css_selector(".registerIcon img")  # ok
            rain_percent = forecast.find_elements_by_css_selector(".tabRain span[class='percent']")  # ok
            rain_mm = forecast.find_elements_by_css_selector(".tabRain span[class='mm']")  # ok

            # iterate over five days
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
                df = df.append(data, ignore_index=True)

            # sleep for a random amount of time
            time.sleep(random.randint(1, 9))
    finally:
        # close chrome window
        driver.quit()

    return df


def cleaner(df):
    # get time today in format YYYY:MM:DD HH:MM:SS
    today = str(datetime.now()).rsplit(".")[0]

    # set date from dd.m --> yyyy-mm-dd
    df['date_forecast'] = str(pd.to_datetime(today).year) + '-' + \
                          df['date_forecast'].str.split('.').apply(lambda x: str(x[1]).zfill(2)) + '-' + \
                          df['date_forecast'].str.split('.').apply(lambda x: str(x[0]).zfill(2))
    # make 'date_forecast' of type datetime
    df['date_forecast'] = pd.to_datetime(df['date_forecast'])

    # Creat dataframe to convert forecast name to number
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
    # Make lowercase
    df_enum['forecast'] = df_enum['forecast'].str.lower()
    # set index from 1 upwards
    df_enum = df_enum.set_index(df_enum.index + 1)

    # create dictionary from dataframe enum
    dict_forecast = df_enum.to_dict()
    # invert the key-value paires
    dict_forecast = {v: k for k, v in dict_forecast['forecast'].items()}

    # add a new row with a numeric forecast
    df['weather_forecast_numeric'] = df['weather_forecast'].str.lower().replace(dict_forecast)
    return df


def writer(df, info='noinfo'):
    # get time today in format YYYY:MM:DD HH:MM:SS
    today = str(datetime.now()).rsplit(".")[0]
    # define file name
    filename = "/home/student/Cloud/Owncloud/Private/SyncVM/cip02-fs21/Project/scraped/meteotest_" + info + '_' + \
               today.rsplit(' ')[0] + ".csv"
    filename_combined = "/home/student/Cloud/Owncloud/Private/SyncVM/cip02-fs21/Project/scraped/meteotest_combined_" \
                        + info + ".csv"
    # write data with pandas
    df.to_csv(filename, header=True, index=False)
    df.to_csv(filename_combined, mode='a', header=False, index=False)


# Die Website lädt über einen bestimmten Zeitraum Daten. Mit Selenium kann ich warten bis die Seite
# geladen ist und anschliessend die .html Datei laden.

while True:
    if (datetime.now().hour == 11) and (datetime.now().minute == 0):
        df_dirty = scraper()
        writer(df_dirty, 'dirty')
        df_clean = cleaner(df_dirty)
        writer(df_clean, 'clean')
    time.sleep(30)