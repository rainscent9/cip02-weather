# import all required libraries
from pathlib import Path
import time
import random
import re
from datetime import datetime
import pandas as pd
import mechanicalsoup
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# define all locations we want to scrape
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
LOCATIONS = {
    'Freiburg': '2660718',
    'Bern': '2661552',
    'Zürich': '2657896',
    'Luzern': '2659811',
    'Jungfroujoch': '2660208'}

# setup a sendgrid client
sg = SendGridAPIClient('---')

def send_notification(subject, message):
    sg.send(Mail(
        from_email='notifications@airborne.swiss',
        to_emails='samuel.loertscher@gmail.com',
        subject=subject,
        html_content=f'<strong>{message}</strong>'))

try:
    # load current data if any such already exist
    df = pd.DataFrame(data=[], columns=[
        'location',
        'scrape_date',
        'prediction_date',
        'temp_max',
        'temp_min',
        'wind',
        'precipitation',
        'forecast'])

    # create a reusable stateful browser
    print(' > create a stateful browser')
    browser = mechanicalsoup.StatefulBrowser(
        user_agent=USER_AGENT,
        raise_on_404=True)

    # iterate over each location individually
    for location, id in LOCATIONS.items():
        # browse the current location and isolate the part in which the content is to be found
        url = f'https://www.meteoblue.com/de/wetter/woche/{location.lower()}_schweiz_{id}'
        print(f' > fetch url: {url}')
        browser.open(url)
        wrapper = browser.page.select('main > .grid > #tab_results > #tab_wrapper')

        # extract the model actualisation date
        print(' > extract prediction date')
        predication_date = (wrapper[0].select_one('.tab_detail .model-info .misc')
                            .find('span', text = re.compile('(?i)modell-aktualisierung'))
                            .findNext('span')
                            .text)

        # iterate over each day of the current location
        print(' > extract weather parameters for each weekday')
        for day in wrapper[0].findAll('div', {'id' : re.compile('(?i)day\d')}):
            df = df.append({
                'location': location,
                'scrape_date': datetime.now().replace(microsecond = 0),
                'prediction_date': predication_date,
                'temp_max': day.select_one('.temps .tab_temp_max').text,
                'temp_min': day.select_one('.temps .tab_temp_min').text,
                'wind': day.select_one('.data .wind').text,
                'precipitation': day.select_one('.data .tab_precip').text,
                'forecast': day.select_one('.weather img')['title']
            }, ignore_index=True)

        # wait between 0.5s and 1.0s to avoid suspicion
        time.sleep(0.5 + 0.5 * random.random())

    # clean the data
    print(' > clean dataset')
    df['prediction_date'] = pd.to_datetime(df['prediction_date'], format='%d.%m.%Y %H:%M')
    df['temp_max'] = df['temp_max'].str.extract(r'(\d\d?)').astype(int)
    df['temp_min'] = df['temp_min'].str.extract(r'(\d\d?)').astype(int)
    df['wind'] = df['wind'].str.extract(r'(\d\d?)').astype(int)
    df['precipitation'] = df['precipitation'].str.extract(r'((?:\d+-\d+)|(?:\.\.\.))')

    # if a dataset already exists we append the current dataset to it
    if Path('./meteoblue.csv').is_file():
        print(' > load and merge existing dataset')
        df_existing = pd.read_csv('./meteoblue.csv')
        df = pd.concat([df_existing, df], ignore_index=True)

    # finally store the dataset on the disk
    print(' > write dataset to disk')
    df.to_csv('meteoblue.csv', index=False)

    print(' > send e-mail notification to samuel.loertscher@gmail.com')
    send_notification('Success from CIP scraper', 'CIP scraper processed successfully')
except Exception as exp:
    print(f' > ERROR: {exp}')
    print(' > send e-mail notification to samuel.loertscher@gmail.com')
    send_notification('Error from CIP scraper', f'ERROR: {exp}')