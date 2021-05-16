import pandas as pd
from pathlib import Path
from datetime import datetime

# path to store .csv files
PATH = '/home/student/Cloud/Owncloud/Private/SyncVM/cip02-fs21/cip02-weather/traffic_data/'


def bicycle_reader(df, name=''):
    # write .csv with original data
    csv_writer(df, name=name, status='_src')
    # replace na values with 0. Assumption: the counters are working 100% of the time, therefore na-values are 0.
    df.loc[:, 'VELO_IN':'FUSS_OUT'] = df.loc[:, 'VELO_IN':'FUSS_OUT'].fillna(0)
    # set data types
    df = df.astype({'VELO_IN': 'int',
                    'VELO_OUT': 'int',
                    'FUSS_IN': 'int',
                    'FUSS_OUT': 'int'})
    df['DATUM'] = pd.to_datetime(df['DATUM'], format='%Y-%m-%dT%H:%M')
    # mask for rows newer than ... and only select those rows
    mask = df['DATUM'] >= '2021-04-17'
    # apply mask on rows, select columns of interest.
    # 1. select subset with loc (more understandable)
    # df = df.loc[mask, 'DATUM': 'FUSS_OUT'] # would be the easier way, but...
    # 2. select subset with filter via regex
    df = df[mask].filter(regex='D(?=A)\\w+|[V]\\w+|F(?=U)\\w+')
    # combine all 24 traffic counters over the whole day and calculate the sum of each day per column
    df = df.resample('D', on='DATUM').sum()
    # calculate total of both directions
    df['VELO_TOT'] = df['VELO_IN'] + df['VELO_OUT']
    df['FUSS_TOT'] = df['FUSS_IN'] + df['FUSS_IN']
    df.drop(columns=df.columns[0:4], inplace=True)
    return df


def bus_reader(df, name=''):
    # write .csv with original data
    csv_writer(df, name=name, status='_src')
    # fill na values with 0 for counters
    df.loc[:, 'In':'Out'] = df.loc[:, 'In':'Out'].fillna(0)
    # set data types
    df = df.astype({'In': 'int',
                    'Out': 'int'})
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y-%m-%dT%H:%M:%S')
    # rename the date (make it consistent with the other traffic data)
    df = df.rename(columns={'Timestamp': 'DATUM'})
    # mask for rows newer than ... and only select those rows
    mask = df['DATUM'] >= '2021-04-17'
    df = df.loc[mask, 'In':'DATUM']
    # resample to days
    df = df.resample('D', on='DATUM').sum()
    # calculate total frequency per day (in plus out of the bus)
    df['BUS_TOT'] = df['In'] + df['Out']
    # drop no more used columns
    df.drop(columns=['In', 'Out'], inplace=True)
    return df


def car_reader(df, name=''):
    # write .csv with original data
    csv_writer(df, name=name, status='_src')
    # fill na values with 0 for counters
    df['AnzFahrzeuge'] = df['AnzFahrzeuge'].fillna(0)
    # set data types
    df = df.astype({'AnzFahrzeuge': 'int'})
    df['MessungDatZeit'] = pd.to_datetime(df['MessungDatZeit'], format='%Y-%m-%dT%H:%M:%S')
    # rename the counter and date (make it consistent with the other traffic data)
    df = df.rename(columns={'AnzFahrzeuge': 'CAR_TOT', 'MessungDatZeit': 'DATUM'})
    # mask for rows newer than ... and only select those rows
    mask = df['DATUM'] >= '2021-04-17'
    df = df.loc[mask, ['DATUM', 'CAR_TOT']]
    # resample to days
    df = df.resample('D', on='DATUM').sum()
    return df


def csv_writer(df, name='', status=''):
    # write data into .csv file
    # get time today in format YYYY:MM:DD HH:MM:SS
    today = str(datetime.now()).rsplit('.')[0]
    # define file name
    filename = PATH + 'download/' + name + '_' + today.rsplit(' ')[0] + status + '.csv'
    # write data with pandas
    df.to_csv(filename, header=True, index=True)


# dictionary of transport type, wrangling function and data url
data_dir = {'bicycle':
            [bicycle_reader,
             'https://data.stadt-zuerich.ch/dataset/ted_taz_verkehrszaehlungen_werte_fussgaenger_velo/download/2021_'
             'verkehrszaehlungen_werte_fussgaenger_velo.csv'],
            'bus':
            [bus_reader,
             'https://data.stadt-zuerich.ch/dataset/vbz_frequenzen_hardbruecke/download/frequenzen_'
             'hardbruecke_2021.csv'],
            'car':
            [car_reader,
             'https://data.stadt-zuerich.ch/dataset/sid_dav_verkehrszaehlung_miv_od2031/download/sid_dav_'
             'verkehrszaehlung_miv_OD2031_2021.csv']
            }


if __name__ == '__main__':
    # create folder 'scraped' if not jet existing
    Path(PATH + '/download').mkdir(parents=True, exist_ok=True)
    # download data, main
    for i in data_dir:
        # write cleaned .csv in the following line, dirty .csv is written within called function
        csv_writer(data_dir[i][0](pd.read_csv(data_dir[i][1]), i), name=i, status='_stage')
