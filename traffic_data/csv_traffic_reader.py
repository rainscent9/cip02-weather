import pandas as pd
from datetime import datetime

URL = 'https://data.stadt-zuerich.ch/dataset/ted_taz_verkehrszaehlungen_werte_fussgaenger_velo/' \
      'download/2021_verkehrszaehlungen_werte_fussgaenger_velo.csv'
PATH = '/home/student/Cloud/Owncloud/Private/SyncVM/cip02-fs21/cip02-weather/traffic_data/'

df = pd.DataFrame()
df = pd.read_csv(URL)

# replace na values with 0. Assumption: the counters are working 100% of the time, therefore na-values are 0.
df.loc[:, 'VELO_IN':'FUSS_OUT'] = df.loc[:, 'VELO_IN':'FUSS_OUT'].fillna(0)

# set data types
df = df.astype({'VELO_IN': 'int',
                'VELO_OUT': 'int',
                'FUSS_IN': 'int',
                'FUSS_OUT': 'int'})
df['DATUM'] = pd.to_datetime(df['DATUM'], format='%Y-%m-%dT%H:%M')

# mask for rows newer than may 2021
mask = df['DATUM'] >= '2021-04-17'
# apply mask on rows, select columns of interest.
# 1. select subset with loc (more understandable)
# df = df.loc[mask, 'DATUM': 'FUSS_OUT'] # would be the easier way, but...
# 2. select subset with filter
df = df[mask].filter(regex='D(?=A)\w+|[V]\w+|F(?=U)\w+')

# combine all 24 traffic counter over the whole day and calculate the sum of each day per column
df = df.resample('D', on='DATUM').sum()
# calculate total of both directions
df['VELO_TOT'] = df['VELO_IN'] + df['VELO_OUT']
df['FUSS_TOT'] = df['FUSS_IN'] + df['FUSS_IN']
df.drop(columns=df.columns[0:4], inplace=True)

# write data into .csv file
# get time today in format YYYY:MM:DD HH:MM:SS
today = str(datetime.now()).rsplit(".")[0]
# define file name
filename = PATH + "download/traffic_zuerich_" + today.rsplit(' ')[0] + ".csv"
# write data with pandas
df.to_csv(filename, header=True, index=True)

