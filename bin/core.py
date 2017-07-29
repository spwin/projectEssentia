from datetime import datetime
import pandas as pd
import sqlite3 as db
import random
import time
import os


def createInstruments(currencies={
    'USD': 1, 'EUR': 0.85, 'JPY': 110.79, 'GBP': 0.76,
    'CHF': 0.97, 'CAD': 1.24, 'AUD': 1.25, 'NZD': 1.33, 'ZAR': 13.03
}):
    instruments = dict()
    for currency_1 in currencies.keys():
        for currency_2 in currencies.keys():
            if currency_1 != currency_2:
                ratio = currencies[currency_2] / currencies[currency_1]
                instruments[currency_1 + currency_2] = ratio
    return instruments


def createInstrumentTypes():
    return ['SELL', 'BUY']


def createCustomList(name, total=5):
    elements = []
    for i in range(total):
        elements.append(name + '_' + str(i + 1))
    return elements


def generateTimestamp(start_date, end_date):
    start_timestamp = time.mktime(time.strptime(start_date, '%b %d %Y %I:%M:%S'))
    end_timestamp = time.mktime(time.strptime(end_date, '%b %d %Y %I:%M:%S'))
    return random.randrange(start_timestamp, end_timestamp)


def priceThreshold(instrument, key):
    threshold = 0.1  # 10%
    start = instrument[key] - instrument[key] * threshold
    stop = instrument[key] + instrument[key] * threshold
    return round(random.uniform(start, stop), 4)


def deleteFilesInFolder(folder):
    if os.path.isdir(folder):
        for the_file in os.listdir(folder):
            file_path = os.path.join(folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(e)


def optional(value):
    return random.choice([value, ''])


def createSampleDataCSV(path='common_directory/',
                        filesCount=3,
                        maxTrades=1000,
                        instruments=createInstruments(),
                        tradeReferences=createCustomList('TR', 5),
                        instrumentTypes=createInstrumentTypes(),
                        underlyingAssets=createCustomList('UA', 10),
                        clientReferences=createCustomList('CR', 3),
                        startDate='Jul 1 2017  08:30:00',
                        endDate='Jul 5 2017  01:33:00',
                        deletePrevious=True
                        ):
    if deletePrevious:
        deleteFilesInFolder('../'+path)

    columns = ['Instrument', 'Price', 'Quantity',
               'Timestamp', 'Trade Reference',
               'Instrument Type', 'Underlying Asset',
               'Client Reference']

    files = []
    for i in range(filesCount):
        fileTrades = {title: [] for title in columns}
        for k in range(random.randint(0, maxTrades)):
            currentInstrument = random.choice(instruments.keys())
            fileTrades[columns[0]].append(currentInstrument)
            fileTrades[columns[1]].append(priceThreshold(instruments, currentInstrument))
            fileTrades[columns[2]].append(random.randint(1, 10000))
            fileTrades[columns[3]].append(generateTimestamp(startDate, endDate))
            fileTrades[columns[4]].append(optional(random.choice(tradeReferences)))
            fileTrades[columns[5]].append(optional(random.choice(instrumentTypes)))
            fileTrades[columns[6]].append(optional(random.choice(underlyingAssets)))
            fileTrades[columns[7]].append(optional(random.choice(clientReferences)))

        trades = pd.DataFrame(fileTrades, columns=columns)
        filename = path + 'trades_list_' + str(i) + '.csv'
        trades.to_csv('../' + filename, sep=',', encoding='utf-8', index=False)
        files.append(filename)
    return files


def readCSVFromFolder(folder, deleteFiles=True):
    tradesList = []
    for filename in os.listdir('../' + folder):
        if filename != '.ipynb_checkpoints' and filename.endswith('.csv'):
            trades = pd.read_csv('../' + folder + '/' + filename, sep=',')
            tradesList.append(trades.fillna(''))

    if deleteFiles:
        deleteFilesInFolder('../' + folder)

    return pd.concat(tradesList, ignore_index=True) if len(tradesList) > 0 else pd.DataFrame()


def saveDataFrameToDb(dataframe, table_name='trades', if_exists='append', db_name='database.db'):
    con = db.connect(db_name)
    normalizedToDB = dataframe.copy()
    normalizedToDB.columns = [x.lower().replace(" ", "_") for x in normalizedToDB.columns]
    normalizedToDB.to_sql(table_name, con, index=False, if_exists=if_exists)
    pass


def addMarketValueColumn(dataframe):
    dataframe['Market Value'] = dataframe['Price'] * dataframe['Quantity']
    return dataframe


def getInstrumentsDailyStats(trades):
    instrumentsDailyStats = dict()
    columns = ['Day', 'Total Market Value', 'Closing Value', 'Average Price']
    for instrument in trades['Instrument'].unique():
        instrumentData = trades.loc[trades['Instrument'] == instrument]
        instrumentData = instrumentData.assign(Date=pd.Series(
            [datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d') for timestamp in instrumentData['Timestamp']],
            index=instrumentData.index))
        dailyData = {title: [] for title in columns}

        for day in instrumentData['Date'].unique():
            currentDay = instrumentData.loc[instrumentData['Date'] == day]
            # Date
            dailyData[columns[0]].append(day)
            # Sum of Market Value column values
            dailyData[columns[1]].append(currentDay['Market Value'].sum())
            # Get price where Timestamp is max
            dailyData[columns[2]].append(currentDay.loc[currentDay['Timestamp'].idxmax()]['Price'])
            # Calculate mean of Price column
            dailyData[columns[3]].append(currentDay['Price'].mean())

        dailyDF = pd.DataFrame(dailyData)
        instrumentsDailyStats[instrument] = dailyDF

    return instrumentsDailyStats


def getTradeReferencesConstituents(trades):
    TrCt = dict()
    for tr in trades['Trade Reference'].unique():
        if tr != '':
            TrCt[tr] = trades.loc[trades['Trade Reference'] == tr]
    return TrCt


def getClosingPosition(currentInstrument):
    instrumentType = ''
    typeSet = currentInstrument.loc[currentInstrument['Instrument Type'] != '']
    if len(typeSet) > 0:
        instrumentType = currentInstrument.loc[typeSet['Timestamp'].idxmax()]['Instrument Type']
    return instrumentType


def getDailyStats(trades):
    dailyStats = dict()
    columns = ['Instrument', 'Total Traded Value', 'Closing Value', 'Closing Position']
    trades = trades.assign(
        Date=pd.Series([datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d') for timestamp in trades['Timestamp']],
                       index=trades.index))
    for day in trades['Date'].unique():
        tradesByDay = trades.loc[trades['Date'] == day]
        dailyInstruments = {title: [] for title in columns}
        for instrument in tradesByDay['Instrument'].unique():
            currentInstrument = tradesByDay.loc[tradesByDay['Instrument'] == instrument]
            dailyInstruments[columns[0]].append(instrument)
            dailyInstruments[columns[1]].append(currentInstrument['Market Value'].sum())
            dailyInstruments[columns[2]].append(
                currentInstrument.loc[currentInstrument['Instrument Type'] == 'SELL']['Market Value'].sum() -
                currentInstrument.loc[currentInstrument['Instrument Type'] == 'BUY']['Market Value'].sum())
            dailyInstruments[columns[3]].append(getClosingPosition(currentInstrument))
        dailyInstrumentsDF = pd.DataFrame(dailyInstruments)
        dailyStats[day] = dailyInstrumentsDF
    return dailyStats


def saveAsCsv(path, title, data, folder):
    fullPath = path + '/' + folder + '/'
    if not os.path.isdir(fullPath):
        os.makedirs(fullPath)
    data.to_csv(fullPath + title + '.csv',
                sep=',', encoding='utf-8', index=False)
    pass


def outputDataToFiles(trades, instDaily, trConsist, dailyStats, path='output', deleteBefore=True):
    if deleteBefore:
        deleteFilesInFolder('../' + path)
    saveAsCsv('../' + path, 'added_market_value', trades, 'Task1')
    for instrument, data in instDaily.items():
        saveAsCsv('../' + path, instrument, data, 'Task2')
    for tr, data in trConsist.items():
        saveAsCsv('../' + path, tr, data, 'Task3')
    for day, data in dailyStats.items():
        saveAsCsv('../' + path, day, data, 'Task4')
