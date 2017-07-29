import core

# Read trades from all the .csv files in given folder
allTrades = core.readCSVFromFolder(
    folder='common_directory',
    deleteFiles=False
)

# Save all the trades to SQLite3 database
core.saveDataFrameToDb(
    allTrades,
    if_exists='replace',
    db_name='database.db'
)

# Add Market Value Column to Panda DataFrame
allTrades = core.addMarketValueColumn(allTrades)

# For each instrument get total market value, the closing value,
# and average price per day
instrumentsDailyStats = core.getInstrumentsDailyStats(allTrades)

# For each trade reference get the constituent trades
TRConsistTrades = core.getTradeReferencesConstituents(allTrades)

# For each day get total traded value, closing value, and closing position
dailyStats = core.getDailyStats(allTrades)

# Output data in .csv format to given folder
# Each Task files are created in different folders
output_dir = 'output'
core.outputDataToFiles(
    allTrades,
    instrumentsDailyStats,
    TRConsistTrades,
    dailyStats,
    path=output_dir,
    deleteBefore=True
)

print('Completed!')
print('Please check the result files in `' + output_dir + '` folder.')
print('=' * 50)
print('Edit `' + __file__ + '` to change parameters.')
