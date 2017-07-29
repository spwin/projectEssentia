import core

files = core.createSampleDataCSV(
    path='common_directory/',
    filesCount=3,
    maxTrades=1000,
    instruments=core.createInstruments(),
    tradeReferences=core.createCustomList('TR', 5),
    instrumentTypes=core.createInstrumentTypes(),
    underlyingAssets=core.createCustomList('UA', 10),
    clientReferences=core.createCustomList('CR', 3),
    startDate='Jul 1 2017  08:30:00',
    endDate='Jul 5 2017  01:33:00',
    deletePrevious=True
)

print('Completed!')
print('Created ' + str(len(files)) + ' files:')
for f in files:
    print(f)
print('=' * 50)
print('Edit `' + __file__ + '` to change parameters.')