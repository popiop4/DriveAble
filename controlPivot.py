#new method using pivot_table
#i think this is the way to go if we can make it work for the other sheets
#we'll just want to smoosh the csv column names together into one for simplicity

import pandas as pd

file = ''
xl = pd.ExcelFile(file)

dfControl= xl.parse('Control')

ids_with_tp = dfControl['identification_id']
ids = []
tps = []

for i in ids_with_tp:
    ids.append(i[:4])
    tps.append(i[5:].upper().strip())

dfControl['subID'] = ids
dfControl['TP'] = tps

dfControlFinal = dfControl.pivot_table(index=['subID'],
                           columns=['TP','lap_number']
                           ).reset_index()

dfControlFinal.to_csv('Output.csv',index=True)
