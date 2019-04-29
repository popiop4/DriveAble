#first pass at converting control sheet to long format
#it's likely able to be improved by splitting into separate functions
#there's probably a better way :)

import pandas as pd

def controlLong(file):

    xl = pd.ExcelFile(file)
    print(xl.sheet_names)

    #make dataframe from control spreadsheet
    dfControl= xl.parse('Control')

    #create new column for ids and timepoints
    ids_with_tp = dfControl['identification_id']
    ids = []
    tps = []

    for i in ids_with_tp:
        ids.append(i[:4])
        tps.append(i[5:])

    dfControl['subID'] = ids
    dfControl['TP'] = tps

    #create separate dataframes for each lap and timepoint
    dfControlT1_lap1 = dfControl[(dfControl['identification_id'].str.contains('T1')) & (dfControl['lap_number'] == 1)]
    dfControlT1_lap2 = dfControl[(dfControl['identification_id'].str.contains('T1')) & (dfControl['lap_number'] == 2)]
    dfControlT1_lap3 = dfControl[(dfControl['identification_id'].str.contains('T1')) & (dfControl['lap_number'] == 3)]

    #rename columns in dataframes to indicate lap and timepoint, function?
    dfControlT1_lap1.columns = dfControlT1_lap1.columns + '_t1_lap1'
    dfControlT1_lap2.columns = dfControlT1_lap2.columns + '_t1_lap2'
    dfControlT1_lap3.columns = dfControlT1_lap3.columns + '_t1_lap3'

    #timepoint 2
    dfControlT2_lap1 = dfControl[(dfControl['identification_id'].str.contains('T2')) & (dfControl['lap_number'] == 1)]
    dfControlT2_lap2 = dfControl[(dfControl['identification_id'].str.contains('T2')) & (dfControl['lap_number'] == 2)]
    dfControlT2_lap3 = dfControl[(dfControl['identification_id'].str.contains('T2')) & (dfControl['lap_number'] == 3)]

    dfControlT2_lap1.columns = dfControlT2_lap1.columns + '_t2_lap1'
    dfControlT2_lap2.columns = dfControlT2_lap2.columns + '_t2_lap2'
    dfControlT2_lap3.columns = dfControlT2_lap3.columns + '_t2_lap3'

    #timepoint 3
    dfControlT3_lap1 = dfControl[(dfControl['identification_id'].str.contains('T3')) & (dfControl['lap_number'] == 1)]
    dfControlT3_lap2 = dfControl[(dfControl['identification_id'].str.contains('T3')) & (dfControl['lap_number'] == 2)]
    dfControlT3_lap3 = dfControl[(dfControl['identification_id'].str.contains('T3')) & (dfControl['lap_number'] == 3)]

    dfControlT3_lap1.columns = dfControlT3_lap1.columns + '_t3_lap1'
    dfControlT3_lap2.columns = dfControlT3_lap2.columns + '_t3_lap2'
    dfControlT3_lap3.columns = dfControlT3_lap3.columns + '_t3_lap3'

    #merge dataframes into one based on subject IDs
    #can probably use a function here
    dfControlFinal = dfControlT1_lap1.merge(dfControlT1_lap2,
                        left_on='subID_t1_lap1',
                        right_on='subID_t1_lap2'
                        )
    dfControlFinal = dfControlFinal.merge(dfControlT1_lap3,
                        left_on='subID_t1_lap2',
                        right_on='subID_t1_lap3'
                        )

    dfControlFinal = dfControlFinal.merge(dfControlT2_lap1,
                        left_on='subID_t1_lap3',
                        right_on='subID_t2_lap1'
                        )
    dfControlFinal = dfControlFinal.merge(dfControlT2_lap2,
                        left_on='subID_t2_lap1',
                        right_on='subID_t2_lap2'
                        )
    dfControlFinal = dfControlFinal.merge(dfControlT2_lap3,
                        left_on='subID_t2_lap2',
                        right_on='subID_t2_lap3'
                        )

    dfControlFinal = dfControlFinal.merge(dfControlT3_lap1,
                        left_on='subID_t2_lap3',
                        right_on='subID_t3_lap1'
                        )
    dfControlFinal = dfControlFinal.merge(dfControlT3_lap2,
                        left_on='subID_t3_lap1',
                        right_on='subID_t3_lap2'
                        )
    dfControlFinal = dfControlFinal.merge(dfControlT3_lap3,
                        left_on='subID_t3_lap2',
                        right_on='subID_t3_lap3'
                        )
                        
    #return control dataframe in long format
    return dfControlFinal.to_csv('Control.csv', index=False)
