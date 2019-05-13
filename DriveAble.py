import pandas as pd
import re
from functools import reduce


def splitIDs(df):
  #split identification_id into subject id and timepoint
  #return dataframe with new columns for subject id and timepoint
  ids_with_tp = df['identification_id']
  ids = []
  tps = []

  for fullID in ids_with_tp:
    ids.append(fullID[:4])
    tps.append(fullID[5:].upper().strip())
    
  df['subID'] = ids
  df['TP'] = tps
    
  return df


def smushNames(df, sheet):
  #combine rows of column names from pivot_table dataframe to create unique column names 
  #returns dataframe with new column names
  index = 0
  oldNames = df.columns
  newNames = []
    
  #control sheet
  if (sheet == 'control'):
    for name in oldNames:
      if (index == 0):
        newNames.append('subID')
      else:
        name = re.sub('.\d', '', name)
        name = re.sub('\d', '', name)
        new = name + '_' + df.iloc[0, index] + '_lap' + df.iloc[1, index]
        newNames.append(new)
      index = index + 1

  #judgement, memory, reaction sheets
  if (sheet == 'judgement' or sheet == 'memory' or sheet == 'reaction'):
    for name in oldNames:
      if (index == 0):
        newNames.append('subID')
      else:
        if ('gap' not in name):
          name = re.sub('.\d', '', name)
          name = re.sub('\d', '', name)
        elif (name != 'gap_1_size' and name != 'gap_2_size'):
          name = re.sub('.\d*$', '', name)
        new = name + '_' + df.iloc[0, index] + '_stage' + df.iloc[1, index] + '_trial' + df.iloc[2, index]
        newNames.append(new)
      index = index + 1
  
  #demo sheet
  if (sheet == 'demo'):
    for name in oldNames:
      if (index == 0):
        newNames.append('subID')
      else:
        name = re.sub('.\d', '', name)
        name = re.sub('\d', '', name)
        new = name + '_' + df.iloc[0, index] + '_' + df.iloc[1, index] + '_demonum' + df.iloc[2, index]
        newNames.append(new)
      index = index + 1

  #client sheet
  if (sheet == 'clients'):
    for name in oldNames:
      if (index == 0):
        newNames.append('subID')
      else:
        name = re.sub('.\d', '', name)
        name = re.sub('\d', '', name)
        new = name + '_' + df.iloc[0, index]
        newNames.append(new)
      index = index + 1

    #collision sheet
  if (sheet == 'collision'):
    for name in oldNames:
      if (index == 0):
        newNames.append('subID')
      else:
        name = re.sub('.\d', '', name)
        name = re.sub('\d', '', name)
        new = name + '_' + df.iloc[0, index] + '_ctrl' + df.iloc[1, index] + '_trial' + df.iloc[2, index]
        newNames.append(new)
      index = index + 1 

  df.columns = newNames
  return df

def dropColumns(df):
  #drop redundant columns
  cols = []

  for column in df.columns:
    if ('organization_name' not in column and
    'configuration_id' not in column and
    'evaluation_id' not in column and
    'identification_id' not in column and
    'examiner_id' not in column and
    'birth_date' not in column and
    'assessment_time_start' not in column and
    'sex' not in column and
    'data_source' not in column and
    'care_srv' not in column):
      cols.append(column)

  df=df[cols]
  return df


def checkForDups(xl):
  #looks for duplicate id/tp combinations
  #outputs csv of duplicates
  #returns list of duplicate ids

  df = xl.parse('Clients')

  df['identification_id'] = df['identification_id'].str.lower()

  df = df[(~df['identification_id'].str.contains('demo')) 
                    & (~df['identification_id'].str.contains('test'))]

  df = splitIDs(df)

  dups = df.duplicated(subset='identification_id', keep=False)

  dfDupsFinal = df[dups]

  dfDupsFinal.to_csv('dfDupsFinal.csv',index=False) #output csv of duplicates

  duplicates = df[dups]['subID']

  duplicateIDs = df[dups]['subID'].values

  uniqueDuplicateIDs = [] 

  for thing in duplicateIDs: 
  	if thing not in uniqueDuplicateIDs: 
  		uniqueDuplicateIDs.append(thing) 

  return uniqueDuplicateIDs


def finalNames(df, sheet):
  #adds task/tab names to the front of the column names so there will be no duplicates
  #in the final merge
  index = 0
  oldNames = df.columns
  newNames = []
    
  for name in oldNames:
    if (index == 0):
      newNames.append('subID')
    else:
      new = sheet + '_' + name
      newNames.append(new)
    index = index + 1

  df.columns = newNames
  return df


def clientLong(xl):

  dfClients = xl.parse('Clients')

  dfClients = dfClients[(~dfClients['identification_id'].str.contains('demo')) 
                      & (~dfClients['identification_id'].str.contains('test'))]

  dfClients = splitIDs(dfClients)

  dfClientsFinal = dfClients.pivot_table(index=['subID'],
                           columns=['TP'],
                           #aggfunc=lambda x: ' !!!!!!!!! '.join(x)
                           aggfunc='first'
                           )#.reset_index()

  dfClientsFinal.to_csv('dfClientsFinal.csv',index=True)

  dfClientsFinal = pd.read_csv('dfClientsFinal.csv')

  dfClientsFinal = smushNames(dfClientsFinal, 'clients')

  dfClientsFinal = dfClientsFinal.drop([0,1])

  #dfClientsFinal.to_csv('dfClientsFinal.csv',index=False)

  return(dfClientsFinal)


def demoLong(xl):

  dfDemos= xl.parse('Demos')

  dfDemos = dfDemos[(~dfDemos['identification_id'].str.contains('demo')) 
                  & (~dfDemos['identification_id'].str.contains('test'))]
  
  dfDemos = splitIDs(dfDemos)

  dfDemosFinal = dfDemos.pivot_table(index=['subID'],
                           columns=['TP', 'task', 'demonum'],
                           #aggfunc=lambda x: ' !!!!!!!!! '.join(x)
                           aggfunc='first'
                           )#.reset_index()

  dfDemosFinal.to_csv('dfDemosFinal.csv',index=True)

  dfDemosFinal = pd.read_csv('dfDemosFinal.csv')

  dfDemosFinal = smushNames(dfDemosFinal, 'demo')

  dfDemosFinal = dfDemosFinal.drop([0,1,2,3])

  dfDemosFinal = dropColumns(dfDemosFinal)

  #dfDemosFinal.to_csv('dfDemosFinal.csv',index=False)

  return(dfDemosFinal)


def controlLong(xl):
  #outputs long format control sheet to csv

  dfControl= xl.parse('Control') #pull sheet into dataframe

  dfControl = dfControl[(~dfControl['identification_id'].str.contains('demo')) 
                      & (~dfControl['identification_id'].str.contains('test'))] #remove demos

  dfControl = splitIDs(dfControl) #add subID and TP columns

  dfControlFinal = dfControl.pivot_table(index=['subID'], 
                           columns=['TP','lap_number'],
                           #aggfunc=lambda x: ' !!!!!!!!! '.join(x)
                           aggfunc='first'
                           )#.reset_index()

  dfControlFinal.to_csv('dfControlFinal.csv',index=True) #put into csv
  
  dfControlFinal = pd.read_csv('dfControlFinal.csv') #read csv into dataframe to loses multiindex, not sure how else to do this

  dfControlFinal = smushNames(dfControlFinal, 'control') #update column names

  dfControlFinal = dfControlFinal.drop([0,1,2]) #drop other column names

  dfControlFinal = dropColumns(dfControlFinal) #drop redundant columns

  #dfControlFinal.to_csv('dfControlFinal.csv',index=False) #output csv

  return(dfControlFinal)


def judgementLong(xl):

  dfJudgement= xl.parse('Judgement')

  dfJudgement = dfJudgement[(~dfJudgement['identification_id'].str.contains('demo')) 
                          & (~dfJudgement['identification_id'].str.contains('test'))]

  dfJudgement = splitIDs(dfJudgement)
  
  dfJudgementFinal = dfJudgement.pivot_table(index=['subID'],
                           columns=['TP','trial_stage','trial_number'],
                           #aggfunc=lambda x: ' !!!!!!!!! '.join(x)
                           aggfunc='first'
                           )#.reset_index()

  dfJudgementFinal.to_csv('dfJudgementFinal.csv',index=True)
  
  dfJudgementFinal = pd.read_csv('dfJudgementFinal.csv')

  dfJudgementFinal = smushNames(dfJudgementFinal, 'judgement')

  dfJudgementFinal = dfJudgementFinal.drop([0,1,2,3])

  dfJudgementFinal = dropColumns(dfJudgementFinal)

  #dfJudgementFinal.to_csv('dfJudgementFinal.csv',index=False)

  return(dfJudgementFinal)


def memoryLong(xl):

  dfMemory = xl.parse('Memory')

  dfMemory = dfMemory[(~dfMemory['identification_id'].str.contains('demo')) 
                    & (~dfMemory['identification_id'].str.contains('test'))]

  dfMemory = splitIDs(dfMemory)

  dfMemoryFinal = dfMemory.pivot_table(index=['subID'],
                           columns=['TP','trial_stage','trial_number'],
                           #aggfunc=lambda x: ' !!!!!!!!! '.join(x)
                           aggfunc='first'
                           )#.reset_index()

  dfMemoryFinal.to_csv('dfMemoryFinal.csv',index=True)

  dfMemoryFinal = pd.read_csv('dfMemoryFinal.csv')

  dfMemoryFinal = smushNames(dfMemoryFinal, 'memory')

  dfMemoryFinal = dfMemoryFinal.drop([0,1,2,3])

  dfMemoryFinal = dropColumns(dfMemoryFinal)

  #dfMemoryFinal.to_csv('dfMemoryFinal.csv',index=False)

  return(dfMemoryFinal)


def reactionLong(xl):
  
  dfReaction = xl.parse('Reaction Time') 

  dfReaction = dfReaction[(~dfReaction['identification_id'].str.contains('demo'))
                       & (~dfReaction['identification_id'].str.contains('test'))]
  
  dfReaction = splitIDs(dfReaction)

  dfReactionFinal = dfReaction.pivot_table(index=['subID'],
                           columns=['TP','trial_stage','trial_number'],
                           #aggfunc=lambda x: ' !!!!!!!!! '.join(x)
                           aggfunc='first'
                           )#.reset_index()

  dfReactionFinal.to_csv('dfReactionFinal.csv',index=True)
  
  dfReactionFinal = pd.read_csv('dfReactionFinal.csv')

  dfReactionFinal = smushNames(dfReactionFinal, 'reaction')

  dfReactionFinal = dfReactionFinal.drop([0,1,2,3])

  dfReactionFinal = dropColumns(dfReactionFinal)

  #dfReactionFinal.to_csv('dfReactionFinal.csv',index=False)

  return(dfReactionFinal)

def collisionLong(xl, dups):
  dfCollision= xl.parse('Control Collision')

  dfCollision = dfCollision[(~dfCollision['identification_id'].str.contains('demo')) 
                      & (~dfCollision['identification_id'].str.contains('test'))]

  dfCollision = splitIDs(dfCollision)

  dfCollision = dfCollision[~dfCollision['subID'].isin(dups)]

  counter = 0
  ctrlIDs = []
  lastID = ''
  lastCtrl = 0
  newCtrl = 1

  for ctrl, fullID in zip(dfCollision['ctrl_id'], dfCollision['identification_id']):
    if(fullID != lastID or newCtrl == 4):
      newCtrl = 1
      ctrlIDs.append(newCtrl)
      counter = 0
    elif(fullID == lastID and ctrl > lastCtrl and newCtrl != 3):
      newCtrl = newCtrl + 1
      ctrlIDs.append(newCtrl)
    else:
      ctrlIDs.append(newCtrl)
    lastID = fullID
    lastCtrl = ctrl
    counter = counter + 1

  dfCollision['new_ctrl_id']=ctrlIDs

  #dfCollision = splitIDs(dfCollision)

  dfCollisionFinal = dfCollision.pivot_table(index=['subID'],
                           columns=['TP','new_ctrl_id','trial'],
                            aggfunc='first',
                            dropna=False
                           )#.reset_index()

  dfCollisionFinal.to_csv('dfCollisionFinal.csv',index=True)

  dfCollisionFinal = pd.read_csv('dfCollisionFinal.csv')

  dfCollisionFinal = smushNames(dfCollisionFinal, 'collision')

  dfCollisionFinal = dfCollisionFinal.drop([0,1,2,3])

  dfCollisionFinal = dropColumns(dfCollisionFinal)

  #dfCollisionFinal.to_csv('dfCollisionFinal.csv',index=False)

  return dfCollisionFinal

def main():
  file = 'drive.xlsx'
  xl = pd.ExcelFile(file)
  
  dups = checkForDups(xl)
  okDups = [6020,6028]

  for thing in okDups:
    stringThing = str(thing)
    if stringThing in dups:
      dups.remove(stringThing) 

  control = controlLong(xl)
  judgement = judgementLong(xl)
  memory = memoryLong(xl)
  reaction = reactionLong(xl)
  demo = demoLong(xl)
  client = clientLong(xl)
  collision = collisionLong(xl, dups)

  dfClientsFinal = client[~client['subID'].isin(dups)]
  dfControlFinal = control[~control['subID'].isin(dups)]
  dfJudgementFinal = judgement[~judgement['subID'].isin(dups)]
  dfMemoryFinal = memory[~memory['subID'].isin(dups)]
  dfReactionFinal = reaction[~reaction['subID'].isin(dups)]
  dfDemosFinal = demo[~demo['subID'].isin(dups)]
  dfCollisionFinal = collision[~collision['subID'].isin(dups)]

  dfClientsFinal.to_csv('dfClientsFinal.csv',index=False)
  dfControlFinal.to_csv('dfControlFinal.csv',index=False)
  dfJudgementFinal.to_csv('dfJudgementFinal.csv',index=False)
  dfMemoryFinal.to_csv('dfMemoryFinal.csv',index=False)
  dfReactionFinal.to_csv('dfReactionFinal.csv',index=False)
  dfDemosFinal.to_csv('dfDemosFinal.csv',index=False)
  dfCollisionFinal.to_csv('dfCollisionFinal.csv',index=False)

  #add tab/task names to front of column names to prevent duplicates
  dfClientsFinal = finalNames(dfClientsFinal, 'clients')
  dfControlFinal = finalNames(dfControlFinal, 'control')
  dfJudgementFinal = finalNames(dfJudgementFinal, 'judgement')
  dfMemoryFinal = finalNames(dfMemoryFinal, 'memory')
  dfReactionFinal = finalNames(dfReactionFinal, 'reaction')
  dfDemosFinal = finalNames(dfDemosFinal, 'demos')
  dfCollisionFinal = finalNames(dfCollisionFinal, 'collision')

  dfs = [dfClientsFinal, dfControlFinal, dfJudgementFinal, dfMemoryFinal, dfReactionFinal, dfDemosFinal, dfCollisionFinal]

  dfFinal = reduce(lambda left,right: pd.merge(left,right,on='subID'), dfs) #merge all dataframes

  dfFinal.to_csv('dfFinal.csv',index=False)

main()



