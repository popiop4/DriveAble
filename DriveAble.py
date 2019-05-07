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
        name = re.sub('.\d', '', name)
        name = re.sub('\d', '', name)
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
    'sex' not in column):
      cols.append(column)

  df=df[cols]
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

  dfClientsFinal.to_csv('dfClientsFinal.csv',index=False)

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

  dfDemosFinal.to_csv('dfDemosFinal.csv',index=False)

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

  dfControlFinal.to_csv('dfControlFinal.csv',index=False) #output csv

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

  dfJudgementFinal.to_csv('dfJudgementFinal.csv',index=False)

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

  dfMemoryFinal.to_csv('dfMemoryFinal.csv',index=False)

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

  dfReactionFinal.to_csv('dfReactionFinal.csv',index=False)

  return(dfReactionFinal)

def collisionLong(xl):
  dfCollision= xl.parse('Control Collision')

  dfCollision = dfCollision[(~dfCollision['identification_id'].str.contains('demo')) 
                      & (~dfCollision['identification_id'].str.contains('test'))]

  ctrlIDs = []
  lastID = ''
  lastCtrl = 0
  newCtrl = 1

  for ctrl, fullID in zip(dfCollision['ctrl_id'], dfCollision['identification_id']):
    if(fullID != lastID):
      newCtrl = 1
      ctrlIDs.append(newCtrl)
    elif(fullID == lastID and ctrl != lastCtrl):
      newCtrl = newCtrl + 1
      ctrlIDs.append(newCtrl)
    else:
      ctrlIDs.append(newCtrl)
    lastID = fullID
    lastCtrl = ctrl

  dfCollision['new_ctrl_id']=ctrlIDs

  dfCollision = splitIDs(dfCollision)

  dfCollisionFinal = dfCollision.pivot_table(index=['subID'],
                           columns=['TP','new_ctrl_id','trial'],
                            aggfunc='first'
                           )#.reset_index()

  dfCollisionFinal.to_csv('dfCollisionFinal.csv',index=True)

  dfCollisionFinal = pd.read_csv('dfCollisionFinal.csv')

  dfCollisionFinal = smushNames(dfCollisionFinal, 'collision')

  dfCollisionFinal = dfCollisionFinal.drop([0,1,2,3])

  dfCollisionFinal = dropColumns(dfCollisionFinal)

  dfCollisionFinal.to_csv('dfCollisionFinal.csv',index=False)

  return dfCollisionFinal

def main():
  file = 'drive.xlsx'
  xl = pd.ExcelFile(file)
  
  control = controlLong(xl)
  judgement = judgementLong(xl)
  memory = memoryLong(xl)
  reaction = reactionLong(xl)
  demo = demoLong(xl)
  client = clientLong(xl)
  collision = collisionLong(xl)

  dfs = [client, demo, control, collision, judgement, memory, reaction]

  dfFinal = reduce(lambda left,right: pd.merge(left,right,on='subID'), dfs) #merge all dataframes

  dfFinal.to_csv('dfFinal.csv',index=False)

main()



