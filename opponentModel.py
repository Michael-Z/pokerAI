import MySQLdb
import MySQLdb.cursors
import pandas as pd
import numpy as np
import random
from sklearn.cluster import KMeans
from sklearn.cross_validation import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier

# connect to database
with open('pwd.txt') as f:
    pwd = f.read().strip()
db = MySQLdb.connect(host='localhost',port=3307,user='ntaylorwss',passwd=pwd)
cur = db.cursor()
cur.execute('USE pokersample;')

allCols = set(pd.read_sql_query("DESCRIBE features;", db).Field)
allCols -= {'ActionID','Round','FacingBet'}

# useful list for changing string to int
actions = ['deadblind','blind','fold','check','call','bet','raise']

# separate features for clustering from features for prediction
clusteringFeatures = {'Player','AllFold', 
          'AllCheck', 'AllCall', 'AllBet', 'AllRaise', 'PreflopFoldPct', 
          'PreflopCheckPct', 'PreflopCallPct', 'PreflopBetPct', 'PreflopRaisePct',
          'FlopFoldPct', 'FlopCheckPct', 'FlopCallPct', 'FlopBetPct', 'FlopRaisePct', 
          'TurnFoldPct', 'TurnCheckPct', 'TurnCallPct', 'TurnBetPct', 'TurnRaisePct', 
          'RiverFoldPct', 'RiverCheckPct', 'RiverCallPct', 'RiverBetPct', 
          'RiverRaisePct', 'VPIP', 'ThreeBetPct', 'SeeSDPct',
          'WinWhenSeeFlopPct', 'WinWithoutSDPct', 'WinAtSDPct', 'ContBetPct',
          'BetRiverPct', 'CallOrRaisePFRPct', 'FoldToCBetPct', 'CallCBetPct', 
          'RaiseCBetPct', 'FoldToFlopBetPct', 'CallFlopBetPct', 'RaiseFlopBetPct'}

#### CLUSTERING ####
# get data
q = '''SELECT {} FROM features 
        WHERE Action!="blind" AND Action!="deadblind"
                AND Round="river" AND FacingBet=1
        ;'''.format(
        ','.join(clusteringFeatures))
allPoker_c = pd.read_sql_query(q,db)

'''
# get data (from CSV)
allPoker = pd.read_csv('samplefeatures/subsets/River-True.csv')
allPoker_c = allPoker[clusteringFeatures]
'''

# subset down to players with at least minActs num of hands played
minActs = 50
actionsByPlayer = list(allPoker_c.groupby('Player'))
poker_c = pd.concat([a[1] for a in actionsByPlayer if a[1].shape[0]>=minActs])

# filter to unique players
poker_c.drop_duplicates(inplace=True)
players = poker_c.pop('Player')

# run k-means, store results
k = 4
cls = KMeans(k)
cls.fit(poker_c)
playerClusters = cls.labels_

#### PREDICTING ####
# get data
q = '''SELECT {} FROM features 
        WHERE Action!="blind" AND Action!="deadblind"
                AND Round="river" AND FacingBet=1
        ;'''.format(
        ','.join(allCols))
allPoker_p = pd.read_sql_query(q,db)

'''
# get data (from CSV)
allPoker = pd.read_csv('samplefeatures/subsets/River-True.csv')
allPoker_c = allPoker[allCols]
'''

# string feature to int
allPoker_p['LastAction'] = [actions.index(a) for a in allPoker_p.LastAction]

# subset down to players with at least minActs num of hands played
minActs = 50
actionsByPlayer = list(allPoker_p.groupby('Player'))
poker_p = pd.concat([a[1] for a in actionsByPlayer if a[1].shape[0]>=minActs])

# randomly select subset of features
poker_p = poker_p

# separate allPoker_p into clusters
clusterDFs = []
for i in range(k):
    playersInCluster = [p for j,p in enumerate(players) if playerClusters[j]==i]
    X = pd.DataFrame(poker_p.ix[poker_p.Player.isin(playersInCluster)])
    X.drop('Player', axis=1, inplace=True)
    y = X.pop('Action')
    splitData = train_test_split(X,y, test_size=0.2)
    clusterDFs.append(splitData)

# train and test each cluster's model
totalRight = []
totalPreds = []
for i in range(k):
    clf = RandomForestClassifier()
    X_train,X_test,y_train,y_test = clusterDFs[i]
    clf.fit(X_train, y_train)
    preds = clf.predict(X_test)
    totalRight.append(float((preds==y_test).sum()))
    totalPreds.append(len(y_test))
    
accs = [r/p for r,p in zip(totalRight,totalPreds)]
totalAcc = sum(totalRight) / sum(totalPreds)
print "accuracies of cluster models:", ','.join([str(round(i,3)) for i in accs])
print "total accuracy on all data:", totalAcc
