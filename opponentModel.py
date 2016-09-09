import os
import json
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.cross_validation import train_test_split

# models to try
# comment is model's un-tuned accuracy on all data
#from sklearn.ensemble import AdaBoostClassifier # 0.813 (DecisionTree depth 2)
from sklearn.ensemble import GradientBoostingClassifier # 0.822
#from sklearn.ensemble import BaggingClassifier # 0.814
#from sklearn.linear_model import LogisticRegression # 0.814
#from sklearn.naive_bayes import GaussianNB # 0.810

# load data
os.chdir('samplefeatures/subsets')
with open('FeatureSets.json') as json_data:
    fs = json.load(json_data)

# useful list for changing string to int
actions = ['deadblind','blind','fold','check','call','bet','raise']

####################### FIND BEST K, OUTPUT MODELS ############################
accsByK = {}
for k in range(3,9):
    # re-read in data (has been deleted for memory)
    poker_c = pd.read_csv('River-True.csv', header=None, names=fs['River-True'])
    
    #### CLUSTERING ####
    # separate features for clustering from features for prediction
    clusteringFeatures = ['Player','AllFold', 
              'AllCheck', 'AllCall', 'AllBet', 'AllRaise', 'PreflopFoldPct', 
              'PreflopCheckPct', 'PreflopCallPct', 'PreflopBetPct', 'PreflopRaisePct',
              'FlopFoldPct', 'FlopCheckPct', 'FlopCallPct', 'FlopBetPct', 'FlopRaisePct', 
              'TurnFoldPct', 'TurnCheckPct', 'TurnCallPct', 'TurnBetPct', 'TurnRaisePct', 
              'RiverFoldPct', 'RiverCheckPct', 'RiverCallPct', 'RiverBetPct', 
              'RiverRaisePct', 'VPIP', 'ThreeBetPct', 'SeeSDPct',
              'WinWhenSeeFlopPct', 'WinWithoutSDPct', 'WinAtSDPct', 'ContBetPct',
              'BetRiverPct', 'CallOrRaisePFRPct', 'FoldToCBetPct', 'CallCBetPct', 
              'RaiseCBetPct', 'FoldToFlopBetPct', 'CallFlopBetPct', 'RaiseFlopBetPct']
    
    # subset down to players with at least minActs num of hands played
    minActs = 50
    poker_c = poker_c.ix[:,clusteringFeatures]
    actionsByPlayer = list(poker_c.groupby('Player'))
    poker_c = pd.concat([a[1] for a in actionsByPlayer if a[1].shape[0]>=minActs])
    del actionsByPlayer
    
    # filter to unique players
    poker_c.drop_duplicates(inplace=True)
    players = poker_c.pop('Player')
    
    # run k-means, store results
    cls = KMeans(k)
    cls.fit(poker_c)
    playerClusters = cls.labels_
    
    #### PREDICTING ####
    # get data
    del poker_c
    poker_p = pd.read_csv('River-True.csv', header=None, names=fs['River-True'])
    
    # string feature to int
    poker_p['LastAction'] = [actions.index(a) for a in poker_p.LastAction]
    
    # subset down to players with at least minActs num of hands played
    minActs = 50
    actionsByPlayer = list(poker_p.groupby('Player'))
    poker_p = pd.concat([a[1] for a in actionsByPlayer if a[1].shape[0]>=minActs])
    del actionsByPlayer
    
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
        clf = GradientBoostingClassifier()
        X_train,X_test,y_train,y_test = clusterDFs[i]
        clf.fit(X_train, y_train)
        preds = clf.predict(X_test)
        totalRight.append(float((preds==y_test).sum()))
        totalPreds.append(len(y_test))
        
    accs = [r/p for r,p in zip(totalRight,totalPreds)]
    totalAcc = sum(totalRight) / sum(totalPreds)
    print "K = {}".format(k)
    print "accuracies of cluster models:", ','.join([str(round(i,3)) for i in accs])
    print "total accuracy on all data:", totalAcc
    accsByK[k] = totalAcc