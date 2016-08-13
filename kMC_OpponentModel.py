import MySQLdb
import MySQLdb.cursors
import pandas as pd
import numpy as np
import random
from sklearn.naive_bayes import GaussianNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import train_test_split
from sklearn.metrics import accuracy_score

# connect to database
with open('pwd.txt') as f:
    pwd = f.read().strip()
db = MySQLdb.connect(host='localhost',port=3307,user='ntaylorwss',passwd=pwd)
cur = db.cursor()
cur.execute('USE pokersample;')
cols = list(pd.read_sql_query("DESCRIBE features;", db).Field)

# test on river-facingbet subset
q = '''SELECT {} FROM features 
        WHERE Action!="blind" AND Action!="deadblind"
                AND Round="river" AND FacingBet=1
        ;'''.format(
        ','.join([cols[i] for i in [0]+range(3,len(cols)-1)]))
allPoker = pd.read_sql_query(q,db)

# subset down to players with at least minActs num of hands played
minActs = 50
actionsByPlayer = list(allPoker.groupby('Player'))
poker = pd.concat([a[1] for a in actionsByPlayer if a[1].shape[0]>=minActs])
players = list(poker.Player.unique())

# parameters
k = 4
testSize = 0.2

# initialize player clusters
clusters = [i%k for i in range(len(players))]
random.shuffle(clusters)

# string Actions to numeric
actions = ['blind','deadblind','fold','check','call','bet','raise']
poker['Action'] = [actions.index(a) for a in poker.Action]
poker['LastAction'] = [actions.index(a) for a in poker.LastAction]

def splitData(poker, testSize):
    """
    parameters:
    data - the data to be split
    returns:
    dataByPlayer - {player: [X_train,X_test,y_train,y_test]}
    """
    dataByPlayer = {}
    players = poker.Player.unique()
    for p in players:
        playerData = poker.ix[poker.Player==p]
        playerX = playerData.drop(['Action','Player'],axis=1)
        playerY = playerData.Action
        dataByPlayer[p] = train_test_split(playerX, playerY, test_size=testSize)
    
    return dataByPlayer

def trainModel(i):
    # grab data
    playersInCluster = [players[j] for j in range(len(players))
                        if clusters[j]==i]
    X_train = pd.concat([dataByPlayer[p][0] for p in playersInCluster])
    y_train = pd.concat([dataByPlayer[p][2] for p in playersInCluster])
    
    # initialize model
    model = DecisionTreeClassifier()
    
    # fit model to data
    model.fit(X_train,y_train)
    
    # return model
    return model
    
def loadTestData(p):
    return [dataByPlayer[p][i] for i in [1,3]]

def testModel(m, d):
    X_test,y_test = d
    preds = m.predict(X_test)
    return accuracy_score(y_test, preds)    

# split up data, to be accessed when training models
dataByPlayer = splitData(poker, testSize)

# train models
prevClusters = []
#while clusters!=prevClusters:
for i in range(5):
    prevClusters = list(clusters)
    models = []
    for i in range(k):
        models.append(trainModel(i))
    for j,p in enumerate(players):
        testData = loadTestData(p)
        likelihoods = []
        for i in range(k):
            likelihoods.append(testModel(models[i], testData))
        clusters[j] = np.argmax(likelihoods)

for i in range(k):
    playersInCluster = [p for j,p in enumerate(players) if clusters[j]==i]
    X_train,X_test,y_train,y_test = [pd.concat([dataByPlayer[p][ii] 
                                                for p in playersInCluster])
                                    for ii in range(4)]
    model = RandomForestClassifier()
    model.fit(X_train,y_train)
    
    print accuracy_score(y_test, model.predict(X_test))