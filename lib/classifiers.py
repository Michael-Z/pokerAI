import os
import json
import pandas as pd
from sklearn.cross_validation import train_test_split, StratifiedKFold
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import f1_score, confusion_matrix
import numpy as np
from datetime import datetime

# models to try
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB

print "Start time:", datetime.now()

# start CSV for results
with open('../report/data/classifierResults.csv','w') as f:
    header = ['Classifier','TrainF1','ValidF1','AvgPredictTime']
    f.write(','.join(header))

# load data
testing = True
os.chdir('../data/test') if testing else os.chdir('../data/full')
with open('../util/FeatureSets.json') as json_data:
    fs = json.load(json_data)

# useful list for changing string to int
actions = ['None','deadblind','blind','fold','check','call','bet','raise']

# prep for classifiers
def prepPoker(poker, rd, isFB):
    # string feature to dummy
    poker = poker.join(pd.get_dummies(poker.LastAction))
    poker.drop('LastAction', axis=1, inplace=True)
    
    # subset down to players with at least minActs num of hands played
    minActs = 50
    actionsByPlayer = list(poker.groupby('Player'))
    poker = pd.concat([a[1] for a in actionsByPlayer if a[1].shape[0]>=minActs])
    
    # remove all-in bets/raises
    poker = poker.ix[poker.AllIn==0]
    
    if isFB:
        poker = poker.ix[poker.Action.isin(['fold','call','raise'])]
        poker.drop('IsAgg', axis=1, inplace=True)
    else:
        poker = poker.ix[poker.Action.isin(['check','bet'])]
        
    poker.drop(['Player','Amount_rp','AllIn',
                'BetsRaisesP','BetsRaisesF','BetsRaisesT','BetsRaisesR',
                'HoleCard1','HoleCard2'],
                axis=1, inplace=True)
    
    return poker

#initialize
subset = 'Flop-True'
poker = pd.read_csv('data_engineered/subsets/classifier/{}.csv'.format(subset), 
                    header=None, names=fs[subset],
                    nrows=500000)

isFB = subset[-4:]=='True'
poker = prepPoker(poker, subset[:subset.find('-')], isFB)

# separate targets and features
labels = poker.pop('Action')

# split train and test
X_train,X_test,y_train,y_test = train_test_split(poker,labels, test_size=0.3)

for clf in [DecisionTreeClassifier(),
            AdaBoostClassifier(DecisionTreeClassifier(max_depth=2)),
            GradientBoostingClassifier(),
            RandomForestClassifier(),
            LogisticRegression(),
            GaussianNB()
            ]:

    # cross validate
    metrics = {'TrainingF1s': [], 'ValidF1s': [], 'PredictTimes': []}
    for train,valid in StratifiedKFold(y_train, n_folds=10):
        # split data
        X_minitrain = X_train.iloc[train]
        y_minitrain = y_train.iloc[train]
        X_validate = X_train.iloc[valid]
        y_validate = y_train.iloc[valid]
        
        # train and predict
        clf.fit(X_minitrain, y_minitrain)
        
        startTime = datetime.now()
        trPreds = clf.predict(X_minitrain)
        vPreds = clf.predict(X_validate)
        predictTime = (datetime.now() - startTime).total_seconds()
        
        # get metrics
        if isFB:
            trainF1 = f1_score(y_minitrain, trPreds, average='weighted')
            validF1 = f1_score(y_validate, vPreds, average='weighted')
        else:
            trainF1 = f1_score(y_minitrain, trPreds, pos_label='bet')
            validF1 = f1_score(y_validate, vPreds, pos_label='bet')
        
        metrics['PredictTimes'].append(predictTime)
        metrics['TrainingF1s'].append(trainF1)
        metrics['ValidF1s'].append(validF1)
        
    # get classifier name
    clfName = str(clf)
    clfName = clfName[:clfName.find('(')]
    
    # write metrics to CSV
    newRow = '{},{},{},{}'.format(clfName, np.mean(metrics['TrainingF1s']),
                np.mean(metrics['ValidF1s']), np.mean(metrics['PredictTimes']))
    with open('../../report/data/classifierResults.csv','ab') as f:
        f.write('\n' + newRow)
        
    print "Finished algorithm {} at {}".format(clfName, datetime.now())
    
'''
CHOICE: RANDOM FOREST
'''

# tune random forest for every dataset
gsTable = []
fullConfusion = np.zeros((5,5))
indF1s = {}

for df in ['Preflop-True','Flop-False','Flop-True','Turn-False','Turn-True',
           'River-False','River-True']:
               
    isFB = df[-4:]=='True'
    
    poker = pd.read_csv('data_engineered/subsets/classifier/{}.csv'.format(df), 
                        header=None, names=fs[df],
                        nrows=500000)
    poker = prepPoker(poker, isFB)
    labels = poker.pop('Action')
                        
    X_train,X_test,y_train,y_test = train_test_split(poker, labels, test_size=0.3)
               
    lr = LogisticRegression()
    
    # grid search
    parameters = {'n_estimators':[10,20,40,80,200],
                  'bootstrap':[True,False],
                  'max_features':[30,50,None]
                  }
    clf = GridSearchCV(lr, parameters, scoring='f1_weighted')
    clf.fit(X_train, y_train)
    params = clf.best_params_
    clfBest = clf.best_estimator_
    
    # fit and predict on testing
    finalPreds = clfBest.predict(X_test)
    
    # add to confusion matrix for overall F1, also report individual F1
    if isFB:
        f1 = f1_score(y_test, finalPreds, average='weighted')
    else:
        f1 = f1_score(y_test, finalPreds, pos_label='bet')
    finalConfusion = confusion_matrix(y_test, finalPreds)
    inds = [0,2,4] if isFB else [1,3]
    for i,row in zip(inds, finalConfusion): fullConfusion[i,inds] = row
    
    params['Dataset'] = df
    gsTable.append(params)
    indF1s[df] = f1
    
    print "Finished {} at {}".format(df, datetime.now())
    
pd.DataFrame(gsTable).to_csv('../../report/data/classifierGridSearch.csv',index=False,
                            columns=['Dataset']+parameters.keys())
                            
def F1fromConfusion(confusion):
    allF1scores = []
    allProportions = []
    for i in range(confusion.shape[0]):
        # get 2x2 confusion matrix
        newConfusion = np.zeros((2,2))
        newConfusion[0,0] = confusion[i,i] # TP
        newConfusion[1,0] = confusion[:,i].sum() - confusion[i,i] # FP
        newConfusion[0,1] = confusion[i,:].sum() - confusion[i,i] # FN
        newConfusion[1,1] = confusion.sum() - newConfusion.sum() # TN
        # get F1 from newConfusion
        precision = newConfusion[0,0] / newConfusion[:,0].sum()
        recall = newConfusion[0,0] / newConfusion[0,:].sum()
        f1 = 2 * (precision * recall) / (precision + recall)
        allF1scores.append(f1)
        # get proportion
        prop = confusion[i,:].sum() / confusion.sum()
        allProportions.append(prop)
        
    return np.dot(allF1scores, allProportions)

print "Individual F1 scores are:\n", \
        '\n'.join('{}-{}'.format(k,v) for k,v in indF1s.iteritems())
print "\nFinal F1 score is:", F1fromConfusion(fullConfusion)
