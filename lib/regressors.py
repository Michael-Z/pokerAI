import os
import json
import pandas as pd
from sklearn.cross_validation import train_test_split, KFold
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import mean_absolute_error
import numpy as np
from datetime import datetime

# models to try
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.ensemble import AdaBoostRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.linear_model import BayesianRidge
from sklearn.linear_model import Lasso
from sklearn.linear_model import LinearRegression

print "Start time:", datetime.now()

# start CSV for results
with open('../report/data/regressorResults.csv','w') as f:
    header = ['Classifier','TrainMAE','ValidMAE','AvgPredictTime']
    f.write(','.join(header))

# load data
testing = True
os.chdir('../data/test') if testing else os.chdir('../data/full')
with open('../util/FeatureSets.json') as json_data:
    fs = json.load(json_data)

# useful list for changing string to int
actions = ['None','deadblind','blind','fold','check','call','bet','raise']

# initialize
# create regressor subsets
if not os.listdir('data_engineered/subsets/regressor'):
    os.chdir('data_engineered/subsets/classifier')
    for f in os.listdir(os.getcwd()):
        if f[-4:]=='True':
            command = r"""
            awk -F ',' 'BEGIN {{OFS=","}} {{ if ($(NF-1) == "\"raise\"" && $(NF-3) == 0) print }}' {0} > ../regressor/{0}
            """.format(f).strip()
        else:
            command = r"""
            awk -F ',' 'BEGIN {{OFS=","}} {{ if ($(NF-1) == "\"bet\"" && $(NF-3) == 0) print }}' {0} > ../regressor/{0}
            """.format(f).strip()
        os.system(command)
        
    os.chdir('../../..')
    
os.chdir('data_engineered/subsets/regressor')

subset = 'River-False'
poker = pd.read_csv('{}.csv'.format(subset), 
                    header=None, names=fs[subset],
                    nrows=500000)
    
# prep for classifiers
def prepPoker(poker, isFB):
    # string feature to dummy
    poker = poker.join(pd.get_dummies(poker.LastAction))
    poker.drop('LastAction', axis=1, inplace=True)
    
    # subset down to players with at least minActs num of hands played
    minActs = 50
    actionsByPlayer = list(poker.groupby('Player'))
    poker = pd.concat([a[1] for a in actionsByPlayer if a[1].shape[0]>=minActs])
    
    # get only relevant actions    
    if isFB:
        poker = poker.ix[poker.Action=='raise']
        poker.drop('IsAgg', axis=1, inplace=True)
    else:
        poker = poker.ix[poker.Action=='bet']
        
    # remove allin bets
    poker = poker.ix[poker.AllIn==0]
    
    # and bets that are abnormally large
    poker = poker.ix[poker.Amount_rp<2.5]
    
    # remove unnecessary columns
    poker.drop(['Player','Action','AllIn',
                'BetsRaisesP','BetsRaisesF','BetsRaisesT','BetsRaisesR',
                'HoleCard1','HoleCard2'], axis=1, inplace=True)
    
    return poker

isFB = subset[-4:]=='True'
poker = prepPoker(poker, isFB)

# separate targets and features
labels = poker.pop('Amount_rp')

# split train and test
X_train,X_test,y_train,y_test = train_test_split(poker,labels, test_size=0.3)

for rgr in [DecisionTreeRegressor(),
            ExtraTreesRegressor(),
            AdaBoostRegressor(DecisionTreeRegressor(max_depth=2)),
            RandomForestRegressor(),
            Ridge(),
            BayesianRidge(),
            Lasso(),
            LinearRegression()
            ]:

    # cross validate
    metrics = {'TrainingMAEs': [], 'ValidMAEs': [], 'PredictTimes': []}
    for train,valid in KFold(len(y_train), n_folds=10):
        # split data
        X_minitrain = X_train.iloc[train]
        y_minitrain = y_train.iloc[train]
        X_validate = X_train.iloc[valid]
        y_validate = y_train.iloc[valid]
        
        # train and predict
        rgr.fit(X_minitrain, y_minitrain)
        
        startTime = datetime.now()
        trPreds = rgr.predict(X_minitrain)
        vPreds = rgr.predict(X_validate)
        predictTime = (datetime.now() - startTime).total_seconds()
        
        # get metrics
        trainMAE = mean_absolute_error(y_minitrain, trPreds)
        validMAE = mean_absolute_error(y_validate, vPreds)
        
        metrics['PredictTimes'].append(predictTime)
        metrics['TrainingMAEs'].append(trainMAE)
        metrics['ValidMAEs'].append(validMAE)
        
    # get regressor name
    rgrName = str(rgr)
    rgrName = rgrName[:rgrName.find('(')]
    
    # write metrics to CSV
    newRow = '{},{},{},{}'.format(rgrName, np.mean(metrics['TrainingMAEs']),
                np.mean(metrics['ValidMAEs']), np.mean(metrics['PredictTimes']))
    with open('../../../../../report/data/regressorResults.csv','ab') as f:
        f.write('\n' + newRow)
        
    print "Finished algorithm {} at {}".format(rgrName, datetime.now())
   
'''
CHOICE: RANDOM FOREST
'''

# tune [choice] for every dataset
gsTable = []
scores = {}

for df in ['Preflop-True','Flop-False','Flop-True','Turn-False','Turn-True',
           'River-False','River-True']:
               
    isFB = df[-4:]=='True'
    
    poker = pd.read_csv('{}.csv'.format(df), 
                        header=None, names=fs[df],
                        nrows=500000)
    poker = prepPoker(poker, isFB)
    labels = poker.pop('Amount_rp')
                        
    X_train,X_test,y_train,y_test = train_test_split(poker, labels, test_size=0.3)
               
    rf = RandomForestRegressor()
    
    # grid search
    parameters = {'n_estimators':[10,20,40,80,200],
                  'bootstrap':[True,False],
                  'max_features':[30,50,None]
                 }
    rgr = GridSearchCV(rf, parameters, scoring='mean_absolute_error')
    rgr.fit(X_train, y_train)
    params = rgr.best_params_
    rgrBest = rgr.best_estimator_
    
    # fit and predict on testing
    finalPreds = rgrBest.predict(X_test)
    finalScore = mean_absolute_error(y_test, finalPreds)
    
    params['Dataset'] = df
    gsTable.append(params)
    
    scores[df] = finalScore
    
    print "Finished {} at {}".format(df, datetime.now())
    
pd.DataFrame(gsTable).to_csv('../../../../report/data/regressorGridSearch.csv',index=False,
                            columns=['Dataset']+parameters.keys())

print "Individual MAE scores:\n", \
        '\n'.join('-'.join([k,v]) for k,v in scores.iteritems())
print "Average MAE score:", np.mean(scores.values())