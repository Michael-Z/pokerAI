import os
import json
import pandas as pd
from sklearn.cross_validation import train_test_split, KFold
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import mean_absolute_error
import numpy as np
from datetime import datetime
from itertools import product

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
        if f[-8:-4]=='True':
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

subset = 'Preflop-True'
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


# tune [choice] for every dataset
gsTable = []
scores = {}

for df in ['Flop-False','Flop-True','Turn-False','Turn-True',
           'River-False','River-True']:
               
    isFB = df[-4:]=='True'
    
    poker = pd.read_csv('{}.csv'.format(df), 
                        header=None, names=fs[df],
                        nrows=500000)
    poker = prepPoker(poker, isFB)
    labels = poker.pop('Amount_rp')
                        
    X_train,X_test,y_train,y_test = train_test_split(poker, labels, test_size=0.3)
               
    rf = RandomForestRegressor(n_estimators=50, n_jobs=-1)
    
    # grid search
    gsResults = []
    parameters = {
                  'max_features':[10,30,None],
        		  'max_depth':[5,10,20,40,60]
                  }
    for f,d in product(parameters['max_features'], parameters['max_depth']):
        cvScores = []
        for i in xrange(3):            
            rf = RandomForestRegressor(n_estimators=50, n_jobs=-1, verbose=9,
                                        max_features=f, max_depth=d)
            X_subtrain,X_valid,y_subtrain,y_valid = train_test_split(X_train, y_train, test_size=0.3)        
            rf.fit(X_subtrain, y_subtrain)
            cvScores.append(mean_absolute_error(y_valid, rf.predict(X_valid)))
        result = {'max_features':f, 'max_depth':d, 'score':float(sum(cvScores)) / len(cvScores)}
        gsResults.append(result)
                
        print """
        \n\n\n\n\n\n\n
        Completed randomforest with f={} and d={}, got {}
        """.format(f,d,result['score'])
        
    params = sorted(gsResults, key = lambda x: x['score'])[-1]
    params.pop('score')
    params['n_estimators'] = 200
    params['n_jobs'] = -1
    
    rgrBest = RandomForestRegressor(**params)
    
    # fit and predict on testing
    rgrBest.fit(X_train, y_train)
    finalPreds = rgrBest.predict(X_test)
    finalScore = mean_absolute_error(y_test, finalPreds)
    
    params['Dataset'] = df
    gsTable.append(params)
    
    scores[df] = finalScore
    
    print "Finished {} at {}".format(df, datetime.now())
    
pd.DataFrame(gsTable).to_csv('../../../../../report/data/regressorGridSearch.csv',index=False,
                            columns=['Dataset']+parameters.keys())

allMAE = '\n'.join('{}-{}'.format(k,v) for k,v in scores.iteritems())
overallMAE = np.mean(scores.values())

print "Individual MAE scores:\n", allMAE
        
print "Average MAE score:", overallMAE

with open('../../../../../report/data/regressorMAE.txt','w') as f:
    f.write('Overall MAE is {}, list of MAEs is {}'.format(allMAE, overallMAE))
'''