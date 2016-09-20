import json
import pandas as pd
from itertools import product

with open('../data/util/FeatureSets.json') as json_data:
    fs = json.load(json_data)

results = []
for r,b in product(['Preflop','Flop','Turn','River'], [False,True]):
    if r=='Preflop' and not b: continue
    test = pd.read_csv("../data/test/data_engineered/subsets/{}-{}.csv".format(r,b), nrows=500000, names=fs['{}-{}'.format(r,b)])
    vc = test.Action.value_counts()
    result = dict(vc / vc.sum())
    result['Table'] = '{}-{}'.format(r,b)
    results.append(result)
    
breakdown = pd.DataFrame(results, columns=['Table','fold','check','call','bet','raise'])
pd.DataFrame(breakdown).fillna(0).round(3).to_csv('data/labelBreakdown.csv', index=False)