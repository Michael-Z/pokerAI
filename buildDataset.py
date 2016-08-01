import sys
import os
import shutil
import gc
import multiprocessing
import pandas as pd
import numpy as np
from datetime import datetime
from bisect import bisect_left
import csv
from itertools import izip, product, combinations
import MySQLdb

######################### PREP AND UTILITY FUNCTIONS ##########################
# quick convert of numeric list to list of strings
def toStrings(l):
    l = list(l)
    if type(l[0])==float:
        return [str(round(x,3)) for x in l]
    return [str(x) for x in l]

# get DB password from file
with open('pwd.txt') as f:
    pwd = f.read().strip()

# connect to DB
db = MySQLdb.connect(host='localhost',port=3307,user='ntaylorwss',passwd=pwd)
cur = db.cursor()
cur.execute('USE poker;')

# restart the table if it already exists
allTables = ['features','quickFeatures','tableFeatures','columnFeatures',
             'columnFeaturesTemp','featuresOldTemp','featuresOld',
             'featuresNewTemp','featuresNew']
for t in allTables:
    try:
        cur.execute('DROP TABLE {};'.format(t))
    except MySQLdb.OperationalError:
        pass

# create folders for text files
tables = ['quick','table','column']
if os.path.exists('features'):
    shutil.rmtree('features')
for fdr in ['table','column']:
    os.makedirs('features/{}'.format(fdr))
os.makedirs('features/new')

########################## CREATE TABLES IN DATABASE ##########################
datatypes = {'ActionID': 'int NOT NULL AUTO_INCREMENT','Action': 'varchar(10)',
             'AggInPosVsMe': 'tinyint(1)','AggStack': 'decimal(10,2)',
             'AggressorPos': 'tinyint(2)','AllAggFactor': 'decimal(8,4)',
             'AllBet': 'decimal(4,3)','AllCall': 'decimal(4,3)',
             'AllCheck': 'decimal(4,3)','AllFold': 'decimal(4,3)',
             'AllRaise': 'decimal(4,3)','AmountToCall': 'decimal(10,2)',
             'AvgCardRankFlop': 'decimal(4,2)','AvgCardRankRiver': 'decimal(4,2)',
             'AvgCardRankTurn': 'decimal(4,2)','BetRiverPct': 'decimal(4,3)',
             'BetsRaisesF': 'tinyint(2)','BetsRaisesGame': 'tinyint(2)',
             'BetsRaisesP': 'tinyint(2)','BetsRaisesR': 'tinyint(2)',
             'BetsRaisesT': 'tinyint(2)','BigBlind': 'decimal(4,2)',
             'CBisCheckRaise': 'tinyint(1)','CallCBetPct': 'decimal(4,3)',
             'CallFlopBetPct': 'decimal(4,3)','CallOrRaisePFRPct': 'decimal(4,3)',
             'ContBetPct': 'decimal(4,3)','CurrentPot': 'decimal(10,2)',
             'ESvsAgg': 'decimal(10,2)','EffectiveStack': 'decimal(10,2)',
             'FacingBet': 'tinyint(2)','FinalPotLastHandTable': 'decimal(10,2)',
             'FlopAggFactor': 'decimal(8,4)','FlopBetPct': 'decimal(4,3)',
             'FlopCallPct': 'decimal(4,3)','FlopCheckPct': 'decimal(4,3)',
             'FlopFoldPct': 'decimal(4,3)','FlopRaisePct': 'decimal(4,3)',
             'FlushRivered': 'tinyint(1)','FlushTurned': 'tinyint(1)',
             'FoldToCBetPct': 'decimal(4,3)','FoldToFlopBetPct': 'decimal(4,3)',
             'HighCardFlop': 'tinyint(2)','HighCardRiver': 'tinyint(2)',
             'HighCardTurn': 'tinyint(2)','InvestedThisGame': 'decimal(10,2)',
             'IsAgg': 'tinyint(1)','IsBB': 'tinyint(1)',
             'IsSB': 'tinyint(1)','LastAction': 'varchar(10)',
             'LastToAct': 'tinyint(2)','LastToActStack': 'decimal(10,2)',
             'MaxOtherStackRelBBRelSelf': 'decimal(6,2)',
             'MeanOtherStackRelBBRelSelf': 'decimal(6,2)',
             'MinOtherStackRelBBRelSelf': 'decimal(6,2)',
             'NetAtTable': 'decimal(10,2)','NetLastHandRelSS': 'decimal(8,2)',
             'NumChecksGame': 'tinyint(2)','NumFaceCardsFlop': 'tinyint(2)',
             'NumFaceCardsRiver': 'tinyint(2)','NumFaceCardsTurn': 'tinyint(2)',
             'NumPairsFlop': 'tinyint(2)','NumPairsRiver': 'tinyint(2)',
             'NumPairsTurn': 'tinyint(2)','NumPlayersLeft': 'tinyint(2)',
             'NumPlayersStart': 'tinyint(2)','PartInLastHand': 'tinyint(1)',
             'PreflopAggFactor': 'decimal(8,4)','PreflopBetPct': 'decimal(4,3)',
             'PreflopCallPct': 'decimal(4,3)','PreflopCheckPct': 'decimal(4,3)',
             'PreflopFoldPct': 'decimal(4,3)','PreflopRaisePct': 'decimal(4,3)',
             'RaiseCBetPct': 'decimal(4,3)','RaiseFlopBetPct': 'decimal(4,3)',
             'RangeFlop': 'tinyint(2)','RangeRiver': 'tinyint(2)',
             'RangeTurn': 'tinyint(2)','RiverAggFactor': 'decimal(8,4)',
             'RiverBetPct': 'decimal(4,3)','RiverBrick': 'tinyint(1)',
             'RiverCallPct': 'decimal(4,3)','RiverCheckPct': 'decimal(4,3)',
             'RiverFoldPct': 'decimal(4,3)','RiverOverCard': 'tinyint(1)',
             'RiverRaisePct': 'decimal(4,3)','Round': 'varchar(10)',
             'SDOtherStackRelBBRelSelf': 'decimal(6,2)',
             'SeatRelDealerRelNP': 'tinyint(2)',
             'SeeSDPct': 'decimal(4,3)','StackToPot': 'decimal(8,2)',
             'ThreeBetPct': 'decimal(4,3)','ThreeOrMoreToStraightRiver': 'tinyint(1)',
             'ThreeOrMoreToStraightTurn': 'tinyint(1)',
             'ThreeToFlushDrawFlop': 'tinyint(1)',
             'ThreeToStraightFlop': 'tinyint(1)',
             'TurnAggFactor': 'decimal(8,4)','TurnBetPct': 'decimal(4,3)',
             'TurnBrick': 'tinyint(1)','TurnCallPct': 'decimal(4,3)',
             'TurnCheckPct': 'decimal(4,3)','TurnFoldPct': 'decimal(4,3)',
             'TurnOverCard': 'tinyint(1)','TurnRaisePct': 'decimal(4,3)',
             'TwoToFlushDrawFlop': 'tinyint(1)','TwoToStraightDrawFlop': 'tinyint(1)',
             'TwoToStraightDrawTurn': 'tinyint(1)','VPIP': 'decimal(4,3)',
             'WinAtSDPct': 'decimal(4,3)','WinWithoutSDPct': 'decimal(4,3)',
             'WinWhenSeeFlopPct': 'decimal(4,3)','sdVPIP': 'decimal(8,4)'
             }

tableCols = {
    'quickFeatures': ['ActionID', 'Action', 'Round', 'FacingBet',
          'AmountToCall', 'CurrentPot', 'NumPlayersStart',
          'NumPlayersLeft', 'BigBlind'],
    'tableFeatures': ['ActionID', 'NumChecksGame', 'LastToAct', 'LastToActStack',
          'FinalPotLastHandTable', 'CBisCheckRaise', 'BetsRaisesGame',
          'BetsRaisesP', 'BetsRaisesF', 'BetsRaisesT', 'BetsRaisesR',
          'NumPairsFlop', 'NumPairsTurn', 'NumPairsRiver', 
          'TwoToFlushDrawFlop', 'ThreeToFlushDrawFlop', 'FlushTurned', 
          'FlushRivered', 'HighCardFlop', 'HighCardTurn', 'HighCardRiver', 
          'RangeFlop', 'RangeTurn', 'RangeRiver', 'TwoToStraightDrawFlop',
          'TwoToStraightDrawTurn', 'ThreeToStraightFlop', 
          'ThreeOrMoreToStraightTurn', 'ThreeOrMoreToStraightRiver', 
          'TurnOverCard', 'RiverOverCard', 'NumFaceCardsFlop', 
          'NumFaceCardsTurn', 'NumFaceCardsRiver', 'AvgCardRankFlop', 
          'AvgCardRankTurn', 'AvgCardRankRiver', 'TurnBrick', 
          'RiverBrick', 'MeanOtherStackRelBBRelSelf', 
          'SDOtherStackRelBBRelSelf', 'MaxOtherStackRelBBRelSelf', 
          'MinOtherStackRelBBRelSelf', 'AggressorPos', 'AggInPosVsMe', 
          'AggStack', 'IsAgg', 'SeatRelDealerRelNP', 'EffectiveStack', 
          'ESvsAgg', 'StackToPot', 'IsSB', 'IsBB', 'InvestedThisGame'],
    'columnFeaturesTemp': ['ActionID', 'Action', 'LastAction', 'AllFold', 
          'AllCheck', 'AllCall', 'AllBet', 'AllRaise', 'PreflopFoldPct', 
          'PreflopCheckPct', 'PreflopCallPct', 'PreflopBetPct', 'PreflopRaisePct',
          'FlopFoldPct', 'FlopCheckPct', 'FlopCallPct', 'FlopBetPct', 'FlopRaisePct', 
          'TurnFoldPct', 'TurnCheckPct', 'TurnCallPct', 'TurnBetPct', 'TurnRaisePct', 
          'RiverFoldPct', 'RiverCheckPct', 'RiverCallPct', 'RiverBetPct', 
          'RiverRaisePct', 'VPIP', 'NetAtTable', 'ThreeBetPct', 'SeeSDPct', 
          'WinWhenSeeFlopPct', 'WinWithoutSDPct', 'WinAtSDPct', 'ContBetPct', 
          'BetRiverPct', 'CallOrRaisePFRPct', 'FoldToCBetPct', 'CallCBetPct', 
          'RaiseCBetPct', 'FoldToFlopBetPct', 'CallFlopBetPct', 'RaiseFlopBetPct', 
          'NetLastHandRelSS', 'PartInLastHand'],
    'featuresOld': ['ActionID', 'Action', 'Round', 'FacingBet', 
          'AmountToCall', 'CurrentPot', 'NumPlayersStart', 'NumPlayersLeft', 
          'BigBlind', 'NumChecksGame', 'LastToAct', 'LastToActStack', 
          'FinalPotLastHandTable', 'CBisCheckRaise', 'BetsRaisesGame', 
          'BetsRaisesP', 'BetsRaisesF', 'BetsRaisesT', 'BetsRaisesR', 
          'NumPairsFlop', 'NumPairsTurn', 'NumPairsRiver', 'TwoToFlushDrawFlop',
          'ThreeToFlushDrawFlop', 'FlushTurned', 'FlushRivered', 'HighCardFlop',
          'HighCardTurn', 'HighCardRiver', 'RangeFlop', 'RangeTurn', 'RangeRiver',
          'TwoToStraightDrawFlop', 'TwoToStraightDrawTurn', 'ThreeToStraightFlop',
          'ThreeOrMoreToStraightTurn', 'ThreeOrMoreToStraightRiver', 'TurnOverCard',
          'RiverOverCard', 'NumFaceCardsFlop', 'NumFaceCardsTurn', 'NumFaceCardsRiver', 
          'AvgCardRankFlop', 'AvgCardRankTurn', 'AvgCardRankRiver', 'TurnBrick', 
          'RiverBrick', 'MeanOtherStackRelBBRelSelf', 'SDOtherStackRelBBRelSelf', 
          'MaxOtherStackRelBBRelSelf', 'MinOtherStackRelBBRelSelf', 'AggressorPos', 
          'AggInPosVsMe', 'AggStack', 'IsAgg', 'SeatRelDealerRelNP', 'EffectiveStack', 
          'ESvsAgg', 'StackToPot', 'IsSB', 'IsBB', 'InvestedThisGame', 'LastAction', 
          'AllFold', 'AllCheck', 'AllCall', 'AllBet', 'AllRaise', 'PreflopFoldPct', 
          'PreflopCheckPct', 'PreflopCallPct', 'PreflopBetPct', 'PreflopRaisePct',
          'FlopFoldPct', 'FlopCheckPct', 'FlopCallPct', 'FlopBetPct', 'FlopRaisePct',
          'TurnFoldPct', 'TurnCheckPct', 'TurnCallPct', 'TurnBetPct', 'TurnRaisePct',
          'RiverFoldPct', 'RiverCheckPct', 'RiverCallPct', 'RiverBetPct',
          'RiverRaisePct', 'VPIP', 'NetAtTable', 'ThreeBetPct', 'SeeSDPct', 
          'WinWhenSeeFlopPct','WinWithoutSDPct', 'WinAtSDPct', 'ContBetPct',
          'BetRiverPct', 'CallOrRaisePFRPct', 'FoldToCBetPct', 'CallCBetPct', 
          'RaiseCBetPct', 'FoldToFlopBetPct', 'CallFlopBetPct', 'RaiseFlopBetPct',
          'NetLastHandRelSS','PartInLastHand'],
    'featuresNew': ['ActionID', 'Action', 'sdVPIP', 'AllAggFactor',
          'PreflopAggFactor', 'FlopAggFactor', 'TurnAggFactor', 'RiverAggFactor']}
tableCols['columnFeatures'] = list(tableCols['columnFeaturesTemp'])
tableCols['columnFeatures'].remove('Action')
tableCols['features'] = tableCols['featuresOld'] + tableCols['featuresNew'][2:]

def createTable(tableName, pk = True):
    cols = tableCols[tableName]
    q = """CREATE TABLE {}
    ( {}
    ) ENGINE = MYISAM;"""
    colStrings = '\n'.join('{} {},'.format(c,datatypes[c]) for c in cols)
    colStrings = colStrings[:-1] # remove trailing comma
    if pk:
        colStrings += ',\n PRIMARY KEY (ActionID)'
    finalQ = q.format(tableName, colStrings)
    cur.execute(finalQ)
    return finalQ

startTime = datetime.now()

for t in tableCols:
    createTable(t)

print "Checkpoint, tables created:", datetime.now()-startTime

######################## POPULATE QUICK FEATURES ##############################
cur.execute("""INSERT INTO quickFeatures
            (Action,Round,FacingBet,AmountToCall,CurrentPot,
             NumPlayersStart,NumPlayersLeft,BigBlind)
            SELECT a.Action,a.Round,a.CurrentBet>a.InvestedThisRound,
                    a.CurrentBet-a.InvestedThisRound,a.CurrentPot,
                    g.NumPlayers,a.NumPlayersLeft,g.BigBlind
            FROM actions AS a
            INNER JOIN games AS g
            ON a.GameNum=g.GameNum
            WHERE (Action!="blind") AND (Action!="deadblind");""")
print "Checkpoint, quickFeatures populated:", datetime.now()-startTime
######################## POPULATE TABLE FEATURES ##############################
os.chdir('features/table')
#### VECTORIZED FEATURES ####
# needed columns: GameNum,Player,Action
cols = ['GameNum','Player','Action','Round','SeatRelDealer','CurrentStack']
cur.execute("""SELECT a.{} FROM actions AS a
            INNER JOIN games AS g
            ON a.GameNum=g.GameNum;""".format(','.join(cols)))
poker = []
gamesInChunk = set()

newCols = {}
# write new columns to txt files
def getCols(poker):
    # initialize
    global newCols
    #newCols = {}
    poker = pd.DataFrame(poker, columns=cols)
    pokerWOB = pd.DataFrame(poker.ix[~(poker.Action.isin(['deadblind','blind']))])
    
    # vectorized columns
    pokerWOB['PrevAction'] = poker.groupby(['GameNum','Player']).Action.shift(1).ix[pokerWOB.index].fillna('None')
    newCols['CBisCheckRaise'] = (pokerWOB.PrevAction=='check') & (pokerWOB.Action=='raise')
    
    pokerWOB['BetOrRaise'] = pokerWOB.Action.isin(['bet','raise'])
    newCols['BetsRaisesGame'] = pokerWOB.groupby('GameNum').BetOrRaise.cumsum()
    
    agg = poker.Action.isin(['deadblind','blind','bet','raise']).astype(bool)
    pokerWOB['AggressorPos'] = (poker.SeatRelDealer*agg).replace(to_replace=0,method='ffill').ix[pokerWOB.index]
    newCols['AggressorPos'] = pokerWOB.AggressorPos
    newCols['AggInPosVsMe'] = pokerWOB.AggressorPos < pokerWOB.SeatRelDealer
    newCols['AggStack'] = (poker.CurrentStack*agg).replace(to_replace=0,method='ffill').ix[pokerWOB.index]
    
    # total bets and raises for each round
    # (not vectorized, but convenient to calculate here)
    for r in ['P','F','R','T']:
        newCols['BetsRaises{}'.format(r)] = []
    relevantCols = ['GameNum','Round','BetOrRaise']
    brDF = zip(*[pokerWOB[c] for c in relevantCols])
    rounds = ['Preflop','Flop','Turn','River']
    newBRCols = {r:[] for r in rounds}
    counts = {r:0 for r in rounds}
    for i,(g,r,bor) in enumerate(brDF):
        if g!=brDF[i-1][0]:
            counts = {r:0 for r in rounds}
        for rd in rounds:
            newBRCols[rd].append(counts[rd])
            if r==rd: counts[r] += bor
    for rd in rounds:
        newCols['BetsRaises{}'.format(rd[0])] = newBRCols[rd]
    
    # write columns to text
    for c,v in newCols.iteritems():
        with open('{}.txt'.format(c),'ab') as f:
            f.write('\n'.join(toStrings(v)) + '\n')

# populate chunk; call getCols on chunk; empty chunk; repeat
for row in cur:
    poker.append(row)
    gamesInChunk.add(row[0])
    if len(gamesInChunk) % 69768 == 0:
        getCols(poker)
        poker = []
        gamesInChunk = set()
getCols(poker)
del poker,gamesInChunk

print "Checkpoint, all vectorized features done:", datetime.now()-startTime

#### LOOPING FEATURES ####
cols =  ['GameNum','Round','SeatRelDealer','Action','CurrentStack',
         'TableName','CurrentPot']
cur.execute("""SELECT {}
            FROM actions AS a
            INNER JOIN games AS g
            ON a.GameNum=g.GameNum;""".format('a.'+','.join(cols)))
poker = []
gamesInChunk = set()

def getCols(poker):
    # initialize
    newCols = {}
    poker = pd.DataFrame(poker, columns=cols)
    pokerWOB = poker.ix[~(poker.Action.isin(['blind','deadblind']))]
    
    # last to act
    relevantCols = ['GameNum','Round','SeatRelDealer','Action']
    ltaDF = zip(*[poker[c] for c in relevantCols])
    LTAbyRow = []
    m = len(ltaDF)
    for i,(gameNum,rd,seat,action) in enumerate(ltaDF):
        ap = []
        windowStart = i
        windowEnd = i
        while windowStart>=0 and ltaDF[windowStart][:2]==(gameNum,rd):
            if ltaDF[windowStart][3]!='fold':
                ap.append(ltaDF[windowStart][2])
            windowStart -= 1
        while windowEnd<m and ltaDF[windowEnd][:2]==(gameNum,rd):
            ap.append(ltaDF[windowEnd][2])
            windowEnd += 1
        LTAbyRow.append(min(ap))
    newCols['LastToAct'] = [LTAbyRow[i] for i in pokerWOB.index]
    del LTAbyRow
    gc.collect()

    # last to act stack
    ltasDF = zip(pokerWOB.GameNum, pokerWOB.CurrentStack, 
                 pokerWOB.SeatRelDealer, newCols['LastToAct'])
    LTASbyRow = []
    m = len(ltasDF)
    for i,(gameNum,stack,seat,lta) in enumerate(ltasDF):
        s = 0
        windowStart = i
        windowEnd = i
        while windowStart>=0 and ltasDF[windowStart][0]==gameNum:
            r = ltasDF[windowStart]
            if r[3]==r[2]:
                s = r[1]
                break
            windowStart -= 1
        if s==0:
            while windowEnd<m and ltasDF[windowEnd][0]==gameNum:
                r = ltasDF[windowEnd]
                if r[3]==r[2]:
                    s = r[1]
                    break
                windowEnd += 1
        LTASbyRow.append(s)
    newCols['LastToActStack'] = LTASbyRow
    del LTASbyRow
    gc.collect()

    # final pot of last hand at table
    relevantCols = ['GameNum','TableName','CurrentPot']
    tlhpDF = zip(*[pokerWOB[c] for c in relevantCols])
    TLHPbyRow = []
    tableLastHandPot = {}
    for i,(gameNum,table,cp) in enumerate(tlhpDF):
        if not table in tableLastHandPot:
            tableLastHandPot[table] = -1
        TLHPbyRow.append(tableLastHandPot[table])
        if (i+1)<len(tlhpDF) and gameNum!=tlhpDF[i+1][0]:
            tableLastHandPot[table] = cp
    newCols['FinalPotLastHandTable'] = TLHPbyRow
    del TLHPbyRow
    gc.collect()

    # write columns to text
    for c,v in newCols.iteritems():
        with open('{}.txt'.format(c),'ab') as f:
            f.write('\n'.join(toStrings(v)) + '\n')

# populate chunk; call getCols on chunk; empty chunk; repeat
for row in cur:
    poker.append(row)
    gamesInChunk.add(row[0])
    if len(gamesInChunk) % 69768 == 0:
        print len(gamesInChunk)
        getCols(poker)
        poker = []
        gamesInChunk = set()
getCols(poker)
del poker,gamesInChunk

print "Checkpoint, all looping features done:", datetime.now()-startTime

#### BOARD FEATURES ####
cols = ['Board{}'.format(i) for i in range(1,6)] + ['Action']
colsSQL = ['.'.join([a,b]) for a,b in zip(['b']*5+['a'], cols)]
cur.execute("""SELECT {} FROM actions AS a
                INNER JOIN boards AS b
                ON a.GameNum=b.GameNum;""".format(','.join(cols)))
poker = []
gamesInChunk = set()

def getCols(poker):
    # initialize
    newCols = {}
    poker = pd.DataFrame(poker, columns=cols)
    pokerWOB = poker.ix[~(poker.Action.isin(['deadblind','blind']))]
    boardDF = pokerWOB[cols[:-1]]
    boardSuits = boardDF%4
    boardRanks = boardDF%13
    
    # build DFs of rank counts and suit counts (nrow*13 and nrow*4 dims)
    rankCountsFlop, rankCountsTurn, rankCountsRiver = [{},{},{}]
    for r in xrange(13):
        rankCountsFlop[r] = list((boardRanks.ix[:,:2]==r).sum(axis=1))
        rankCountsTurn[r] = list((boardRanks.ix[:,:3]==r).sum(axis=1))
        rankCountsRiver[r] = list((boardRanks==r).sum(axis=1))
    rankCountsFlop = pd.DataFrame(rankCountsFlop)
    rankCountsTurn = pd.DataFrame(rankCountsTurn)
    rankCountsRiver = pd.DataFrame(rankCountsRiver)
    
    suitCountsFlop, suitCountsTurn, suitCountsRiver = [{},{},{}]
    for r in xrange(13):
        suitCountsFlop[r] = list((boardSuits.ix[:,:2]==r).sum(axis=1))
        suitCountsTurn[r] = list((boardSuits.ix[:,:3]==r).sum(axis=1))
        suitCountsRiver[r] = list((boardSuits==r).sum(axis=1))
    suitCountsFlop = pd.DataFrame(suitCountsFlop)
    suitCountsTurn = pd.DataFrame(suitCountsTurn)
    suitCountsRiver = pd.DataFrame(suitCountsRiver)
    
    # Number of pairs on the board
    newCols['NumPairsFlop'] = (rankCountsFlop==2).sum(axis=1)
    newCols['NumPairsTurn'] = (rankCountsTurn==2).sum(axis=1)
    newCols['NumPairsRiver'] = (rankCountsRiver==2).sum(axis=1)
    
    # Flush draw on the flop (2 to a suit, not 3)
    newCols['TwoToFlushDrawFlop'] = (suitCountsFlop==2).sum(axis=1)>0
    
    # Flush draw on the flop (3 to a suit)
    newCols['ThreeToFlushDrawFlop'] = (suitCountsFlop==3).sum(axis=1)>0
    
    # Flush draw on the turn (still 2 to a suit, not 3)
    newCols['TwoToFlushDrawTurn'] = (suitCountsTurn==2).sum(axis=1)>0
    
    # Flush draw connects on the turn (from 2 to 3)
    newCols['FlushTurned'] = (pd.Series(newCols['TwoToFlushDrawFlop'])) & \
                            ((suitCountsTurn==3).sum(axis=1)>0)
                            
    # Flush draw connects on the river (from 2 to 3)
    newCols['FlushRivered'] = (pd.Series(newCols['TwoToFlushDrawTurn'])) & \
                            ((suitCountsRiver==3).sum(axis=1)>0)
    
    # High card on each street
    newCols['HighCardFlop'] = boardRanks.ix[:,:2].max(axis=1)
    newCols['HighCardTurn'] = boardRanks.ix[:,:3].max(axis=1)
    newCols['HighCardRiver'] = boardRanks.max(axis=1)
    
    # Range of cards on each street
    newCols['RangeFlop'] = pd.Series(newCols['HighCardFlop']) - boardRanks.ix[:,:2].min(axis=1)
    newCols['RangeTurn'] = pd.Series(newCols['HighCardTurn']) - boardRanks.ix[:,:3].min(axis=1)
    newCols['RangeRiver'] = pd.Series(newCols['HighCardRiver']) - boardRanks.min(axis=1)
    
    # build DF of card ranks differences (1-2, 1-3, ..., 4-5) for straight draw finding
    diffs = {}
    for a,b in product(range(5), range(5)):
        if a!=b:
            k = '-'.join([str(a+1),str(b+1)])
            if not k in diffs:
                diffs[k] = []
            diffs[k].append(list(abs(boardRanks.ix[:,a] - boardRanks.ix[:,b]))[0])
    diffs = pd.DataFrame(diffs)
    
    # 2 to a straight draw on each flop, turn
    diffsFlop = diffs[[c for c in diffs.columns
                        if not ('3' in c or '4' in c)]]
    newCols['TwoToStraightDrawFlop'] = ((diffsFlop==1).sum(axis=1))>=1 
    
    diffsTurn = diffs[[c for c in diffs.columns if not '4' in c]]
    newCols['TwoToStraightDrawTurn'] = ((diffsTurn==1).sum(axis=1))>=1
    
    # 3+ to a straight on flop
    newCols['ThreeToStraightFlop'] = pd.Series(newCols['RangeFlop'])==2
    
    # 3+ to a straight on turn
    comboRanges = []
    for cards in combinations(range(4),3):
        c = boardRanks.ix[:,cards]
        comboRanges.append((c.max(axis=1) - c.min(axis=1) == 2) & (c.notnull().sum(axis=1)==3))
    newCols['ThreeOrMoreToStraightTurn'] = pd.DataFrame(comboRanges).sum()>0
    
    # 3+ to a straight on river
    comboRanges = []
    for cards in combinations(range(5),3):
        c = boardRanks.ix[:,cards]
        comboRanges.append((c.max(axis=1) - c.min(axis=1) == 2) & (c.notnull().sum(axis=1)==3))
    newCols['ThreeOrMoreToStraightRiver'] = pd.DataFrame(comboRanges).sum()>0
    
    # turn is over card (greater than max(flop))
    newCols['TurnOverCard'] = boardRanks['Board4'] > boardRanks.ix[:,:3].max(axis=1)
    
    # river is over card (greater than max(flop+turn))
    newCols['RiverOverCard'] = boardRanks['Board5'] > boardRanks.ix[:,:4].max(axis=1)
    
    # num face cards each street
    newCols['NumFaceCardsFlop'] = (boardRanks.ix[:,:3]>=9).sum(axis=1)
    newCols['NumFaceCardsTurn'] = (boardRanks.ix[:,:4]>=9).sum(axis=1)
    newCols['NumFaceCardsRiver'] = (boardRanks>=9).sum(axis=1)
    
    # average card rank
    newCols['AvgCardRankFlop'] = (boardRanks.ix[:,:3]).mean(axis=1)
    newCols['AvgCardRankTurn'] = (boardRanks.ix[:,:4]).mean(axis=1)
    newCols['AvgCardRankRiver'] = boardRanks.mean(axis=1)
    
    # turn is a brick (5 or less and not pair and not making a flush)
    newCols['TurnBrick'] = (boardRanks.Board4<=5) & (pd.Series(newCols['NumPairsFlop'])==pd.Series(newCols['NumPairsTurn'])) & (~pd.Series(newCols['FlushTurned']))
    
    # river is a brick (5 or less and not pair and not making a flush)
    newCols['RiverBrick'] = (boardRanks.Board5<=5) & (pd.Series(newCols['NumPairsTurn'])==pd.Series(newCols['NumPairsRiver'])) & (~pd.Series(newCols['FlushRivered']))
    
    # write to text files
    for c,v in newCols.iteritems():
        with open('{}.txt'.format(c),'ab') as f:
            f.write('\n'.join(toStrings(v)) + '\n')
            
# populate chunk; call getCols on chunk; empty chunk; repeat
for row in cur:
    poker.append(row)
    gamesInChunk.add(row[0])
    if len(gamesInChunk) % 69768 == 0:
        getCols(poker)
        poker = []
        gamesInChunk = set()
getCols(poker)
del poker,gamesInChunk

print "Checkpoint, all table features done:", datetime.now()-startTime

######################## POPULATE COLUMN FEATURES #############################
os.chdir('../column')
# list of possible actions, for reference in multiple columns
actionsWB = ['deadblind','blind','fold','check','call','bet','raise']
actions = actionsWB[2:]

# player's last action
cur.execute('SELECT ActionID,Player,Action FROM actions;')
with open('LastAction.txt','ab') as outF:
    pla = []
    lastActions = {}
    for i,p,a in cur:
        if p in lastActions:
            pla.append(lastActions[p])
        else:
            pla.append('None')
        lastActions[p] = a
        if i % 10000000 == 0:
            outF.write('\n'.join(pla) + '\n')
            pla = []
    outF.write('\n'.join(pla))
    pla = None
print "Checkpoint, first column feature done:", datetime.now()-startTime
############################################
# players' actions by round as percentages
## GET
cur.execute('SELECT Player,Action,Round FROM actions;')
rounds = ['Preflop','Flop','Turn','River','All']
playersActionsByRound = {}
colsToBeWritten = {a: {r:[] for r in rounds} for a in actions}
for p,a,r in cur:
    if p=='': continue
    if p in playersActionsByRound:
        if a in playersActionsByRound[p]:
            if r in playersActionsByRound[p][a]:
                playersActionsByRound[p][a][r] += 1.
            else:
                playersActionsByRound[p][a][r] = 1.
            playersActionsByRound[p][a]['All'] += 1.
        else:
            playersActionsByRound[p][a] = {r:1., 'All':1.}
    else:
        playersActionsByRound[p] = {a:{r:1., 'All':1.}}
## WRITE
cur.execute('SELECT ActionID,Player,Action FROM actions;')
for i,p,a in cur:
    actionsByRound = playersActionsByRound[p]
    # fill all missing keys, e.g. player never folded on flop add a 0 there
    for a in actions:
        if not a in actionsByRound:
            actionsByRound[a] = {r:0. for r in rounds}
        else:
            for r in rounds:
                if not r in actionsByRound[a]:
                    actionsByRound[a][r] = 0.
    # collect
    for a in actions:
        byRound = actionsByRound[a]
        for r in rounds[:-1]:
            numAinR = sum(actionsByRound[A][r] for A in actions)
            if numAinR!=0:
                rAsPct = byRound[r] / numAinR
            else:
                rAsPct = 0.
            colsToBeWritten[a][r].append(rAsPct)
        colsToBeWritten[a]['All'].append(byRound['All'])
    if i % 10000000 == 0:
        for a in actions:
            for r in rounds:
                f = open('{}{}Pct.txt'.format(r,a[:1].upper()+a[1:]),'ab')
                f.write('\n'.join(toStrings(colsToBeWritten[a][r])) + '\n')
                f.close()
        colsToBeWritten[a][r] = []
for a in actions:
    for r in rounds:
        f = open('{}{}{}.txt'.format(
                r,a[:1].upper()+a[1:], 'Pct'*(r!='All')),'ab')
        f.write('\n'.join(toStrings(colsToBeWritten[a][r])))
        f.close()
colsToBeWritten,playerActionsByRound = [None,None]
############################################
# VPIP (voluntarily put $ in pot)
## GET
cur.execute('SELECT Player,Round,Action FROM actions;')
vpip = []
pfr = []
playersCalls = {}
playersRaises = {}
playersPreflopOps = {}
rounds = set()
for p,r,a in cur:
    if r=='Preflop':
        if p in playersPreflopOps:
            playersPreflopOps[p] += 1.
            playersCalls[p] += a=='call'
            playersRaises[p] += a=='raise'
        else:
            playersPreflopOps[p] = 1.
            playersCalls[p] = int(a=='call')
            playersRaises[p] = int(a=='raise')
## WRITE
cur.execute('SELECT ActionID,Player FROM actions;')
with open('VPIP.txt','ab') as vpipF:
    for i,p in cur:
        vpip.append((playersCalls[p]+playersRaises[p]) / playersPreflopOps[p])
        if i % 10000000 == 0:
            vpipF.write('\n'.join(toStrings(vpip)) + '\n')
            vpip = []
    vpipF.write('\n'.join(toStrings(vpip)))
    vpip,playersCalls,playersRaises,playersPreflopOps = [None,None,None,None]
############################################
# net at table
cur.execute("""SELECT a.Player,g.TableName,a.CurrentStack,a.StartStack 
            FROM actions AS a
            INNER JOIN games AS g ON a.GameNum=g.GameNum;""")
with open('NetAtTable.txt','ab') as outF:
    nat = []
    playerTableStartStacks = {}
    for p,t,c,s in cur:
        if p in playerTableStartStacks:
            if t in playerTableStartStacks[p]:
                # player seen at table before, take difference
                nat.append(c - playerTableStartStacks[p][t])
            else:
                # player not seen at table before, record start stack at table, take 0
                playerTableStartStacks[p][t] = s
                nat.append(0)
        else:
            # player not seen before, record first table start stack, take 0
            playerTableStartStacks[p] = {t: s}
            nat.append(0)
        if i % 10000000 == 0:
            outF.write('\n'.join(toStrings(nat)) + '\n')
            nat = []
    outF.write('\n'.join(toStrings(nat)))
    nat,playerTableStartStacks = [None,None]
############################################
# 3-bet %
## GET
cur.execute('SELECT Player,Round,Action FROM actions;')
threeBets = []
player3Bets = {}
player3BetOpps = {}
lastRowRound = ''
for p,r,a in cur:
    if r!=lastRowRound:
        better = ''
        raiser = ''
    if a=='bet':
        better = p
    if a=='raise':
        raiser = p
    if a!='bet' and better==p:
        if p in player3BetOpps:
            player3Bets[p] += a=='raise'
            player3BetOpps[p] += 1.
        else:
            player3Bets[p] = a=='raise'
            player3BetOpps[p] = 1.
    lastRowRound = r
cur.execute('SELECT ActionID,Player FROM actions;')
with open('ThreeBetPct.txt','ab') as outF:
    for i,p in cur:
        if p in player3BetOpps:
            threeBets.append(player3Bets[p] / player3BetOpps[p])
        else:
            threeBets.append(0)
        if i % 10000000 == 0:
            outF.write('\n'.join(toStrings(threeBets)) + '\n')
            threeBets = []
    outF.write('\n'.join(toStrings(threeBets)))
    threeBets,player3Bets,player3BetOpps = [None,None,None]
############################################
# see showdown %
## GET
cur.execute('SELECT Player,GameNum,HoleCard1 FROM actions;')
ssPct = []
playerGameSeesSD = {}
for p,g,c in cur:
    if p in playerGameSeesSD:
        if not g in playerGameSeesSD[p]:
                playerGameSeesSD[p][g] = c!='-1'
    else:
        playerGameSeesSD[p] = {g: c!='-1'}
## WRITE
cur.execute('SELECT ActionID,Player FROM actions;')
with open('SeeSDPct.txt','ab') as outF:
    for i,p in cur:
        allG = playerGameSeesSD[p].values()
        ssPct.append(np.mean(allG))
        if i % 10000000 == 0:
            outF.write('\n'.join(toStrings(ssPct)) + '\n')
            ssPct = []
    outF.write('\n'.join(toStrings(ssPct)))
    ssPct,playerGameSeesSD = [None,None]
############################################
# win % when see flop
## GET
cur.execute('SELECT Player,GameNum,Round,Winnings FROM actions;')
wf = []
playerFlopWins = {}
playerFlopOpps = {}
for p,g,r,w in cur:
    if r!='Preflop':
        if p in playerFlopOpps:
            if not g in playerFlopOpps[p]:
                playerFlopOpps[p].add(g)
                playerFlopWins[p] += w>0
        else:
            playerFlopOpps[p] = {g}
            playerFlopWins[p] = w>0
## WRITE
cur.execute('SELECT ActionID,Player FROM actions;')
with open('WinWhenSeeFlopPct.txt','ab') as outF:
    for i,p in cur:
        if p in playerFlopOpps:
            wf.append(float(playerFlopWins[p]) / len(playerFlopOpps[p]))
        else:
            wf.append(0)
        if i % 10000000 == 0:
            outF.write('\n'.join(toStrings(wf)) + '\n')
            wf = []
    outF.write('\n'.join(toStrings(wf)))
    wf,playerFlopWins,playerFlopOpps = [None,None,None]
############################################
# win without showdown % (wins without showdown / total wins)
## GET
cur.execute('SELECT ActionID,Player,GameNum,HoleCard1,Winnings FROM actions;')
wws = []
playerWinsWSD = {}
playerWins = {}
for i,p,g,c,w in cur:
    if w:
        if p in playerWins:
            playerWins[p].add(g)
            if c=='-1':
                playerWinsWSD[p].add(g)
        else:
            playerWins[p] = {g}
            if c=='-1':
                playerWinsWSD[p] = {g}
## WRITE
cur.execute('SELECT ActionID,Player FROM actions;')
with open('WinWithoutSDPct.txt','ab') as outF:
    for i,p in cur:
        if p in playerWinsWSD:
            wws.append(float(len(playerWinsWSD[p])) / len(playerWins[p]))
        else:
            wws.append(0)
        if i % 10000000 == 0:
            outF.write('\n'.join(toStrings(wws)) + '\n')
            wws = []
    outF.write('\n'.join(toStrings(wws)))
    wws,playerWinsWSD,playerWins = [None,None,None]
############################################
# win % at showdown
## GET
cur.execute('SELECT Player,GameNum,HoleCard1,Winnings FROM actions;')
ws = []
playerWinsAtSD = {}
playerShowdowns = {}
for p,g,c,w in cur:
    if c!='-1':
        if p in playerWinsAtSD:
            playerShowdowns[p].add(g)
            if w:
                playerWinsAtSD[p].add(g)
        else:
            playerShowdowns[p] = {g}
            if w:
                playerWinsAtSD[p] = {g}
## WRITE
cur.execute('SELECT ActionID,Player FROM actions;')
with open('WinAtSDPct.txt','ab') as outF:
    for i,p in cur:
        if p in playerWinsAtSD:
            ws.append(float(len(playerWinsAtSD[p])) / len(playerShowdowns[p]))
        else:
            ws.append(0)
        if i % 10000000 == 0:
            outF.write('\n'.join(toStrings(ws)) + '\n')
            ws = []
    outF.write('\n'.join(toStrings(ws)))
    ws,playerWinsAtSD,playerShowdowns = [None,None,None]
############################################
# continuation bet %
## GET
cur.execute('SELECT Player,GameNum,Round,Action FROM actions;')
cb = []
playerContBets = {}
playerContBetOpps = {}
lastG = ''
for p,g,r,a in cur:
    if not r in ['Preflop','Flop']:
        continue
    if g!=lastG:
        agg = ''
    if r=='Preflop':
        if a=='raise':
            agg = p
    elif r=='Flop':
        if p==agg:
            if p in playerContBetOpps:
                playerContBetOpps[p] += 1.
            else:
                playerContBetOpps[p] = 1.
                playerContBets[p] = 0.
            playerContBets[p] += a=='bet'
    lastG = g
## WRITE
cur.execute('SELECT ActionID,Player FROM actions;')
with open('ContBetPct.txt','ab') as outF:
    for i,p in cur:
        if p in playerContBets:
            cb.append(playerContBets[p] / playerContBetOpps[p])
        else:
            cb.append(0)
        if i % 10000000 == 0:
            outF.write('\n'.join(toStrings(cb)))
            cb = []
    outF.write('\n'.join(toStrings(cb)))
    cb,playerContBets,playerContBetOpps = [None,None,None]
############################################
# bet river %
## GET
cur.execute('SELECT Player,Round,Action FROM actions;')
br = []
playerRiverBets = {}
playerRiverOpps = {} # bets and checks
for p,r,a in cur:
    if r=='River':
        if p in playerRiverOpps:
            playerRiverOpps[p] += a in ['bet','check']
            playerRiverBets[p] += a=='bet'
        else:
            playerRiverOpps[p] = a in ['bet','check']
            playerRiverBets[p] = a=='bet'
## WRITE
cur.execute('SELECT ActionID,Player FROM actions;')
with open('BetRiverPct.txt','ab') as outF:
    for i,p in cur:
        if p in playerRiverOpps and playerRiverOpps[p]>0:
            br.append(float(playerRiverBets[p]) / playerRiverOpps[p])
        else:
            br.append(0)
        if i % 10000000 == 0:
            outF.write('\n'.join(toStrings(br)) + '\n')
            br = []
    outF.write('\n'.join(toStrings(br)))
    br,playerRiverBets,playerRiverOpps = [None,None,None]
############################################
# call/raise preflop raise %
## GET
cur.execute('SELECT Player,Round,GameNum,Action FROM actions;')
cpfr = []
playerPFRCalls = {}
playerPFROpps = {}
lastRaiseG = ''
for p,r,g,a in cur:
    if r!='Preflop':
        continue
    if a=='raise':
        lastRaiseG = g
    if g==lastRaiseG:
        if p in playerPFROpps:
            playerPFROpps[p] += 1
        else:
            playerPFROpps[p] = 1
            playerPFRCalls[p] = 0.
        if a in ['call','raise']:
            playerPFRCalls[p] += 1.
## WRITE
cur.execute('SELECT ActionID,Player FROM actions;')
with open('CallOrRaisePFRPct.txt','ab') as outF:
    for i,p in cur:
        if p in playerPFROpps and playerPFROpps[p]>0:
            cpfr.append(playerPFRCalls[p] / playerPFROpps[p])
        else:
            cpfr.append(0)
        if i % 10000000 == 0:
            outF.write('\n'.join(toStrings(cpfr)) + '\n')
            cpfr = []
    outF.write('\n'.join(toStrings(cpfr)))
    cpfr,playerPFRCalls,playerPFROpps = [None,None,None]
############################################
# fold to, call, raise C-bet %
## GET
cur.execute('SELECT Player,Round,GameNum,Action FROM actions;')
fcb,ccb,rcb = [[]]*3
playerCBetActions = {}
playerCBetOpps = {}
cBettor = ''
lastG = ''
cBetSituation = False
for p,r,g,a in cur:
    if not r in ['Preflop','Flop']:
        continue
    if g!=lastG:
        cBettor = ''
    if r=='Preflop':
        if a=='raise':
            cBettor = p
    elif r=='Flop':
        if a=='bet' and p==cBettor:
            cBetSituation = True
        if cBetSituation:
            if p in playerCBetOpps:
                playerCBetOpps[p] += 1
            else:
                playerCBetOpps[p] = 1.
            if p in playerCBetActions:
                if a in playerCBetActions[p]:
                    playerCBetActions[p][a] += 1
                else:
                    playerCBetActions[p][a] = 1.
            else:
                playerCBetActions[p] = {a:1.}
    lastG = g
## WRITE
cur.execute('SELECT ActionID,Player FROM actions;')
with open('FoldToCBetPct.txt','ab') as outFoldF, \
        open('CallCBetPct.txt','ab') as outCallF, open('RaiseCBetPct.txt','ab') as outRaiseF:
    for i,p in cur:
        if p in playerCBetOpps and playerCBetOpps[p]>0:
            if 'fold' in playerCBetActions[p]:
                fcb.append(playerCBetActions[p]['fold'] / playerCBetOpps[p])
            else:
                fcb.append(0)
            if 'call' in playerCBetActions[p]:
                ccb.append(playerCBetActions[p]['call'] / playerCBetOpps[p])
            else:
                ccb.append(0)
            if 'raise' in playerCBetActions[p]:
                rcb.append(playerCBetActions[p]['raise'] / playerCBetOpps[p])
            else:
                rcb.append(0)
        else:
            fcb.append(0)
            ccb.append(0)
            rcb.append(0)
        if i % 10000000 == 0:
            outFoldF.write('\n'.join(toStrings(fcb)) + '\n')
            outCallF.write('\n'.join(toStrings(ccb)) + '\n')
            outRaiseF.write('\n'.join(toStrings(rcb)) + '\n')
            fcb,ccb,rcb = [[]]*3
    outFoldF.write('\n'.join(toStrings(fcb)))
    outCallF.write('\n'.join(toStrings(ccb)))
    outRaiseF.write('\n'.join(toStrings(rcb)))
    fcb,ccb,rcb = [None,None,None]
############################################
# fold to, call, raise flop bet %
## GET
cur.execute('SELECT Player,Round,GameNum,Action FROM actions;')
ffb,cfb,rfb = [[]]*3
playerFBetActions = {}
playerFBetOpps = {}
facingBet = False
lastG = ''
for p,r,g,a in cur:
    if r=='Flop':
        if g!=lastG:
            facingBet = False
        if facingBet:
            if p in playerFBetOpps:
                playerFBetOpps[p] += 1
            else:
                playerFBetOpps[p] = 1
            if p in playerFBetActions:
                if a in playerFBetActions:
                    playerFBetActions[p][a] += 1
                else:
                    playerFBetActions[p][a] = 1.
            else:
                playerFBetActions[p] = {a:1.}
        if a=='bet':
            facingBet = True
        lastG = g
cur.execute('SELECT ActionID,Player FROM actions;')
with open('FoldToFlopBetPct.txt','ab') as outFoldF, \
        open('CallFlopBetPct.txt','ab') as outCallF, open('RaiseFlopBetPct.txt','ab') as outRaiseF:
    for i,p in cur:
        if p in playerFBetOpps and playerFBetOpps[p]>0:
            if 'fold' in playerFBetActions[p]:
                ffb.append(playerFBetActions[p]['fold'] / playerFBetOpps[p])
            else:
                ffb.append(0)
            if 'call' in playerFBetActions[p]:
                ffb.append(playerFBetActions[p]['call'] / playerFBetOpps[p])
            else:
                ffb.append(0)
            if 'raise' in playerFBetActions[p]:
                ffb.append(playerFBetActions[p]['raise'] / playerFBetOpps[p])
            else:
                ffb.append(0)
        else:
            ffb.append(0)
            cfb.append(0)
            rfb.append(0)
        if i % 10000000 == 0:
            outFoldF.write('\n'.join(toStrings(ffb)) + '\n')
            outCallF.write('\n'.join(toStrings(cfb)) + '\n')
            outRaiseF.write('\n'.join(toStrings(rfb)) + '\n')
            ffb,cfb,rfb = [[]]*3
    outFoldF.write('\n'.join(toStrings(ffb)))
    outCallF.write('\n'.join(toStrings(cfb)))
    outRaiseF.write('\n'.join(toStrings(rfb)))
    ffb,cfb,rfb = [None,None,None]
############################################
# net from last hand, rel start stack
cur.execute('SELECT ActionID,Player,GameNum,StartStack FROM actions;')
with open('NetLastHandRelSS.txt','ab') as outF:
    playerLastStacks = {}
    nets = []
    for i,p,g,s in cur:
        if p in playerLastStacks:
            nets.append(s - playerLastStacks[p]['Stack'])
            if g!=playerLastStacks[p]['GameNum']:
                playerLastStacks[p] = {'GameNum':g, 'Stack':s}
        else:
            playerLastStacks[p] = {'GameNum':g, 'Stack':s}
        if i % 10000000 == 0:
            outF.write('\n'.join(toStrings(nets)) + '\n')
            nets = []
    outF.write('\n'.join(toStrings(nets)))
    nets,playerLastStacks = [None,None]
############################################
# participated in last hand
cur.execute('SELECT ActionID,Player,GameNum,Action FROM actions;')
with open('PartInLastHand.txt','ab') as outF:
    playerPInLastHand = {}
    playerPInCurrentHand = {}
    plh = []
    lastG = ''
    for i,p,g,a in cur:
        # first run, populate lastHand with -1's, populate currentHand with actual
        # on same hand, take lastHand, don't update anything
        # on new hand, take currentHand, assign lastHand = currentHand, populate currentHand with actual
        if p in playerPInLastHand:
            if g==lastG:
                plh.append(playerPInLastHand[p])
            else:
                plh.append(playerPInCurrentHand[p])
                playerPInLastHand[p] = playerPInCurrentHand[p]
                playerPInCurrentHand[p] = a!='fold'
        else:
            playerPInLastHand[p] = -1
            playerPInCurrentHand[p] = a!='fold'
        if i % 10000000 == 0:
            outF.write('\n'.join(toStrings(plh)) + '\n')
            plh = []
        lastG = g
    outF.write('\n'.join(toStrings(plh)))
    plh,PlayerPInLastHand,playerPInCurrentHand = [None,None,None]
print "Checkpoint, all column features done:", datetime.now()-startTime
#################### COLUMN TXT TO CSV, CSV IMPORT ############################
# text to CSV
os.chdir('../column')
os.system('paste -d, {} > features.csv'.format(
        ' '.join('{}.txt'.format(fName) for fName in tableCols['columnFeatures'])))

#import CSV to columnFeatures (temp table->remove blinds->real table)
cur.execute("""LOAD DATA INFILE '{}/features.csv'
                INTO TABLE columnFeaturesTemp
                FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                ({});""".format(os.getcwd(), ','.join(tableCols['columnFeaturesTemp'])))
cur.execute("""INSERT INTO columnFeatures
            SELECT {}
            FROM columnFeaturesTemp AS t
            WHERE t.Action!="blind" AND t.Action!="deadblind"
            ;""".format(','.join(tableCols['columnFeatures'])))
cur.execute('DROP TABLE columnFeaturesTemp;')
print "Checkpoint, columnFeatures table populated:", datetime.now()-startTime
################## MERGE TABLES TO FEATURESOLD, DELETE ########################
colsToGrab = ['q.ActionID']
colsToGrab += ['q.{}'.format(c) for c in tableCols['quickFeatures'][1:]]
colsToGrab += ['t.{}'.format(c) for c in tableCols['tableFeatures'][1:]]
colsToGrab += ['c.{}'.format(c) for c in tableCols['columnFeatures'][1:]]
cur.execute("""INSERT INTO featuresOld
            SELECT {}
            FROM quickFeatures AS q
            INNER JOIN tableFeatures AS t ON q.ActionID=t.ActionID
            INNER JOIN columnFeatures AS c ON q.ActionID=c.ActionID
            ;""".format(','.join(colsToGrab)))
for t in tables:
    cur.execute('DROP TABLE {}Features;'.format(t))
print "Checkpoint, quick/table/column to featuresOld:", datetime.now()-startTime
########################### CREATE FEATURESNEW ################################
os.chdir('../new')
# sd of VPIP for each player
## GET
cur.execute("""SELECT a.Player,f.VPIP FROM featuresOld AS f
            INNER JOIN actions AS a ON a.ActionID=f.ActionID""")
sdv = []
playerVPIP = {}
for p,v in cur:
    if p in playerVPIP:
        playerVPIP[p].append(v)
    else:
        playerVPIP[p] = [v]
for p in playerVPIP:
    playerVPIP[p] = np.std(playerVPIP[p])
## WRITE
cur.execute('SELECT ActionID,Player FROM actions;')
with open('sdVPIP.txt','ab') as outF:
    for i,p in cur:
        sdv.append(playerVPIP[p])
        if i % 10000000 == 0:
            outF.write('\n'.join(toStrings(sdv)) + '\n')
            sdv = []
    outF.write('\n'.join(toStrings(sdv)))
    sdv,playerVPIP = [None,None]
############################################
# aggression factor overall
def getAF(r):
    cur.execute('SELECT {0}BetPct,{0}RaisePct,{0}CallPct FROM featuresOld;'.format(r))
    with open('{}AggFactor.txt'.format(r),'ab') as outF:
        af = []
        for b,r,c in cur:
            af.append((b+r)/c)
            if i % 10000000 == 0:
                outF.write('\n'.join(toStrings(af)) + '\n')
                af = []
        outF.write('\n'.join(toStrings(af)))
        af = None
getAF('All')
# aggression factor on flop
getAF('Flop')
# aggression factor on turn
getAF('Turn')
# aggression factor on river
getAF('River')
print "Checkpoint, new features created:", datetime.now()-startTime
#################### NEW TXT TO CSV, CSV IMPORT ###############################
os.system('paste -d, {} > ../features.csv'.format(
        ' '.join('{}.txt'.format(fName) for fName in tableCols['featuresNew'])))

# import new features to table
cur.execute("""LOAD DATA INFILE '{}/features.csv'
                INTO TABLE featuresNewTemp
                FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                ({});""".format(os.getcwd(), ','.join(tableCols['featuresNew'])))
cur.execute("""INSERT INTO featuresNew
            SELECT {}
            FROM featuresNewTemp
            WHERE Action!="blind" AND Action!="deadblind"
            ;""".format(','.join(tableCols['featuresNew'])))
cur.execute('DROP TABLE newFeaturesTemp;')
print "Checkpoint, newFeatures populated:", datetime.now()-startTime
################## MERGE OLD AND NEW TO FEATURES, DELETE ######################
colsToGrab = ['o.ActionID']
colsToGrab += ['o.{}'.format(c) for c in tableCols['featuresOld'][1:]]
colsToGrab += ['n.{}'.format(c) for c in tableCols['featuresNew'][1:]]
cur.execute("""INSERT INTO features
            SELECT {}
            FROM featuresOld AS o
            INNER JOIN featuresNew AS n ON o.ActionID=n.ActionID
            ;""".format(','.join(colsToGrab)))
cur.execute('DROP TABLE featuresOld;')
cur.execute('DROP TABLE featuresNew;')
print "END! features populated:", datetime.now()-startTime