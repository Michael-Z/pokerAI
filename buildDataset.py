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

# create: quickFeatures, columnFeatures, tableFeatures,
#           featuresOld, featuresNew, features
quickCreate = """CREATE TABLE quickFeatures
            ( ActionID int NOT NULL AUTO_INCREMENT,
              Action varchar(10),
              Round varchar(10),
              FacingBet tinyint(2),
              AmountToCall decimal(10,2),
              CurrentPot decimal(10,2),
              NumPlayersStart tinyint(2),
              NumPlayersLeft tinyint(2),
              BigBlind decimal(4,2),
              PRIMARY KEY (ActionID)
            ) ENGINE = MYISAM;"""
tableCreate = """ CREATE TABLE tableFeatures
            ( ActionID int NOT NULL AUTO_INCREMENT,
              NumChecksGame tinyint(2),
              LastToAct tinyint(2),
              LastToActStack decimal(10,2),
              FinalPotLastHandTable decimal(10,2),
              CBisCheckRaise tinyint(1),
              BetsRaisesGame tinyint(2),
              BetsRaisesPF tinyint(2),
              BetsRaisesF tinyint(2),
              BetsRaisesT tinyint(2),
              BetsRaisesR tinyint(2),
              NumPairsFlop tinyint(2),
              NumPairsTurn tinyint(2),
              NumPairsRiver tinyint(2),
              TwoToFlushDrawFlop tinyint(1),
              ThreeToFlushDrawFlop tinyint(1),
              FlushTurned tinyint(1),
              FlushRivered tinyint(1),
              HighCardFlop tinyint(2),
              HighCardTurn tinyint(2),
              HighCardRiver tinyint(2),
              RangeFlop tinyint(2),
              RangeTurn tinyint(2),
              RangeRiver tinyint(2),
              TwoToStraightDrawFlop tinyint(1),
              TwoToStraightDrawTurn tinyint(1),
              ThreeToStraightFlop tinyint(1),
              ThreeOrMoreToStraightTurn tinyint(1),
              ThreeOrMoreToStraightRiver tinyint(1),
              TurnOverCard tinyint(1),
              RiverOverCard tinyint(1),
              NumFaceCardsFlop tinyint(2),
              NumFaceCardsTurn tinyint(2),
              NumFaceCardsRiver tinyint(2),
              AvgCardRankFlop decimal(4,2),
              AvgCardRankTurn decimal(4,2),
              AvgCardRankRiver decimal(4,2),
              TurnBrick tinyint(1),
              RiverBrick tinyint(1),
              MeanOtherStackRelBBRelSelf decimal(6,2),
              SDOtherStackRelBBRelSelf decimal(6,2),
              MaxOtherStackRelBBRelSelf decimal(6,2),
              MinOtherStackRelBBRelSelf decimal(6,2),
              AggressorPos tinyint(2),
              AggInPosVsMe tinyint(1),
              AggStack decimal(10,2),
              IsAgg tinyint(1),
              SeatRelDealerRelNP tinyint(2),
              EffectiveStack decimal(10,2),
              ESvsAgg decimal(10,2),
              StackToPot decimal(8,2),
              IsSB tinyint(1),
              IsBB tinyint(1),
              InvestedThisGame decimal(10,2),
              PRIMARY KEY (ActionID)
            ) ENGINE = MYISAM;"""
columnTempCreate = """ CREATE TABLE columnFeaturesTemp
            ( ActionID int NOT NULL AUTO_INCREMENT,
              Action varchar(10),
              LastAction varchar(10),
              AllFoldPct decimal(4,3),
              AllCheckPct decimal(4,3),
              AllCallPct decimal(4,3),
              AllBetPct decimal(4,3),
              AllRaisePct decimal(4,3),
              PreflopFoldPct decimal(4,3),
              PreflopCheckPct decimal(4,3),
              PreflopCallPct decimal(4,3),
              PreflopBetPct decimal(4,3),
              PreflopRaisePct decimal(4,3),
              FlopFoldPct decimal(4,3),
              FlopCheckPct decimal(4,3),
              FlopCallPct decimal(4,3),
              FlopBetPct decimal(4,3),
              FlopRaisePct decimal(4,3),
              TurnFoldPct decimal(4,3),
              TurnCheckPct decimal(4,3),
              TurnCallPct decimal(4,3),
              TurnBetPct decimal(4,3),
              TurnRaisePct decimal(4,3),
              RiverFoldPct decimal(4,3),
              RiverCheckPct decimal(4,3),
              RiverCallPct decimal(4,3),
              RiverBetPct decimal(4,3),
              RiverRaisePct decimal(4,3),
              VPIP decimal(4,3),
              NetAtTable decimal(10,2),
              ThreeBetPct decimal(4,3),
              SeeSDPct decimal(4,3),
              WinWhenSeeFlopPct decimal(4,3),
              WinWithoutSDPct decimal(4,3),
              WinAtSDPct decimal(4,3),
              ContBetPct decimal(4,3),
              BetRiverPct decimal(4,3),
              CallOrRaisePFRPct decimal(4,3),
              FoldToCBetPct decimal(4,3),
              CallCBetPct decimal(4,3),
              RaiseCBetPct decimal(4,3),
              FoldToFlopBetPct decimal(4,3),
              CallFlopBetPct decimal(4,3),
              RaiseFlopBetPct decimal(4,3),
              NetLastHandRelSS decimal(8,2),
              PartInLastHand tinyint(1),
              PRIMARY KEY (ActionID)
            ) ENGINE = MYISAM;"""
columnCreate = """ CREATE TABLE columnFeatures
            ( ActionID int NOT NULL AUTO_INCREMENT,
              LastAction varchar(10),
              AllFoldPct decimal(4,3),
              AllCheckPct decimal(4,3),
              AllCallPct decimal(4,3),
              AllBetPct decimal(4,3),
              AllRaisePct decimal(4,3),
              PreflopFoldPct decimal(4,3),
              PreflopCheckPct decimal(4,3),
              PreflopCallPct decimal(4,3),
              PreflopBetPct decimal(4,3),
              PreflopRaisePct decimal(4,3),
              FlopFoldPct decimal(4,3),
              FlopCheckPct decimal(4,3),
              FlopCallPct decimal(4,3),
              FlopBetPct decimal(4,3),
              FlopRaisePct decimal(4,3),
              TurnFoldPct decimal(4,3),
              TurnCheckPct decimal(4,3),
              TurnCallPct decimal(4,3),
              TurnBetPct decimal(4,3),
              TurnRaisePct decimal(4,3),
              RiverFoldPct decimal(4,3),
              RiverCheckPct decimal(4,3),
              RiverCallPct decimal(4,3),
              RiverBetPct decimal(4,3),
              RiverRaisePct decimal(4,3),
              VPIP decimal(4,3),
              NetAtTable decimal(10,2),
              ThreeBetPct decimal(4,3),
              SeeSDPct decimal(4,3),
              WinWhenSeeFlopPct decimal(4,3),
              WinWithoutSDPct decimal(4,3),
              WinAtSDPct decimal(4,3),
              ContBetPct decimal(4,3),
              BetRiverPct decimal(4,3),
              CallOrRaisePFRPct decimal(4,3),
              FoldToCBetPct decimal(4,3),
              CallCBetPct decimal(4,3),
              RaiseCBetPct decimal(4,3),
              FoldToFlopBetPct decimal(4,3),
              CallFlopBetPct decimal(4,3),
              RaiseFlopBetPct decimal(4,3),
              NetLastHandRelSS decimal(8,2),
              PartInLastHand tinyint(1),
              PRIMARY KEY (ActionID)
            ) ENGINE = MYISAM;"""
oldCreate = """CREATE TABLE featuresOld
            ( ActionID int NOT NULL AUTO_INCREMENT,
              Action varchar(10),
              Round varchar(10),
              FacingBet tinyint(2),
              AmountToCall decimal(10,2),
              CurrentPot decimal(10,2),
              NumPlayersStart tinyint(2),
              NumPlayersLeft tinyint(2),
              BigBlind decimal(4,2),
              NumChecksGame tinyint(2),
              LastToAct tinyint(2),
              LastToActStack decimal(10,2),
              FinalPotLastHandTable decimal(10,2),
              CBisCheckRaise tinyint(1),
              BetsRaisesGame tinyint(2),
              BetsRaisesPF tinyint(2),
              BetsRaisesF tinyint(2),
              BetsRaisesT tinyint(2),
              BetsRaisesR tinyint(2),
              NumPairsFlop tinyint(2),
              NumPairsTurn tinyint(2),
              NumPairsRiver tinyint(2),
              TwoToFlushDrawFlop tinyint(1),
              ThreeToFlushDrawFlop tinyint(1),
              FlushTurned tinyint(1),
              FlushRivered tinyint(1),
              HighCardFlop tinyint(2),
              HighCardTurn tinyint(2),
              HighCardRiver tinyint(2),
              RangeFlop tinyint(2),
              RangeTurn tinyint(2),
              RangeRiver tinyint(2),
              TwoToStraightDrawFlop tinyint(1),
              TwoToStraightDrawTurn tinyint(1),
              ThreeToStraightFlop tinyint(1),
              ThreeOrMoreToStraightTurn tinyint(1),
              ThreeOrMoreToStraightRiver tinyint(1),
              TurnOverCard tinyint(1),
              RiverOverCard tinyint(1),
              NumFaceCardsFlop tinyint(2),
              NumFaceCardsTurn tinyint(2),
              NumFaceCardsRiver tinyint(2),
              AvgCardRankFlop decimal(4,2),
              AvgCardRankTurn decimal(4,2),
              AvgCardRankRiver decimal(4,2),
              TurnBrick tinyint(1),
              RiverBrick tinyint(1),
              MeanOtherStackRelBBRelSelf decimal(6,2),
              SDOtherStackRelBBRelSelf decimal(6,2),
              MaxOtherStackRelBBRelSelf decimal(6,2),
              MinOtherStackRelBBRelSelf decimal(6,2),
              AggressorPos tinyint(2),
              AggInPosVsMe tinyint(1),
              AggStack decimal(10,2),
              IsAgg tinyint(1),
              SeatRelDealerRelNP tinyint(2),
              EffectiveStack decimal(10,2),
              ESvsAgg decimal(10,2),
              StackToPot decimal(8,2),
              IsSB tinyint(1),
              IsBB tinyint(1),
              InvestedThisGame decimal(10,2),
              LastAction varchar(10),
              AllFoldPct decimal(4,3),
              AllCheckPct decimal(4,3),
              AllCallPct decimal(4,3),
              AllBetPct decimal(4,3),
              AllRaisePct decimal(4,3),
              PreflopFoldPct decimal(4,3),
              PreflopCheckPct decimal(4,3),
              PreflopCallPct decimal(4,3),
              PreflopBetPct decimal(4,3),
              PreflopRaisePct decimal(4,3),
              FlopFoldPct decimal(4,3),
              FlopCheckPct decimal(4,3),
              FlopCallPct decimal(4,3),
              FlopBetPct decimal(4,3),
              FlopRaisePct decimal(4,3),
              TurnFoldPct decimal(4,3),
              TurnCheckPct decimal(4,3),
              TurnCallPct decimal(4,3),
              TurnBetPct decimal(4,3),
              TurnRaisePct decimal(4,3),
              RiverFoldPct decimal(4,3),
              RiverCheckPct decimal(4,3),
              RiverCallPct decimal(4,3),
              RiverBetPct decimal(4,3),
              RiverRaisePct decimal(4,3),
              VPIP decimal(4,3),
              NetAtTable decimal(10,2),
              ThreeBetPct decimal(4,3),
              SeeSDPct decimal(4,3),
              WinWhenSeeFlopPct decimal(4,3),
              WinPctWithoutSD decimal(4,3),
              WinAtSDPct decimal(4,3),
              ContBetPct decimal(4,3),
              BetRiverPct decimal(4,3),
              CallOrRaisePFRPct decimal(4,3),
              FoldToCBetPct decimal(4,3),
              CallCBetPct decimal(4,3),
              RaiseCBetPct decimal(4,3),
              FoldToFlopBetPct decimal(4,3),
              CallFlopBetPct decimal(4,3),
              RaiseFlopBetPct decimal(4,3),
              NetLastHandRelSS decimal(8,2),
              PartInLastHand tinyint(1),
              PRIMARY KEY (ActionID)
            ) ENGINE = MYISAM;"""
newTempCreate = """CREATE TABLE featuresNewTemp
            ( ActionID int NOT NULL AUTO_INCREMENT,
              Action varchar(10),
              sdVPIP decimal(8,4),
              AllAggFactor decimal(8,4),
              PreflopAggFactor decimal(8,4),
              FlopAggFactor decimal(8,4),
              TurnAggFactor decimal(8,4),
              RiverAggFactor decimal(8,4),
              PRIMARY KEY (ActionID)
            ) ENGINE = MYISAM;"""
newCreate = newTempCreate.replace('featuresNewTemp','featuresNew')
mainCreate = """CREATE TABLE features
            ( ActionID int NOT NULL AUTO_INCREMENT,
              Action varchar(10),
              Round varchar(10),
              FacingBet tinyint(2),
              AmountToCall decimal(10,2),
              CurrentPot decimal(10,2),
              NumPlayersStart tinyint(2),
              NumPlayersLeft tinyint(2),
              BigBlind decimal(4,2),
              NumChecksGame tinyint(2),
              LastToAct tinyint(2),
              LastToActStack decimal(10,2),
              FinalPotLastHandTable decimal(10,2),
              CBisCheckRaise tinyint(1),
              BetsRaisesGame tinyint(2),
              BetsRaisesPF tinyint(2),
              BetsRaisesF tinyint(2),
              BetsRaisesT tinyint(2),
              BetsRaisesR tinyint(2),
              NumPairsFlop tinyint(2),
              NumPairsTurn tinyint(2),
              NumPairsRiver tinyint(2),
              TwoToFlushDrawFlop tinyint(1),
              ThreeToFlushDrawFlop tinyint(1),
              FlushTurned tinyint(1),
              FlushRivered tinyint(1),
              HighCardFlop tinyint(2),
              HighCardTurn tinyint(2),
              HighCardRiver tinyint(2),
              RangeFlop tinyint(2),
              RangeTurn tinyint(2),
              RangeRiver tinyint(2),
              TwoToStraightDrawFlop tinyint(1),
              TwoToStraightDrawTurn tinyint(1),
              ThreeToStraightFlop tinyint(1),
              ThreeOrMoreToStraightTurn tinyint(1),
              ThreeOrMoreToStraightRiver tinyint(1),
              TurnOverCard tinyint(1),
              RiverOverCard tinyint(1),
              NumFaceCardsFlop tinyint(2),
              NumFaceCardsTurn tinyint(2),
              NumFaceCardsRiver tinyint(2),
              AvgCardRankFlop decimal(4,2),
              AvgCardRankTurn decimal(4,2),
              AvgCardRankRiver decimal(4,2),
              TurnBrick tinyint(1),
              RiverBrick tinyint(1),
              MeanOtherStackRelBBRelSelf decimal(6,2),
              SDOtherStackRelBBRelSelf decimal(6,2),
              MaxOtherStackRelBBRelSelf decimal(6,2),
              MinOtherStackRelBBRelSelf decimal(6,2),
              AggressorPos tinyint(2),
              AggInPosVsMe tinyint(1),
              AggStack decimal(10,2),
              IsAgg tinyint(1),
              SeatRelDealerRelNP tinyint(2),
              EffectiveStack decimal(10,2),
              ESvsAgg decimal(10,2),
              StackToPot decimal(8,2),
              IsSB tinyint(1),
              IsBB tinyint(1),
              InvestedThisGame decimal(10,2),
              LastAction varchar(10),
              AllFoldPct decimal(4,3),
              AllCheckPct decimal(4,3),
              AllCallPct decimal(4,3),
              AllBetPct decimal(4,3),
              AllRaisePct decimal(4,3),
              PreflopFoldPct decimal(4,3),
              PreflopCheckPct decimal(4,3),
              PreflopCallPct decimal(4,3),
              PreflopBetPct decimal(4,3),
              PreflopRaisePct decimal(4,3),
              FlopFoldPct decimal(4,3),
              FlopCheckPct decimal(4,3),
              FlopCallPct decimal(4,3),
              FlopBetPct decimal(4,3),
              FlopRaisePct decimal(4,3),
              TurnFoldPct decimal(4,3),
              TurnCheckPct decimal(4,3),
              TurnCallPct decimal(4,3),
              TurnBetPct decimal(4,3),
              TurnRaisePct decimal(4,3),
              RiverFoldPct decimal(4,3),
              RiverCheckPct decimal(4,3),
              RiverCallPct decimal(4,3),
              RiverBetPct decimal(4,3),
              RiverRaisePct decimal(4,3),
              VPIP decimal(4,3),
              NetAtTable decimal(10,2),
              ThreeBetPct decimal(4,3),
              SeeSDPct decimal(4,3),
              WinWhenSeeFlopPct decimal(4,3),
              WinPctWithoutSD decimal(4,3),
              WinAtSDPct decimal(4,3),
              ContBetPct decimal(4,3),
              BetRiverPct decimal(4,3),
              CallOrRaisePFRPct decimal(4,3),
              FoldToCBetPct decimal(4,3),
              CallCBetPct decimal(4,3),
              RaiseCBetPct decimal(4,3),
              FoldToFlopBetPct decimal(4,3),
              CallFlopBetPct decimal(4,3),
              RaiseFlopBetPct decimal(4,3),
              NetLastHandRelSS decimal(8,2),
              PartInLastHand tinyint(1),
              sdVPIP decimal(8,4),
              AllAggFactor decimal(8,4),
              PreflopAggFactor decimal(8,4),
              FlopAggFactor decimal(8,4),
              TurnAggFactor decimal(8,4),
              RiverAggFactor decimal(8,4),
              PRIMARY KEY (ActionID)
            ) ENGINE = MYISAM;"""

startTime = datetime.now()
creations = [quickCreate,tableCreate,columnCreate,columnTempCreate,
             oldCreate,newCreate,newTempCreate,mainCreate]
for q in creations:
    cur.execute(q)

print "Checkpoint, tables created:", datetime.now()-startTime

# get cols in order from creation queries
tableCols = {}
for t,q in zip(tables+['columntemp','old','new','newtemp','main'],creations):
    cols = [f.strip()[:-1] for f in q.split('\n')][2:-2]
    tableCols[t] = [f[:f.find(' ')] for f in cols]

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

######################## POPULATE COLUMN FEATURES #############################
os.chdir('features/column')
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
# TODO: fix "ALL" columns, because they're giving pct's that are >1.0
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
        f = open('{}{}Pct.txt'.format(r,a[:1].upper()+a[1:]),'ab')
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
        ' '.join('{}.txt'.format(fName) for fName in tableCols['column'])))

#import CSV to columnFeatures (temp table->remove blinds->real table)
cur.execute("""LOAD DATA INFILE '{}/features.csv'
                INTO TABLE columnFeaturesTemp
                FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                ({});""".format(os.getcwd(), ','.join(tableCols['column'])))
cur.execute("""INSERT INTO columnFeatures
            SELECT {}
            FROM columnFeaturesTemp
            WHERE Action!="blind" AND Action!="deadblind"
            ;""".format(','.join(tableCols['column'])))
cur.execute('DROP TABLE columnFeaturesTemp;')
print "Checkpoint, columnFeatures table populated:", datetime.now()-startTime
################## MERGE TABLES TO FEATURESOLD, DELETE ########################
cur.execute("""INSERT INTO featuresOld
            SELECT *
            FROM quickFeatures AS q
            INNER JOIN tableFeatures AS t ON q.ActionID=t.ActionID
            INNER JOIN columnFeatures AS c ON q.ActionID=c.ActionID
            ;""")
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
        p = p[0]
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
        ' '.join('{}.txt'.format(fName) for fName in tableCols['new'])))

# import new features to table
cur.execute("""LOAD DATA INFILE '{}/features.csv'
                INTO TABLE featuresNewTemp
                FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                ({});""".format(os.getcwd(), ','.join(tableCols['new'])))
cur.execute("""INSERT INTO featuresNew
            SELECT {}
            FROM featuresNewTemp
            WHERE Action!="blind" AND Action!="deadblind"
            ;""".format(','.join(tableCols['new'])))
cur.execute('DROP TABLE newFeaturesTemp;')
print "Checkpoint, newFeatures populated:", datetime.now()-startTime
################## MERGE OLD AND NEW TO FEATURES, DELETE ######################
cur.execute("""INSERT INTO features
            SELECT *
            FROM featuresOld AS o
            INNER JOIN featuresNew AS n ON o.ActionID=n.ActionID
            ;""")
cur.execute('DROP TABLE featuresOld;')
cur.execute('DROP TABLE featuresNew;')
print "END! features populated:", datetime.now()-startTime