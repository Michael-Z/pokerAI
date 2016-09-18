import os
import shutil
import datetime
import calendar
from copy import copy
import locale
from bisect import bisect_left
import codecs
import multiprocessing
import MySQLdb
import csv
import itertools
import random
from uuid import uuid4

testing = True
testSize = 3000

locale.setlocale(locale.LC_NUMERIC, 'en_US.utf8')

cardNumRangeT = [str(i) for i in range(2,10)] + ['T','J','Q','K','A']
cardNumRange10 = [str(i) for i in range(2,11)] + ['J','Q','K','A']
cardSuitRange = ['d','c','h','s']
deckT = [str(i) + str(j) for i in cardNumRangeT for j in cardSuitRange]
deck10 = [str(i) + str(j) for i in cardNumRange10 for j in cardSuitRange]
actions = ['blind','deadblind','fold','check','call','bet','raise']

# convert to float in the face of errors (commas for decimals)
def toFloat(s):
    if len(s)>=3 and s[-3]==',':
        s[-3] = '.'
    return locale.atof(s)

# quick convert of numeric list to list of strings
def toStrings(l):
    l = list(l)
    if type(l[0])==float:
        return [str(round(x,3)) for x in l]
    return [str(x) for x in l]

def readABSfile(filename):
    # HANDS INFORMATION
    with open(filename,'r') as f:
        startString = "Stage #"
        fileContents = [startString + theRest for theRest in f.read().replace('\r','').split(startString)]
        fileContents = fileContents[1:]
    
    data = []
    lineToRead = True
    
    for i,hand in enumerate(fileContents):
        try:
            ###################### HAND INFORMATION ###########################
            # add small and big blinds
            fn = filename[filename.find("data_raw")+8:]
            bb = float(fn[:fn.find("/")])
            if bb==0.25:
                sb = 0.1
            else:
                sb = bb/2
            # add date
            dateStart = hand.find("-") + 2
            dateEnd = dateStart + 10
            dateStr = hand[dateStart:dateEnd]
            dateObj = datetime.datetime.strptime(dateStr, '%Y-%m-%d').date()
            # add time
            timeStart = dateEnd + 1
            timeEnd = timeStart + 8
            timeStr = hand[timeStart:timeEnd]
            timeObj = datetime.datetime.strptime(timeStr, '%H:%M:%S').time()
            # add table
            tableStart = hand.find("\n") + 8
            tableEnd = tableStart + hand[tableStart:].find("(") - 1
            table = hand[tableStart:tableEnd]
            assert len(table)<=22
            # add dealer
            dealerStart = hand.find("Seat #") + 6
            dealerEnd = dealerStart + hand[dealerStart:].find(" ")
            dealer = int(hand[dealerStart:dealerEnd])
            # add numPlayers
            lines = [s.rstrip() for s in hand.split('\n')]
            numPlayers = 0
            j = 2
            while lines[j][:5]=="Seat ":
                numPlayers += 1
                j += 1
            # add board
            boardLine = lines[lines.index("*** SUMMARY ***") + 2]
            if boardLine[:5]=="Board":
                board = boardLine[7:-1].split()
            else:
                board = []
    
            ####################### PLAYER INFORMATION ########################
            
            # initialize...
            cp = 0
            cb = 0
            npl = 0
            rd = "Preflop"
            seats = {}
            startStacks = {}
            stacks = {}
            holeCards = {}
            roundInvestments = {}
            lenBoard = 0
            isAllIn = False
            lastNewRoundLine = -1
            winnings = {}
            
            # go through lines to populate seats
            n = 2
            while lines[n][:4]=="Seat":
                line = lines[n]
                playerStart = line.find("-")+2
                playerEnd = playerStart + line[playerStart:].find(' ')
                player = line[playerStart:playerEnd]
                assert not '$' in player and not ')' in player, "bad player name"
                s = line[(line.find(' ')+1):]
                seats[player] = int(s[:s.find(' ')])
                startStacks[player] = toFloat(line[(line.find("(")+2):(line.find("in chips")-1)])
                assert startStacks[player]!=0, "start stack of 0"
                stacks[player] = startStacks[player]
                holeCards[player] = [None, None]
                winnings[player] = 0.
                roundInvestments[player] = 0
                roundActionNum = 1
                npl += 1
                n += 1
            
            # seat nums without skipping seats
            relSeats = {p: bisect_left(seats.values(), seats[p])
                        for p in seats}
            
            # if dealer button is on empty seat, move it back
            while not dealer in seats.values() and dealer > 0:
                dealer -= 1
            
            # go through again to...
            # collect hole card info, check for bad names, find winner
            for j,line in enumerate(lines):
                maybePlayerName = line[:line.find(" ")]
                if len(maybePlayerName)==22:
                    assert maybePlayerName in seats.keys()
                if maybePlayerName in seats.keys() and line.find("Shows")>=0:
                    hc = line[(line.find("[")+1):line.find("]")]
                    hc = hc.split()
                    holeCards[maybePlayerName] = hc
                elif 'Collects' in line:
                    amt = line[(line.find('$')+1):]
                    amt = float(amt[:amt.find(' ')])
                    winnings[maybePlayerName] += amt
            
            for j,line in enumerate(lines):
                # skip SUMMARY section by changing lineToRead when encounter it
                # stop skipping once encounter "Stage"
                if line.find("Stage")>=0:
                    lineToRead = True
                elif line=="*** SUMMARY ***":
                    lineToRead = False
            
                if lineToRead:
                    newRow = {}
                    maybePlayerName = line[:line.find(" ")]
                    
                    if line[:5]=="Stage":
                        stage = str(uuid4())
                    
                    elif line[:3]=="***":
                        nar = j - lastNewRoundLine
                        lastNewRoundLine = j
                        for key in roundInvestments:
                            roundInvestments[key] = 0
                        rdStart = line.find(" ")+1
                        rdEnd = rdStart + line[rdStart:].find("*") - 1
                        rd = line[rdStart:rdEnd].title().strip()
                        if rd!='Pocket Cards':
                            if nar>1:
                                assert roundActionNum!=1, "round with one action"
                            roundActionNum = 1
                            cb = 0
                        if rd=='Flop':
                            lenBoard = 3
                        elif rd=='Turn':
                            lenBoard = 4
                        elif rd=='River':
                            lenBoard = 5
                        elif rd.find("Card")>=0:
                            rd = 'Preflop'
                        elif rd=='Show Down':
                            continue
                        else:
                            raise ValueError
                    
                    # create new row IF row is an action (starts with encrypted player name)
                    elif maybePlayerName in seats.keys():
                        seat = seats[maybePlayerName]
                        relSeat = relSeats[maybePlayerName]
                        fullA = line[(line.find("-") + 2):].strip()
                        isAllIn = fullA.find("All-In")>=0
                        if fullA.find("Posts")>=0:
                            if fullA.find('dead')>=0:
                                a = 'deadblind'
                                amt = fullA[fullA.find("$")+1:]
                                amt = toFloat(amt[:amt.find(" ")])
                            else:
                                a = 'blind'
                                amt = toFloat(fullA[fullA.find("$")+1:])
                            cp += amt
                            roundInvestments[maybePlayerName] += amt
                            oldCB = copy(cb)
                            cb = amt
                            stacks[maybePlayerName] -= amt
                        elif fullA=="Folds":
                            a = 'fold'
                            amt = 0.
                            npl -= 1
                            oldCB = copy(cb)
                        elif fullA.find('Checks')>=0:
                            a = 'check'
                            amt = 0.
                            oldCB = copy(cb)
                        elif fullA.find("Bets")>=0:
                            a = 'bet'
                            amt = toFloat(fullA[(fullA.find("$")+1):])
                            cp += amt
                            roundInvestments[maybePlayerName] += amt
                            oldCB = copy(cb)
                            cb = amt
                            stacks[maybePlayerName] -= amt
                        elif fullA.find('Raises')>=0:
                            a = 'raise'
                            amt = toFloat(fullA[(fullA.find('to')+4):])
                            roundInvestments[maybePlayerName] = amt
                            cp += amt
                            oldCB = copy(cb)
                            cb = amt
                            stacks[maybePlayerName] -= amt
                        elif fullA.find('Calls')>=0:
                            a = 'call'
                            amt = toFloat(fullA[(fullA.find('$')+1):])
                            roundInvestments[maybePlayerName] += amt
                            cp += amt
                            stacks[maybePlayerName] -= amt
                            oldCB = copy(cb)
                            if cb<amt:
                                cb = amt
                        elif isAllIn:
                            revFullA = fullA[::-1]
                            amt = toFloat(revFullA[:revFullA.find('$')][::-1])
                            if cb==0:
                                if roundActionNum<=2 and rd=='Preflop':
                                    a = 'blind'
                                else:
                                    a = 'bet'
                                roundInvestments[maybePlayerName] += amt
                            elif amt > cb:
                                a = 'raise'
                                roundInvestments[maybePlayerName] = amt
                            else:
                                a = 'call'
                                roundInvestments[maybePlayerName] += amt
                            cp += amt
                            oldCB = copy(cb)
                            if cb<amt:
                                cb = amt
                            stacks[maybePlayerName] -= amt
                        else:
                            continue
                        if oldCB > (roundInvestments[maybePlayerName] - amt):
                            assert a!='bet', "illegal action"
                        else:
                            assert a!='call', "illegal action"
                        assert stacks[maybePlayerName]+amt >= 0
                        newRow = {'GameNum':stage,
                                  'RoundActionNum':roundActionNum,
                                  'SeatNum':seat,
                                  'RelSeatNum':relSeat,
                                  'Round':rd,
                                  'Player':maybePlayerName,
                                  'StartStack':round(startStacks[maybePlayerName],2),
                                  'CurrentStack':round(stacks[maybePlayerName] + amt,2),
                                  'Action':a,
                                  'Amount':round(amt,2),
                                  'AllIn':int(isAllIn),
                                  'CurrentBet':round(oldCB,2),
                                  'CurrentPot':round(cp-amt,2),
                                  'NumPlayersLeft':npl+1 if a=='fold' else npl,
                                  'Date': dateObj,
                                  'Time': timeObj,
                                  'SmallBlind': sb,
                                  'BigBlind': bb,
                                  'TableName': table.title(),
                                  'Dealer': dealer,
                                  'NumPlayers': numPlayers,
                                  'LenBoard': lenBoard,
                                  'InvestedThisRound': round(roundInvestments[maybePlayerName] - amt,2),
                                  'Winnings': round(winnings[maybePlayerName],2),
                                  'SeatRelDealer':(seat-dealer)*(seat>dealer)+
                                              (dealer-seat)*(dealer>seat),
                                  'Source':'abs'
                                  }
                        for ii in [1,2]:
                            c = holeCards[maybePlayerName][ii-1]
                            if c is None:
                                newRow['HoleCard'+str(ii)] = -1
                            else:
                                newRow['HoleCard'+str(ii)] = deck10.index(c)
                        for ii in range(1,6):
                            if lenBoard >= ii:
                                newRow['Board'+str(ii)] = deck10.index(board[ii-1])
                            else:
                                newRow['Board'+str(ii)] = -1
                        for act in actions[2:]:
                            newRow['is{}'.format(act)] = int(a==act)
                        data.append(newRow)
                        roundActionNum += 1
            if data[-1]['RoundActionNum']==1:
                data.pop()
        except (ValueError, IndexError, KeyError, TypeError, AttributeError, ZeroDivisionError, AssertionError):
            pass
    
    return data
                
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################

def readFTPfile(filename):
    with codecs.open(filename, encoding='utf-8') as f:
        startString = "Full Tilt Poker Game #"
        fileContents = [startString + theRest for theRest in f.read().replace('\r','').split(startString)]
        fileContents = fileContents[1:]
    
    data = []
    lineToRead = True
    
    for i,hand in enumerate(fileContents):
        try:
            assert not "@" in hand and not "\x16" in hand, "corrupted data"
            ####################### HAND INFORMATION ##############################
            # add small and big blinds
            fn = filename[filename.find("data_raw")+8:]
            bb = float(fn[:fn.find("/")])
            if bb==0.25:
                sb = 0.1
            else:
                sb = bb/2
            # add date
            dateEnd = hand.find("\n")
            dateStart = dateEnd - 10
            dateStr = hand[dateStart:dateEnd]
            dateObj = datetime.datetime.strptime(dateStr, '%Y/%m/%d').date()
            # add time
            timeEnd = dateStart - 6
            timeStart = timeEnd - 8
            timeStr = hand[timeStart:timeEnd].strip()
            timeObj = datetime.datetime.strptime(timeStr, '%H:%M:%S').time()
            # add table
            tableStart = hand.find("Table") + 6
            tableEnd = tableStart + hand[tableStart:].find(" ")
            table = hand[tableStart:tableEnd]
            assert len(table)<=22
            # add dealer
            dealerStart = hand.find("seat #") + 6
            dealerEnd = dealerStart + hand[dealerStart:].find("\n")
            dealer = int(hand[dealerStart:dealerEnd])
            # add numPlayers
            lines = [s.rstrip() for s in hand.split('\n')]
            numPlayers = 0
            j = 1
            while lines[j][:5]=="Seat ":
                if lines[j].find("sitting out")==-1:
                    numPlayers += 1
                j += 1
            # add board
            boardLine = lines[lines.index("*** SUMMARY ***") + 2]
            if boardLine[:5]=="Board":
                board = boardLine[8:-1].split()
            else:
                board = []
        
            ########################## PLAYER INFORMATION #########################
            
            cp = 0
            cb = 0
            npl = 0
            rd = "Preflop"
            seats = {}
            startStacks = {}
            stacks = {}
            holeCards = {}
            roundInvestments = {}
            lenBoard = 0
            winnings = {}
            
            # go through lines to populate seats
            n = 1
            while lines[n][:4]=="Seat":
                line = lines[n]
                playerStart = line.find(":")+2
                playerEnd = playerStart + line[playerStart:].find(' ')
                player = line[playerStart:playerEnd]
                assert not '$' in player and not ')' in player, "bad player name"
                s = line[(line.find(' ')+1):]
                seats[player] = int(s[:s.find(':')])
                startStacks[player] = toFloat(line[(line.find("(")+2):line.find(")")])
                assert startStacks[player]!=0, "start stack of 0"
                stacks[player] = startStacks[player]
                holeCards[player] = [None, None]
                winnings[player] = 0.
                roundInvestments[player] = 0
                roundActionNum = 1
                n += 1
                if line.find('sitting out')==-1:
                    npl += 1
                      
            # seat nums without skipping seats
            relSeats = {p: bisect_left(seats.values(), seats[p])
                        for p in seats}
            
            # if dealer button is on empty seat, move it back
            while not dealer in seats.values() and dealer > 0:
                dealer -= 1            

            # go through again to...
            # collect hole card info, check for bad names, find winner
            for line in lines:
                maybePlayerName = line[:line.find(" ")]
                if len(maybePlayerName)==22 and not line.find("sits down")>=0:
                    assert maybePlayerName in seats.keys(), hand
                if maybePlayerName in seats.keys() and line.find("shows [")>=0:
                    hc = line[(line.find("[")+1):line.find("]")]
                    hc = hc.split()
                    holeCards[maybePlayerName] = hc
                elif 'wins' in line:
                    amt = line[(line.find('$')+1):]
                    amt = float(amt[:amt.find(')')])
                    winnings[maybePlayerName] += amt
                
            for line in lines:
                # skip SUMMARY section by changing lineToRead when encounter it
                # stop skipping once encounter "Stage" or "Game" or whatever
                if line.find("Game")>=0:
                    lineToRead = True
                elif line=="*** SUMMARY ***":
                    lineToRead = False
            
                if lineToRead:
                    newRow = {}
                    maybePlayerName = line[:line.find(" ")]
                    
                    if line[:20]=="Full Tilt Poker Game":
                        stage = str(uuid4())
                        
                    elif line[:3]=="***":
                        for key in roundInvestments:
                            roundInvestments[key] = 0
                        rdStart = line.find(" ")+1
                        rdEnd = rdStart + line[rdStart:].find("*") - 1
                        rd = line[rdStart:rdEnd]
                        rd = rd.title().strip()
                        if rd!="Hole Cards":
                            assert roundActionNum!=1, "round with one action"
                            roundActionNum = 1
                            cb = 0
                        if rd=='Flop':
                            lenBoard = 3
                        elif rd=='Turn':
                            lenBoard = 4
                        elif rd=='River':
                            lenBoard = 5
                        elif rd.find("Card")>=0:
                            rd = 'Preflop'
                        elif rd=='Show Down':
                            continue
                        else:
                            raise ValueError
                    
                    # create new row IF row is an action (starts with encrypted player name)
                    elif maybePlayerName in seats.keys():
                        seat = seats[maybePlayerName]
                        relSeat = relSeats[maybePlayerName]
                        fullA = line[(line.find(" ") + 1):].strip()
                        isAllIn = fullA.find("all in")>=0
                        if fullA.find("posts")>=0:
                            if fullA.find('dead')>=0:
                                a = 'deadblind'
                                amt = toFloat(fullA[fullA.find("$")+1:])
                            else:
                                a = 'blind'
                                amt = toFloat(fullA[fullA.find("$")+1:])
                            cp += amt
                            roundInvestments[maybePlayerName] += amt
                            oldCB = copy(cb)
                            if cb<amt:
                                cb = amt
                            stacks[maybePlayerName] -= amt
                        elif fullA=="folds":
                            a = 'fold'
                            amt = 0.
                            npl -= 1
                            seats.pop(maybePlayerName)
                            oldCB = copy(cb)
                        elif fullA.find('checks')>=0:
                            a = 'check'
                            amt = 0.
                            oldCB = copy(cb)
                        elif fullA.find("bets")>=0 and fullA.find("Uncalled")==-1:
                            a = 'bet'
                            if isAllIn or fullA.find(", ")>=0:
                                amt = toFloat(fullA[(fullA.find('$')+1):fullA.find(", ")])
                            else:
                                amt = toFloat(fullA[(fullA.find('$')+1):])
                            cp += amt
                            roundInvestments[maybePlayerName] += amt
                            oldCB = copy(cb)
                            cb = amt
                            stacks[maybePlayerName] -= amt
                        elif fullA.find('raises')>=0:
                            a = 'raise'
                            if isAllIn or fullA.find(", ")>=0:
                                amt = toFloat(fullA[(fullA.find('$')+1):fullA.find(", ")])
                            else:
                                amt = toFloat(fullA[(fullA.find('$')+1):])
                            roundInvestments[maybePlayerName] = amt
                            cp += amt
                            oldCB = copy(cb)
                            if cb<amt:
                                cb = amt
                            stacks[maybePlayerName] -= amt
                        elif fullA.find('calls')>=0:
                            a = 'call'
                            if isAllIn or fullA.find(", ")>=0:
                                amt = toFloat(fullA[(fullA.find('$')+1):fullA.find(", ")])
                            else:
                                amt = toFloat(fullA[(fullA.find('$')+1):])
                            cp += amt
                            roundInvestments[maybePlayerName] += amt
                            stacks[maybePlayerName] -= amt
                            oldCB = copy(cb)
                            if cb<amt:
                                cb = amt
                        elif fullA=='is sitting out':
                            npl -= 1
                            seats.pop(maybePlayerName)
                            continue
                        else:
                            continue
                        if oldCB > (roundInvestments[maybePlayerName] - amt):
                            assert a!='bet', "illegal action"
                        else:
                            assert a!='call', "illegal action"
                        assert stacks[maybePlayerName]+amt >= 0
                        newRow = {'GameNum':stage,
                                  'RoundActionNum':roundActionNum,
                                  'SeatNum':seat,
                                  'RelSeatNum':relSeat,
                                  'Round':rd,
                                  'Player':maybePlayerName,
                                  'StartStack':round(startStacks[maybePlayerName],2),
                                  'CurrentStack':round(stacks[maybePlayerName] + amt,2),
                                  'Action':a,
                                  'Amount':round(amt,2),
                                  'AllIn':int(isAllIn),
                                  'CurrentPot':round(cp-amt,2),
                                  'CurrentBet':round(oldCB,2),
                                  'NumPlayersLeft': npl+1 if a=='fold' else npl,
                                  'Date': dateObj,
                                  'Time': timeObj,
                                  'SmallBlind': sb,
                                  'BigBlind': bb,
                                  'TableName': table.title(),
                                  'Dealer': dealer,
                                  'NumPlayers': numPlayers,
                                  'LenBoard': lenBoard,
                                  'InvestedThisRound': round(roundInvestments[maybePlayerName] - amt,2),
                                  'Winnings': round(winnings[maybePlayerName],2),
                                  'SeatRelDealer':(seat-dealer)*(seat>dealer)+
                                              (dealer-seat)*(dealer>seat),
                                  'Source':'ftp'
                                  }
                        for ii in [1,2]:
                            c = holeCards[maybePlayerName][ii-1]
                            if c is None:
                                newRow['HoleCard'+str(ii)] = -1
                            else:
                                newRow['HoleCard'+str(ii)] = deckT.index(c)
                        for ii in range(1,6):
                            if lenBoard >= ii:
                                newRow['Board'+str(ii)] = deckT.index(board[ii-1])
                            else:
                                newRow['Board'+str(ii)] = -1
                        for act in actions[2:]:
                            newRow['is{}'.format(act)] = int(a==act)
                        data.append(newRow)
                        roundActionNum += 1
            if data[-1]['RoundActionNum']==1:
                data.pop()
        except (ValueError, IndexError, KeyError, TypeError, AttributeError, ZeroDivisionError, AssertionError):
            pass
        
    return data

###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################

def readONGfile(filename):
    with open(filename,'r') as f:
        startString = "***** History"
        fileContents = [startString + theRest for theRest in f.read().replace('\r','').split(startString)]
        fileContents = fileContents[1:]
    
    data = []
    lineToRead = True
    
    for i,hand in enumerate(fileContents):
        try:
            ####################### HAND INFORMATION ##############################
            # add small and big blinds
            fn = filename[filename.find("data_raw")+8:]
            bb = float(fn[:fn.find("/")])
            if bb==0.25:
                sb = 0.1
            else:
                sb = bb/2
            # add date
            monthStart = hand.find("Start hand:") + 16
            monthEnd = monthStart + 3
            dateStart = monthEnd + 1
            dateEnd = dateStart + 2
            yearStart = dateEnd + 19
            yearEnd = yearStart + 4
            monthConv = {v:k for k,v in enumerate(calendar.month_abbr)}
            dateObj = datetime.date(int(hand[yearStart:yearEnd]),
                                    int(monthConv[hand[monthStart:monthEnd]]),
                                    int(hand[dateStart:dateEnd]))
            # add time
            timeStart = dateEnd + 1
            timeEnd = timeStart + 8
            timeStr = hand[timeStart:timeEnd]
            timeObj = datetime.datetime.strptime(timeStr, '%H:%M:%S').time()
            # add table
            tableStart = hand.find("Table") + 7
            tableEnd = tableStart + hand[tableStart:].find(" ")
            table = hand[tableStart:tableEnd]
            assert len(table)<=22
            # add dealer
            dealerStart = hand.find("Button:") + 13
            dealerEnd = dealerStart + hand[dealerStart:].find("\n")
            dealer = int(hand[dealerStart:dealerEnd])
            # add numPlayers
            lines = [s.rstrip() for s in hand.split('\n')]
            numPlayers = 0
            j = 5
            while lines[j][:5]=="Seat ":
                if lines[j].find("sitting out")==-1:
                    numPlayers += 1
                j += 1
            # add board
            board = []
            flopStart = hand.find("Dealing flop")
            turnStart = hand.find("Dealing turn")
            riverStart = hand.find("Dealing river")
            if flopStart>=0:
                flopStart += 14
                flopEnd = flopStart + 10
                flop = hand[flopStart:flopEnd]
                board += flop.replace(',','').split()
            if turnStart>=0:
                turnStart += 14
                turnEnd = turnStart + 2
                turn = hand[turnStart:turnEnd]
                board.append(turn.replace(',',''))
            if riverStart>=0:
                riverStart += 15
                riverEnd = riverStart + 2
                river = hand[riverStart:riverEnd]
                board.append(river.replace(',',''))
            
            ########################## PLAYER INFORMATION #########################
            
            cp = 0
            cb = 0
            npl = 0
            rd = "Preflop"
            seats = {}
            startStacks = {}
            stacks = {}
            holeCards = {}
            roundInvestments = {}
            lenBoard = 0
            lastNewRoundLine = -1
            winnings = {}
            
            # go through lines to populate seats
            n = 5
            while lines[n][:4]=="Seat":
                line = lines[n]
                playerStart = line.find(":")+2
                playerEnd = playerStart + line[playerStart:].find(' ')
                player = line[playerStart:playerEnd]
                assert not '$' in player and not ')' in player, "bad player name"
                s = line[(line.find(' ')+1):]
                seats[player] = int(s[:s.find(':')])
                startStacks[player] = toFloat(line[(line.find("(")+2):line.find(")")])
                assert startStacks[player]!=0, "start stack of 0"
                stacks[player] = startStacks[player]
                holeCards[player] = [None, None]
                winnings[player] = 0.
                roundInvestments[player] = 0
                roundActionNum = 1
                npl += 1
                n += 1
            
            # seat nums without skipping seats
            relSeats = {p: bisect_left(seats.values(), seats[p])
                        for p in seats}
            
            # if dealer button is on empty seat, move it back
            while not dealer in seats.values() and dealer > 0:
                dealer -= 1
            
            # go through again to...
            # collect hole card info, check for bad names, find winner
            for line in lines:
                maybePlayerName = line[(line.find(":")+2):(line.find("(")-1)]
                if maybePlayerName in seats.keys() and line.find(", [")>=0:
                    hc = line[(line.find("[")+1):-1]
                    hc = hc.split(", ")
                    holeCards[maybePlayerName] = hc
                elif 'won by' in line:
                    amt = line[(line.find('($')+2):]
                    amt = float(amt[:amt.find(')')])
                    player = line[(line.find('won by')+7):]
                    player = player[:player.find(" ")]
                    winnings[player] += amt

            cardLines = [l for l in lines if l.find(", [")>=0]
            for line in cardLines:
                maybePlayerName = line[(line.find(":")+2):(line.find("(")-1)]
                if line.find("[")>=0:
                    hc = line[(line.find("[")+1):-1]
                    hc = hc.split(", ")
                    holeCards[maybePlayerName] = hc
                
            for line in lines:
                maybePlayerName = line[(line.find(":")+2):(line.find("(")-1)]
                if len(maybePlayerName)==22:
                    assert maybePlayerName in seats.keys()
            
            for j,line in enumerate(lines):
                # skip SUMMARY section by changing lineToRead when encounter it
                # stop skipping once encounter "Stage" or "Game" or whatever
                if line.find("History for hand")>=0:
                    lineToRead = True
                elif line=="Summary:":
                    lineToRead = False
            
                if lineToRead:
                    newRow = {}
                    maybePlayerName = line[:line.find(" ")]
                    
                    if line[:22]=="***** History for hand":
                        stage = str(uuid4())
                        
                    elif line[:3]=="---" and len(line)>3:
                        nar = j - lastNewRoundLine
                        lastNewRoundLine = j
                        for key in roundInvestments:
                            roundInvestments[key] = 0
                        rdStart = line.find("Dealing")+8
                        rdEnd = rdStart + line[rdStart:].find("[") - 1
                        rd = line[rdStart:rdEnd].title().strip()
                        if rd!='Pocket Cards':
                            if nar>1:
                                assert roundActionNum!=1, "round with one action"
                            roundActionNum = 1
                            cb = 0
                        if rd=='Flop':
                            lenBoard = 3
                        elif rd=='Turn':
                            lenBoard = 4
                        elif rd=='River':
                            lenBoard = 5
                        elif rd.find("Card")>=0:
                            rd = 'Preflop'
                        else:
                            raise ValueError
                    
                    # create new row IF row is an action (starts with encrypted player name)
                    elif maybePlayerName in seats.keys():
                        seat = seats[maybePlayerName]
                        relSeat = relSeats[maybePlayerName]
                        fullA = line[(line.find(" ") + 1):].strip()
                        isAllIn = fullA.find("all in")>=0
                        if fullA.find("posts")>=0:
                            a = 'blind'
                            amt = toFloat(fullA[fullA.find("$")+1:-1])
                            roundInvestments[maybePlayerName] += amt
                            cp += amt
                            oldCB = copy(cb)
                            if cb<amt:
                                cb = amt
                            stacks[maybePlayerName] -= amt
                        elif fullA=="folds":
                            a = 'fold'
                            amt = 0.
                            npl -= 1
                            oldCB = copy(cb)
                        elif fullA=='checks':
                            a = 'check'
                            amt = 0.
                            oldCB = copy(cb)
                        elif fullA.find("bets")>=0:
                            a = 'bet'
                            amtStart = fullA.find("$")+1
                            if isAllIn:
                                amt = toFloat(fullA[amtStart:(amtStart + fullA[amtStart:].find(" "))])
                                if cb>0:
                                    a = 'raise'
                                    roundInvestments[maybePlayerName] = amt
                                else:
                                    roundInvestments[maybePlayerName] += amt
                            else:
                                amt = toFloat(fullA[amtStart:])
                                roundInvestments[maybePlayerName] += amt
                            cp += amt
                            oldCB = copy(cb)
                            cb = amt
                            stacks[maybePlayerName] -= amt
                        elif fullA.find('raises')>=0:
                            a = 'raise'
                            if isAllIn or fullA.find(", ")>=0:
                                amt = toFloat(fullA[(fullA.find('$')+1):fullA.find(", ")])
                            else:
                                amt = toFloat(fullA[(fullA.find('to')+4):])
                            roundInvestments[maybePlayerName] = amt
                            cp += amt
                            oldCB = copy(cb)
                            if cb<amt:
                                cb = amt
                            stacks[maybePlayerName] -= amt
                        elif fullA.find('calls')>=0:
                            a = 'call'
                            if isAllIn:
                                amt = toFloat(fullA[(fullA.find("$")+1):(fullA.find("[")-1)])
                            else:
                                amt = toFloat(fullA[(fullA.find('$')+1):])
                            roundInvestments[maybePlayerName] += amt
                            cp += amt
                            oldCB = copy(cb)
                            stacks[maybePlayerName] -= amt
                            if cb<amt:
                                cb = amt
                        else:
                            continue
                        if oldCB > (roundInvestments[maybePlayerName] - amt):
                            assert a!='bet', "illegal action"
                        else:
                            assert a!='call', "illegal action"
                        assert stacks[maybePlayerName]+amt >= 0
                        newRow = {'GameNum':stage,
                                  'RoundActionNum':roundActionNum,
                                  'SeatNum':seat,
                                  'RelSeatNum':relSeat,
                                  'Round':rd,
                                  'Player':maybePlayerName,
                                  'StartStack':round(startStacks[maybePlayerName],2),
                                  'CurrentStack':round(stacks[maybePlayerName] + amt,2),
                                  'Action':a,
                                  'Amount':round(amt,2),
                                  'AllIn':int(isAllIn),
                                  'CurrentPot':round(cp-amt,2),
                                  'CurrentBet':round(oldCB,2),
                                  'NumPlayersLeft': npl+1 if a=='fold' else npl,
                                  'Date': dateObj,
                                  'Time': timeObj,
                                  'SmallBlind': sb,
                                  'BigBlind': bb,
                                  'TableName': table.title(),
                                  'Dealer': dealer,
                                  'NumPlayers': numPlayers,
                                  'LenBoard': lenBoard,
                                  'InvestedThisRound': round(roundInvestments[maybePlayerName] - amt,2),
                                  'Winnings': round(winnings[maybePlayerName],2),
                                  'SeatRelDealer':(seat-dealer)*(seat>dealer)+
                                              (dealer-seat)*(dealer>seat),
                                  'Source':'ong'
                                  }
                        for ii in [1,2]:
                            c = holeCards[maybePlayerName][ii-1]
                            if c is None:
                                newRow['HoleCard'+str(ii)] = -1
                            else:
                                newRow['HoleCard'+str(ii)] = deckT.index(c)
                        for ii in range(1,6):
                            if lenBoard >= ii:
                                newRow['Board'+str(ii)] = deckT.index(board[ii-1])
                            else:
                                newRow['Board'+str(ii)] = -1
                        for act in actions[2:]:
                            newRow['is{}'.format(act)] = int(a==act)
                        data.append(newRow)
                        roundActionNum += 1
            if data[-1]['RoundActionNum']==1:
                data.pop()
        except (ValueError, IndexError, KeyError, TypeError, AttributeError, ZeroDivisionError, AssertionError):
            pass
        
    return data

###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################

def readPSfile(filename):
    # HANDS TABLE
    with open(filename,'r') as f:
        startString = "PokerStars Game #"
        fileContents = [startString + theRest for theRest in f.read().replace('\r','').split(startString)]
        fileContents = fileContents[1:]
    
    data = []
    lineToRead = True
    
    for i,hand in enumerate(fileContents):
        try:
            assert not 'Hand cancelled' in hand, "cancelled hand"
            ###################### HAND INFORMATION ###########################
            # add small and big blinds
            fn = filename[filename.find("data_raw")+8:]
            bb = float(fn[:fn.find("/")])
            if bb==0.25:
                sb = 0.1
            else:
                sb = bb/2
            # add date
            dateStart = hand.find("-") + 2
            dateEnd = dateStart + 10
            dateStr = hand[dateStart:dateEnd]
            dateObj = datetime.datetime.strptime(dateStr, '%Y/%m/%d').date()
            # add time
            timeStart = dateEnd + 1
            timeEnd = hand.find("ET\n")
            timeStr = hand[timeStart:timeEnd].strip()
            timeObj = datetime.datetime.strptime(timeStr, '%H:%M:%S').time()
            # add table
            tableStart = hand.find("Table") + 7
            tableEnd = tableStart + hand[tableStart:].find("'")
            table = hand[tableStart:tableEnd]
            assert len(table)<=22
            # add dealer
            dealerEnd = hand.find("is the button") - 1
            dealerStart = tableEnd + hand[tableEnd:].find("#") + 1
            dealer = int(hand[dealerStart:dealerEnd])
            # add numPlayers
            lines = [s.rstrip() for s in hand.split('\n')]
            numPlayers = 0
            j = 2
            while lines[j][:5]=="Seat ":
                numPlayers += 1
                j += 1
            # add board
            boardLine = lines[lines.index("*** SUMMARY ***") + 2]
            if boardLine[:5]=="Board":
                board = boardLine[7:-1].split()
            else:
                board = ''
    
            ####################### PLAYER INFORMATION ########################
            # initialize...
            cp = 0
            cb = 0
            npl = 0
            rd = "Preflop"
            seats = {}
            startStacks = {}
            stacks = {}
            holeCards = {}
            roundInvestments = {}
            lenBoard = 0
            lastNewRoundLine = -1
            sitting = []
            winnings = {}
            
            # go through lines to populate seats
            n = 2
            while lines[n][:4]=="Seat" and lines[n].find("button")==-1:
                line = lines[n]
                playerStart = line.find(":")+2
                playerEnd = playerStart + line[playerStart:].find('(') - 1
                player = line[playerStart:playerEnd]
                assert not '$' in player and not ')' in player, "bad player name"
                s = line[(line.find(' ')+1):]
                seats[player] = int(s[:s.find(':')])
                startStacks[player] = toFloat(line[(line.find("$")+1):line.find(" in chips")])
                assert startStacks[player]!=0, "start stack of 0"
                stacks[player] = startStacks[player]
                holeCards[player] = [None, None]
                roundInvestments[player] = 0
                winnings[player] = 0.
                roundActionNum = 1
                npl += 1
                n += 1
            
            # seat nums without skipping seats
            relSeats = {p: bisect_left(seats.values(), seats[p])
                        for p in seats}
            
            # if dealer button is on empty seat, move it back
            while not dealer in seats.values() and dealer > 0:
                dealer -= 1            

            # go through again to...
            # collect hole card info, check for bad names, find winner
            for line in lines:
                maybePlayerName = line[:line.find(":")]
                if line.find('sit')>=0:
                    sitting.append(maybePlayerName)
                if len(maybePlayerName)==22 and maybePlayerName[:5]!="Total":
                    assert maybePlayerName in seats.keys() + sitting or \
                            maybePlayerName.find("***")>=0, maybePlayerName
                if maybePlayerName in seats.keys() and line.find("shows")>=0:
                    hc = line[(line.find("[")+1):line.find("]")]
                    hc = hc.split()
                    holeCards[maybePlayerName] = hc
                elif 'collected' in line and 'from' in line:
                    amt = line[(line.find('$')+1):]
                    amt = float(amt[:amt.find(' ')])
                    player = line[:line.find(" ")]
                    winnings[player] += amt
            
            for j,line in enumerate(lines):
                # skip SUMMARY section by changing lineToRead when encounter it
                # stop skipping once encounter "Stage"
                if line.find("PokerStars Game")>=0:
                    lineToRead = True
                elif line=="*** SUMMARY ***":
                    lineToRead = False
            
                if lineToRead:
                    newRow = {}
                    maybePlayerName = line[:line.find(":")]
                    
                    if line[:15]=="PokerStars Game":
                        stage = str(uuid4())
                                                
                    elif line[:3]=="***":
                        nar = j - lastNewRoundLine
                        lastNewRoundLine = j
                        for key in roundInvestments:
                            roundInvestments[key] = 0
                        rdStart = line.find(" ")+1
                        rdEnd = rdStart + line[rdStart:].find("*") - 1
                        rd = line[rdStart:rdEnd].title().strip()
                        if rd!='Hole Cards':
                            if nar>1:
                                assert roundActionNum!=1, "round with one action"
                            roundActionNum = 1
                            cb = 0
                        if rd=='Flop':
                            lenBoard = 3
                        elif rd=='Turn':
                            lenBoard = 4
                        elif rd=='River':
                            lenBoard = 5
                        elif rd.find("Card")>=0:
                            rd = 'Preflop'
                        elif rd=='Show Down':
                            lineToRead = False
                        else:
                            raise ValueError
                    
                    # create new row IF row is an action (starts with encrypted player name)
                    elif maybePlayerName in seats.keys():
                        seat = seats[maybePlayerName]
                        relSeat = relSeats[maybePlayerName]
                        fullA = line[(line.find(":") + 2):].strip()
                        isAllIn = fullA.find("all-in")>=0
                        if fullA.find("posts")>=0:
                            a = 'blind'
                            if fullA.find('small & big')>=0:
                                amt = bb
                            else:
                                amt = toFloat(fullA[(fullA.find("$")+1):])
                            roundInvestments[maybePlayerName] += amt
                            cp += amt
                            oldCB = copy(cb)
                            if cb<amt:
                                cb = amt
                            stacks[maybePlayerName] -= amt
                        elif fullA=="folds":
                            a = 'fold'
                            amt = 0.
                            npl -= 1
                            oldCB = copy(cb)
                        elif fullA.find('checks')>=0:
                            a = 'check'
                            amt = 0.
                            oldCB = copy(cb)
                        elif fullA.find("bets")>=0:
                            a = 'bet'
                            if isAllIn:
                                amt = toFloat(fullA[(fullA.find("$")+1):(fullA.find("and is"))])
                            else:
                                amt = toFloat(fullA[(fullA.find("$")+1):])
                            roundInvestments[maybePlayerName] += amt
                            cp += amt
                            oldCB = copy(cb)
                            cb = amt
                            stacks[maybePlayerName] -= amt
                        elif fullA.find('raises')>=0:
                            a = 'raise'
                            if isAllIn:
                                amt = toFloat(fullA[(fullA.find('to')+4):(fullA.find("and is")-1)])
                            else:
                                amt = toFloat(fullA[(fullA.find('to')+4):])
                            roundInvestments[maybePlayerName] = amt
                            cp += amt
                            oldCB = copy(cb)
                            if cb<amt:
                                cb = amt
                            stacks[maybePlayerName] -= amt
                        elif fullA.find('calls')>=0:
                            a = 'call'
                            if isAllIn:
                                amt = toFloat(fullA[(fullA.find('$')+1):(fullA.find("and is")-1)])
                            else:
                                amt = toFloat(fullA[(fullA.find('$')+1):])
                            roundInvestments[maybePlayerName] += amt
                            cp += amt
                            stacks[maybePlayerName] -= amt
                            oldCB = copy(cb)
                            if cb<amt:
                                cb = amt
                        elif fullA=='is sitting out':
                            npl -= 1
                            seats.pop(maybePlayerName)
                            continue
                        else:
                            continue
                        if oldCB > (roundInvestments[maybePlayerName] - amt):
                            assert a!='bet', "illegal action"
                        else:
                            assert a!='call', "illegal action"
                        assert stacks[maybePlayerName]+amt >= 0
                        newRow = {'GameNum':stage,
                                  'RoundActionNum':roundActionNum,
                                  'SeatNum':seat,
                                  'RelSeatNum':relSeat,
                                  'Round':rd,
                                  'Player':maybePlayerName,
                                  'StartStack':round(startStacks[maybePlayerName],2),
                                  'CurrentStack':round(stacks[maybePlayerName] + amt,2),
                                  'Action':a,
                                  'Amount':round(amt,2),
                                  'AllIn':int(isAllIn),
                                  'CurrentBet':round(oldCB,2),
                                  'CurrentPot':round(cp-amt,2),
                                  'NumPlayersLeft':npl+1 if a=='fold' else npl,
                                  'Date': dateObj,
                                  'Time': timeObj,
                                  'SmallBlind': sb,
                                  'BigBlind': bb,
                                  'TableName': table.title(),
                                  'Dealer': dealer,
                                  'NumPlayers': numPlayers,
                                  'LenBoard': lenBoard,
                                  'InvestedThisRound': round(roundInvestments[maybePlayerName] - amt,2),
                                  'Winnings': round(winnings[maybePlayerName],2),
                                  'SeatRelDealer':(seat-dealer)*(seat>dealer)+
                                              (dealer-seat)*(dealer>seat),
                                  'Source':'ps'
                                  }
                        for ii in [1,2]:
                            c = holeCards[maybePlayerName][ii-1]
                            if c is None:
                                newRow['HoleCard'+str(ii)] = -1
                            else:
                                newRow['HoleCard'+str(ii)] = deckT.index(c)
                        for ii in range(1,6):
                            if lenBoard >= ii:
                                newRow['Board'+str(ii)] = deckT.index(board[ii-1])
                            else:
                                newRow['Board'+str(ii)] = -1
                        for act in actions[2:]:
                            newRow['is{}'.format(act)] = int(a==act)
                        data.append(newRow)
                        roundActionNum += 1
            if data[-1]['RoundActionNum']==1:
                data.pop()
        except (ValueError, IndexError, KeyError, TypeError, AttributeError, ZeroDivisionError, AssertionError):
            pass
        
    return data

###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################

def readPTYfile(filename):
    # HANDS TABLE
    with open(filename,'r') as f:
        startString = "Game #"
        fileContents = [startString + theRest for theRest in f.read().replace('\r','').split(startString)]
        fileContents = fileContents[1:]
    
    data = []
        
    for i,hand in enumerate(fileContents):
        try:
            # if lost connection, drop hand
            if hand.find('due to some reason')>=0:
                raise ValueError
            ###################### HAND INFORMATION ###########################
            # add small and big blinds
            fn = filename[filename.find("data_raw")+8:]
            bb = float(fn[:fn.find("/")])
            if bb==0.25:
                sb = 0.1
            else:
                sb = bb/2
            # add date
            dateStart = hand.find(",") + 2
            if bb==10:
                dateStart += hand[dateStart:].find(",") + 2
            dateEnd = dateStart + hand[dateStart:].find(",")
            month, dateNum = hand[dateStart:dateEnd].split()
            monthConv = {v:k for k,v in enumerate(calendar.month_name)}
            year = hand[(hand.find("Table") - 6):(hand.find("Table") - 1)]
            dateObj = datetime.date(int(year),
                                    int(monthConv[month]),
                                    int(dateNum))
            # add time
            timeStart = dateEnd + 2
            timeEnd = timeStart + 8
            timeStr = hand[timeStart:timeEnd]
            timeObj = datetime.datetime.strptime(timeStr, '%H:%M:%S').time()
            # add table
            tableStart = hand.find("Table") + 6
            tableEnd = tableStart + hand[tableStart:].find(" ")
            table = hand[tableStart:tableEnd]
            assert len(table)<=22
            # add dealer
            dealerEnd = hand.find("is the button") - 1
            dealerStart = tableEnd + hand[tableEnd:].find("Seat ") + 5
            dealer = int(hand[dealerStart:dealerEnd])
            # add numPlayers
            npStart = hand.find("Total number of players : ") + 26
            npEnd = npStart + hand[npStart:].find('\n')
            numPlayers = int(hand[npStart:npEnd].strip())
            # add board
            board = []
            flopStart = hand.find("Dealing Flop")
            turnStart = hand.find("Dealing Turn")
            riverStart = hand.find("Dealing River")
            if flopStart>=0:
                flopStart += 18
                flopEnd = flopStart + 10
                flop = hand[flopStart:flopEnd]
                board += flop.replace(',','').split()
            if turnStart>=0:
                turnStart += 18
                turnEnd = turnStart + 2
                turn = hand[turnStart:turnEnd]
                board.append(turn.replace(',',''))
            if riverStart>=0:
                riverStart += 19
                riverEnd = riverStart + 2
                river = hand[riverStart:riverEnd]
                board.append(river.replace(',',''))
    
            ####################### PLAYER INFORMATION ########################
            lines = [s.rstrip() for s in hand.split('\n')]
            lines = [l for l in lines if len(l)>0]
            # initialize...
            cp = 0
            cb = 0
            npl = 0
            rd = "Preflop"
            seats = {}
            startStacks = {}
            stacks = {}
            holeCards = {}
            roundInvestments = {}
            winnings = {}
            lenBoard = 0
            lastNewRoundLine = -1
            
            # go through lines to populate seats
            n = 7
            while lines[n][:4]=="Seat":
                line = lines[n]
                playerStart = line.find(":")+2
                playerEnd = playerStart + line[playerStart:].find(' ')
                player = line[playerStart:playerEnd]
                assert not '$' in player and not ')' in player, "bad player name"
                s = line[(line.find(' ')+1):]
                seats[player] = int(s[:s.find(':')])
                startStacks[player] = toFloat(line[(line.find("$")+1):(line.find("USD")-1)])
                if not hand.find(player+" has left table"):
                    assert startStacks[player]!=0, "start stack of 0"
                stacks[player] = startStacks[player]
                holeCards[player] = [None, None]
                winnings[player] = 0.
                roundInvestments[player] = 0
                roundActionNum = 1
                npl += 1
                n += 1

            # seat nums without skipping seats
            relSeats = {p: bisect_left(seats.values(), seats[p])
                        for p in seats}
            
            # if dealer button is on empty seat, move it back
            while not dealer in seats.values() and dealer > 0:
                dealer -= 1
                        
            # go through again to...
            # collect hole card info, check for bad names, find winner
            for line in lines:
                maybePlayerName = line[:line.find(" ")]
                assert line!="** Dealing **"
                if len(maybePlayerName)==22 and not line.find('has joined')>=0:
                    assert maybePlayerName in seats.keys()
                if maybePlayerName in seats.keys() and line.find("shows")>=0:
                    hc = line[(line.find("[")+2):(line.find("]")-1)]
                    hc = hc.split(", ")
                    holeCards[maybePlayerName] = hc
                elif 'wins' in line:
                    amt = line[(line.find('$')+1):]
                    amt = float(amt[:amt.find('USD')])
                    winnings[maybePlayerName] += amt
            
            for j,line in enumerate(lines):
                # skip SUMMARY section by changing lineToRead when encounter it
                # stop skipping once encounter "Game"
                newRow = {}
                maybePlayerName = line[:line.find(" ")]
                
                if line[:6]=="Game #":
                    stage = str(uuid4())
                    
                elif line[:2]=="**" and line[:5]!="*****":
                    nar = j - lastNewRoundLine
                    lastNewRoundLine = j
                    for key in roundInvestments:
                        roundInvestments[key] = 0
                    rdStart = line.find(" ")+9
                    rdEnd = rdStart + line[rdStart:].find("*") - 1
                    rd = line[rdStart:rdEnd].title().strip()
                    if rd!='Down Cards':
                        if nar>1:
                            assert roundActionNum!=1, "round with one action"
                        roundActionNum = 1
                        cb = 0
                    if rd=='Flop':
                        lenBoard = 3
                    elif rd=='Turn':
                        lenBoard = 4
                    elif rd=='River':
                        lenBoard = 5
                    elif rd.find("Card")>=0:
                        rd = 'Preflop'
                    else:
                        raise ValueError
                
                # create new row IF row is an action (starts with encrypted player name)
                elif maybePlayerName in seats.keys():
                    seat = seats[maybePlayerName]
                    relSeat = relSeats[maybePlayerName]
                    fullA = line[(line.find(" ") + 1):].strip()
                    isAllIn = fullA.find("all-In")>=0
                    if fullA.find("posts")>=0:
                        if fullA.find('dead')>=0:
                            a = 'deadblind'
                            amt = bb
                        else:
                            a = 'blind'
                            amtStart = fullA.find("$") + 1
                            amtEnd = fullA.find("USD") - 1
                            amt = toFloat(fullA[amtStart:amtEnd])
                        roundInvestments[maybePlayerName] += amt
                        cp += amt
                        oldCB = copy(cb)
                        cb = amt
                        stacks[maybePlayerName] -= amt
                    elif fullA=="folds":
                        a = 'fold'
                        amt = 0.
                        npl -= 1
                        oldCB = copy(cb)
                    elif fullA.find('checks')>=0:
                        a = 'check'
                        amt = 0.
                        oldCB = copy(cb)
                    elif fullA.find("bets")>=0:
                        a = 'bet'
                        amt = toFloat(fullA[(fullA.find("$")+1):(fullA.find("USD")-1)])
                        roundInvestments[maybePlayerName] += amt
                        cp += amt
                        oldCB = copy(cb)
                        cb = amt
                        stacks[maybePlayerName] -= amt
                    elif fullA.find('raises')>=0:
                        a = 'raise'
                        amt = toFloat(fullA[(fullA.find('$')+1):(fullA.find("USD")-1)])
                        roundInvestments[maybePlayerName] = amt
                        cp += amt
                        oldCB = copy(cb)
                        cb = amt
                        stacks[maybePlayerName] -= amt
                    elif fullA.find('calls')>=0:
                        a = 'call'
                        amt = toFloat(fullA[(fullA.find('$')+1):(fullA.find("USD")-1)])
                        amt -= roundInvestments[maybePlayerName]
                        roundInvestments[maybePlayerName] += amt
                        cp += amt
                        oldCB = copy(cb)
                        if cb<amt:
                            cb = amt
                        stacks[maybePlayerName] -= amt
                    elif isAllIn:
                        amt = toFloat(fullA[(fullA.find('$')+1):(fullA.find("USD")-1)])
                        if cb==0:
                            a = 'bet'
                            roundInvestments[maybePlayerName] += amt
                        elif amt > cb:
                            a = 'raise'
                            roundInvestments[maybePlayerName] = amt
                        else:
                            a = 'call'
                            amt += roundInvestments[maybePlayerName]
                            roundInvestments[maybePlayerName] = amt
                        cp += amt
                        stacks[maybePlayerName] -= amt
                        oldCB = copy(cb)
                        if cb<amt:
                            cb = amt
                    elif fullA=='is sitting out':
                        npl -= 1
                        seats.pop(maybePlayerName)
                        continue
                    else:
                        continue
                    if oldCB > (roundInvestments[maybePlayerName] - amt):
                        assert a!='bet', "illegal action"
                    else:
                        assert a!='call', "illegal action"
                    assert stacks[maybePlayerName]+amt >= 0
                    newRow = {'GameNum':stage,
                              'RoundActionNum':roundActionNum,
                              'SeatNum':seat,
                              'RelSeatNum':relSeat,
                              'Round':rd,
                              'Player':maybePlayerName,
                              'StartStack':round(startStacks[maybePlayerName],2),
                              'CurrentStack':round(stacks[maybePlayerName] + amt,2),
                              'Action':a,
                              'Amount':round(amt,2),
                              'AllIn':int(isAllIn),
                              'CurrentBet':round(oldCB,2),
                              'CurrentPot':round(cp-amt,2),
                              'NumPlayersLeft':npl+1 if a=='fold' else npl,
                              'Date': dateObj,
                              'Time': timeObj,
                              'SmallBlind': sb,
                              'BigBlind': bb,
                              'TableName': table.title(),
                              'Dealer': dealer,
                              'NumPlayers': numPlayers,
                              'LenBoard': lenBoard,
                              'InvestedThisRound': round(roundInvestments[maybePlayerName] - amt,2),
                              'Winnings': round(winnings[maybePlayerName],2),
                              'SeatRelDealer':(seat-dealer)*(seat>dealer)+
                                              (dealer-seat)*(dealer>seat),
                              'Source':'pty'
                              }
                    for ii in [1,2]:
                        c = holeCards[maybePlayerName][ii-1]
                        if c is None:
                            newRow['HoleCard'+str(ii)] = -1
                        else:
                            newRow['HoleCard'+str(ii)] = deckT.index(c)
                    for ii in range(1,6):
                        if lenBoard >= ii:
                            newRow['Board'+str(ii)] = deckT.index(board[ii-1])
                        else:
                            newRow['Board'+str(ii)] = -1
                    for act in actions[2:]:
                        newRow['is{}'.format(act)] = int(a==act)
                    data.append(newRow)
                    roundActionNum += 1
            if data[-1]['RoundActionNum']==1:
                data.pop()
        except (ValueError, IndexError, KeyError, TypeError, AttributeError, ZeroDivisionError, AssertionError):
            pass

    return data

######################## READ ONE FILE ########################################    
def readFile(filename):
    # get dataframe from one of the source-specific functions
    bf = filename[::-1]
    src = bf[(bf.find('HLN')+3):bf.find('/')][::-1].strip()
    
    # skip ipn, don't have a parser for it
    if src=='ipn':
        return []
        
    # execute read file
    func = 'read{}file'.format(src.upper())
    full = eval('{}("{}")'.format(func, filename))
    
    return full
        
####################### READ ALL FILES ########################################
# if testing, go to test
os.chdir('../test') if testing else os.chdir('../full')

# restart the data folder
if len(os.listdir('data_parsed')) > 0:
    os.system('rm data_parsed/*')
    os.makedir('data_parsed')
os.chdir('data_parsed')

# get all files, not including IPN because they're stupid and also dumb
folders = ["../data_raw/"+fdr for fdr in os.listdir('../data_raw') if fdr.find('.')==-1]
allFiles = [folder+"/"+f for folder in folders for f in os.listdir(folder)
            if f.find('ipn ')==-1]

# fields for each CSV
fields = {'games': ['GameNum','Source','Date','Time','SmallBlind','BigBlind','TableName',
                    'NumPlayers','Dealer'],
          'actions': ['GameNum','Player','Action','SeatNum','RelSeatNum','Round',
                      'RoundActionNum','StartStack','CurrentStack','Amount',
                      'AllIn','CurrentBet','CurrentPot','InvestedThisRound',
                      'NumPlayersLeft','Winnings','HoleCard1','HoleCard2',
                      'SeatRelDealer','isfold','ischeck','iscall','isbet',
                      'israise'],
          'boards': ['GameNum','LenBoard'] + ['Board'+str(i) for i in range(1,6)]}
allFields = list(set(c for l in fields.values() for c in l))
# +1 on fieldInds because in bash fields are indexed starting at 1
fieldInds = {k: [allFields.index(c)+1 for c in v] for k,v in fields.iteritems()}

# one thread for getting and writing a file's contents
def worker(tup):
    i,f = tup
    
    # read in data
    df = readFile(f)
    
    # write each DF to its own CSV
    with open('poker{}.csv'.format(i),'w') as f:
        writer = csv.DictWriter(f, fieldnames=allFields)
        writer.writerows(df)

# write all files
def getData(nFiles, mp=True, useExamples=False):
    # multi-threaded or single-threaded writing, depending on flag
    if mp:
        p = multiprocessing.Pool(8)
        if useExamples:
            p.map_async(worker,enumerate(examples[:nFiles]))
        else:
            p.map_async(worker,enumerate(allFiles[:nFiles]))
        p.close()
        p.join()
    else:
        if useExamples:
            map(worker, enumerate(examples[:nFiles]))
        else:
            map(worker, enumerate(allFiles[:nFiles]))
    
    print "Final runtime of getData:", datetime.datetime.now() - startTime

# if testing, get example files; if not, do all files
mp = True
startTime = datetime.datetime.now()

if testing:
    srcs = ['abs','ftp','ong','ps','pty']
    stks = ['0.5','0.25','1','2','4','6','10']
    examples = []
    for sr,st in itertools.product(srcs,stks):
        allMatches = [f for f in allFiles if f.find("/"+sr)>=0 and f.find("/"+st+"/")>=0]
        if allMatches:
            examples += random.sample(allMatches, len(allMatches))
    examples = examples[:testSize]
    getData(len(examples), mp=mp, useExamples=True)
else:
    getData(len(allFiles))

####################### DATA FORMATTING 2: THE SQL ############################
# concatenate all CSVs to one big CSV
os.system('cat *.csv > fullPoker.csv')

# delete small CSVs
os.system('rm poker*.csv')

## slice each CSV to its relevant columns: actions and boards, then games separate
for f in ['actions','boards','games']:
    os.system("""
    awk 'BEGIN {{FS=OFS=","}} {{print ${}}}' fullPoker.csv > {}.csv
    """.format(',$'.join(toStrings(fieldInds[f])),f).strip())

# delete fullPoker files
os.remove('fullPoker.csv')

# remove duplicate rows from board, game CSVs
os.system('sort -u boards.csv -o boards.csv')
os.system('sort -u games.csv -o games.csv')

# sort boards by game then round
os.system('sort -t, -k 1,1d -k 3,3n boards.csv > boards2.csv')
os.remove('boards.csv')
os.rename('boards2.csv','boards.csv')

# write headers to top of files, then write all data back
for k,v in fields.iteritems():
    with open('{}2.csv'.format(k),'w') as f:
        f.write(','.join(v) + '\n')
    os.system('cat {0}.csv >> {0}2.csv'.format(k))
    os.remove('{}.csv'.format(k))
    os.rename('{}2.csv'.format(k),'{}.csv'.format(k))

print "Final runtime of bash:", datetime.datetime.now() - startTime

# get password from file
with open('../../util/pwd.txt') as f:
    pwd = f.read().strip()

# connect to DB
db = MySQLdb.connect(host='localhost',port=3307,user='ntaylorwss',passwd=pwd,
                     unix_socket='/var/run/mysqld/mysqld.sock',
                     local_infile=1)
cursor = db.cursor()

# if it exists, blow it up and go from the beginning
try:
    cursor.execute('DROP DATABASE poker{};'.format('sample' if testing else ''))
except MySQLdb.OperationalError:
    pass
cursor.execute('CREATE DATABASE poker{};'.format('sample' if testing else ''))
cursor.execute('USE poker{};'.format('sample' if testing else ''))

# queries to create tables
createBoardsTempQuery = """create table boardsTemp
                    ( GameNum varchar(36),
                      LenBoard smallint(2),
                      Board1 smallint(2),
                      Board2 smallint(2),
                      Board3 smallint(2),
                      Board4 smallint(2),
                      Board5 smallint(2),
                      BoardID int NOT NULL AUTO_INCREMENT,
                      PRIMARY KEY (BoardID)
                    ) ENGINE = MYISAM;"""

createBoardsQuery = """create table boards
                    ( GameNum varchar(36),
                      LenBoard smallint(2),
                      Board1 smallint(2),
                      Board2 smallint(2),
                      Board3 smallint(2),
                      Board4 smallint(2),
                      Board5 smallint(2),
                      PRIMARY KEY (GameNum)
                    ) ENGINE = MYISAM;"""

createActionsQuery = """create table actions
                    ( GameNum varchar(36),
                      Player varchar(30),
                      Action varchar(10),
                      SeatNum tinyint(2),
                      RelSeatNum tinyint(2),
                      Round varchar(7),
                      RoundActionNum tinyint(2),
                      StartStack decimal(12,2),
                      CurrentStack decimal(12,2),
                      Amount decimal(10,2),
                      AllIn tinyint(1),
                      CurrentBet decimal(12,2),
                      CurrentPot decimal(12,2),
                      InvestedThisRound decimal(10,2),
                      NumPlayersLeft tinyint(2),
                      Winnings decimal(10,2),
                      HoleCard1 smallint(2),
                      HoleCard2 smallint(2),
                      SeatRelDealer tinyint(2),
                      isFold tinyint(1),
                      isCheck tinyint(1),
                      isCall tinyint(1),
                      isBet tinyint(1),
                      isRaise tinyint(1),
                      ActionID int NOT NULL AUTO_INCREMENT,
                      PRIMARY KEY (ActionID)
                    ) ENGINE = MYISAM;"""
                    
createGamesQuery = """create table games 
                    ( GameNum varchar(36),
                      Source varchar(3),
                      Date date,
                      Time time,
                      SmallBlind decimal(4,2),
                      BigBlind decimal(4,2),
                      TableName varchar(22),
                      Dealer tinyint(2),
                      NumPlayers tinyint(2),
                      PRIMARY KEY (GameNum)
                    ) ENGINE = MYISAM;"""
                    
queries = [createGamesQuery,createBoardsTempQuery,
           createBoardsQuery,createActionsQuery]
for q in queries: cursor.execute(q)

# query to add CSV data to tables
importQuery = """LOAD DATA LOCAL INFILE '{}'
                INTO TABLE {}
                FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                IGNORE 1 LINES
                ({});"""

# run queries: games, then boards, then actions
for f in sorted(os.listdir(os.getcwd()))[::-1]:
    table = f[:-4]
    try:
        if table=='boards':
            cursor.execute(importQuery.format(f,table+'Temp',','.join(fields[table])))
        else:
            cursor.execute(importQuery.format(f, table, ','.join(fields[table])))
        db.commit()
    except Exception:
        db.rollback()
        
# remove rows with boards before last board of game
cursor.execute("""INSERT INTO boards
            SELECT GameNum,LenBoard,Board1,Board2,Board3,Board4,Board5
            FROM boardsTemp
            WHERE BoardID IN (SELECT MAX(BoardID)
                                FROM boardsTemp
                                GROUP BY GameNum)
            ;""")
cursor.execute('DROP TABLE boardsTemp;')

print "Final runtime of SQL:", datetime.datetime.now() - startTime
