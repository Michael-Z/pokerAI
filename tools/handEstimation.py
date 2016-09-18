import random

'''
# get the dat file from:
# https://github.com/christophschmalhofer/poker/blob/master/XPokerEval/XPokerEval.TwoPlusTwo/HandRanks.dat
import array
ranksFile = "../data/util/HandRanks.dat"
handsFile = "../data/util/HandRanks.txt"
ranksData = array.array('i')
with open(ranksFile,'rb') as f:
    ranksData.fromfile(f, 32487834)
ranksData = list(ranksData)
with open(handsFile,'w') as f:
    f.write(','.join(str(i) for i in ranksData))
'''

with open('../data/util/HandRanks.txt') as f:
    handsData = [int(x) for x in f.read().split(',')]

cardNumRange = [str(i) for i in range(2,10)] + ['T','J','Q','K','A']
cardSuitRange = ['d','c','h','s']
deck = [str(i) + str(j) for i in cardNumRange for j in cardSuitRange]

def handEval(cards):
    p = 53
    for c in cards:
        p = handsData[p + c + 1] # + 1 because cards are 1-52, not 0-51, in this table
    return [p>>12, p & 0x00000FFF]
        
def oneSim(hand, board, nPlayers):
    cards = set(range(52)) - set(hand+board)
    cards = random.sample(cards, 5-len(board) + 2*(nPlayers-1))
    otherHands = []
    for i in xrange(nPlayers-1):
        otherHands.append([cards.pop(), cards.pop()])
    # evaluate ours and all other hands, get best other score
    our = handEval(hand + board + cards)
    best = max(handEval(h + board + cards) for h in otherHands)
    # return whether we are the winner (2), tied (1), or lost(0)
    if our > best:
        return 2
    else:
        return int(our==best)

def handOdds(hand, board, nPlayers, nSims):
    points = 0
    for i in xrange(nSims):
        points += oneSim(hand, board, nPlayers)
    winPct = points / (2. * nSims)
    expectedWin = 1. / nPlayers
    return round(winPct - expectedWin, 2)
