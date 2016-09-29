# setup
print(getwd())
library(lattice)
library(reshape)
library(ggplot2)
library(RJSONIO)
library(stringr)
theme_set(theme_minimal(12))

# read in data and correctly label the factor levels of Table
labels <- read.csv('data/labelBreakdown.csv')
labels$Table <- factor(labels$Table, 
                       levels=c('Preflop-True','Flop-False',
                                'Flop-True','Turn-False','Turn-True',
                                'River-False','River-True'))
labels <- melt(labels, id='Table')
names(labels) <- c('Situation','Action','Frequency')

# generate label dist plot, save to PNG in data folder
png('data/labelDist.png', width=720)
ggplot(labels[labels$Frequency!=0,], aes(Situation, Frequency)) +
  geom_bar(aes(fill=Action), position='dodge', stat='identity', width=0.75) +
  theme(axis.text.x = element_text(angle=90, hjust=1),
        axis.title.x=element_blank()) + 
  ggtitle('Label distribution by dataset') +
  ylab('Proportion of actions')
dev.off()

# generate KDEs for bet and raise amounts for each situation
featureSets <- fromJSON('../data/util/FeatureSets.json')
amountDists <- data.frame()
for (f in c('Preflop-True','Flop-False','Flop-True','Turn-False','Turn-True','River-False','River-True')) {
  act <- ifelse(grepl('True',f), 'raise','bet')
  amtDF <- read.csv(gsub('filename',f,'../data/test/data_engineered/subsets/classifier/filename.csv'), nrows=500000, header=FALSE, stringsAsFactors = FALSE)
  names(amtDF) <- featureSets[[f]]
  amt <- amtDF[amtDF$Action==act,'Amount_rp']
  amt <- amt[amt<quantile(amt, 0.95)]
  rd <- substr(f, 1, regexpr('-',f)[1]-1)
  amountDists <- rbind(amountDists, data.frame(Situation = rep(rd, length(amt)), Action = rep(act, length(amt)),Amount=amt))
}

png('data/raiseDist.png', width=720)
ggplot(amountDists[amountDists$Action=='raise',], aes(Amount, fill=Situation, color=Situation)) +
  geom_density(alpha=0.1, bw=0.04) + 
  xlab('Amount Relative to Pot Size') + 
  ggtitle('Amount Distribution for Raises by Situation')
dev.off()

png('data/betDist.png', width=720)
ggplot(amountDists[amountDists$Action=='bet',], aes(Amount, fill=Situation, color=Situation)) +
  geom_density(alpha=0.1, bw=0.025) + 
  xlab('Amount Relative to Pot Size') + 
  ggtitle('Amount Distribution for Bets by Situation')
dev.off()

timings = read.csv('data/timings.csv', stringsAsFactors = FALSE)
timings$Task = factor(timings$Task, levels = timings$Task)

ggplot(timings, aes(x=Task, y=Seconds)) + geom_bar(stat='identity') + 
  theme(axis.text.x = element_text(angle=60, hjust=1),
        axis.title.x=element_blank()) + 
  ggtitle('Subtasks of Project by Computation Time')