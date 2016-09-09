# Capstone Project
## Machine Learning Engineer Nanodegree
Nash Taylor   
TBD, 2016

## I. Definition
_(approx. 1-2 pages)_

### Project Overview
In March of 2016, DeepMind's AlphaGo defeated Lee Sedol, one of the world's top ranked Go players, using an architecture based on Deep Reinforcement Learning. Inspired by this great advancement in game-playing Artificial Intelligence (AI), I will attempt to apply a similar combination of Deep Networks and Reinforcement Learning to the game of No-Limit Texas Holdem Poker.

Texas Holdem (rules [here](https://www.pokerstars.com/poker/games/texas-holdem/)) is the most popular poker variant, and it can be played with 3 styles of betting:

- No-limit: bets have no upper limit, and a player may bet his or her entire stack at any time
- Pot-limit: the upper limit of a bet is controlled by the size of the pot (amount already bet during the hand)
- Fixed-limit: bets can be one of two sizes, the "small bet" (which is typically equivalent to the Big Blind) and the "large bet", twice the size of the small bet

There are 2 characteristics of no-limit holdem that make it particularly interesting from an AI perspective. The first is that it is a game of imperfect information. Unlike Go, where all game information is seen at all times, in poker critical information is missing from the view of the agent: the hole cards of its opponents. Without this information, decision-making becomes much more difficult. The second characteristic is unique to no-limit, and is the reason for its selection among the variants: in no-limit poker, bets can be of nearly any size. This makes the action space potentially continuous, which makes learning much more difficult. In fact, limit holdem, with its very limited action space, is already considered to be [solved](http://ai.cs.unibas.ch/_files/teaching/fs15/ki/material/ki02-poker.pdf), whereas no-limit is still an area of active research.

All supervised models constructed as part of this project utilized [this](http://web.archive.org/web/20110205042259/http://www.outflopped.com/questions/286/obfuscated-datamined-hand-histories) data from [HandHQ.com](handhq.com).

### Problem Statement
The goal of this Artificially Intelligent agent is to play the game of poker in such a way that it consistently makes a profit, and can compete with the highest level of human player. It should profit from the weaknesses of its opponents by first identifying their style, then learning a policy to exploit that style. It should be capable of playing on tables of any size, whether 1-on-1 or with multiple opponents.

This will be accomplished by the following:

1. Parse the raw text of game logs into a feature set providing information on, for each action taken:
    - the play style of the player (e.g. their preflop raise percentage)
    - the player's opponents at the table (e.g. the average stack size)
    - the community cards (e.g. the number of pairs on the board)
    - the state of the game (e.g. the number of players remaining)
2. Build a supervised model (the "action model") that maps the last 3 sections of features to an action (e.g. "Call")
3. Build an unsupervised model (the "player model") that finds clusters within the first section of features
4. Build a poker environment for the agent to interact with
5. Write a hand-strength estimation algorithm, a critical subprocess for decision-making
6. Generate dynamic opponents with the player model by initializing them to cluster centers, and slowly moving them from cluster to cluster (creating a dynamic, ever-changing style). Their actions will be decided by the action model
7. Generate a state representation using information from the environment, the hand strength estimator, and the action model (to predict opponents' responses to actions)
8. Utilize deep reinforcement learning techniques within the poker environment to learn an optimal policy over the state representation

An agent that does this successfully will be one that, while not winning every hand, will be consistently profitable in the long term. The agent will ideally be able to translate this policy to tournament play, but its focus in training will be on the cash-game style where each hand is about maximizing profit, and not on eliminating others or trying to get "in the money".

### Metrics
In this section, you will need to clearly define the metrics or calculations you will use to measure performance of a model or result in your project. These calculations and metrics should be justified based on the characteristics of the problem and problem domain. Questions to ask yourself when writing this section:
- _Are the metrics you’ve chosen to measure the performance of your models clearly discussed and defined?_
- _Have you provided reasonable justification for the metrics chosen based on the problem and solution?_

## II. Analysis
_(approx. 2-4 pages)_

### Data Exploration
In this section, you will be expected to analyze the data you are using for the problem. This data can either be in the form of a dataset (or datasets), input data (or input files), or even an environment. The type of data should be thoroughly described and, if possible, have basic statistics and information presented (such as discussion of input features or defining characteristics about the input or environment). Any abnormalities or interesting qualities about the data that may need to be addressed have been identified (such as features that need to be transformed or the possibility of outliers). Questions to ask yourself when writing this section:
- _If a dataset is present for this problem, have you thoroughly discussed certain features about the dataset? Has a data sample been provided to the reader?_
- _If a dataset is present for this problem, are statistics about the dataset calculated and reported? Have any relevant results from this calculation been discussed?_
- _If a dataset is **not** present for this problem, has discussion been made about the input space or input data for your problem?_
- _Are there any abnormalities or characteristics about the input space or dataset that need to be addressed? (categorical variables, missing values, outliers, etc.)_

### Exploratory Visualization
In this section, you will need to provide some form of visualization that summarizes or extracts a relevant characteristic or feature about the data. The visualization should adequately support the data being used. Discuss why this visualization was chosen and how it is relevant. Questions to ask yourself when writing this section:
- _Have you visualized a relevant characteristic or feature about the dataset or input data?_
- _Is the visualization thoroughly analyzed and discussed?_
- _If a plot is provided, are the axes, title, and datum clearly defined?_

### Algorithms and Techniques
In this section, you will need to discuss the algorithms and techniques you intend to use for solving the problem. You should justify the use of each one based on the characteristics of the problem and the problem domain. Questions to ask yourself when writing this section:
- _Are the algorithms you will use, including any default variables/parameters in the project clearly defined?_
- _Are the techniques to be used thoroughly discussed and justified?_
- _Is it made clear how the input data or datasets will be handled by the algorithms and techniques chosen?_

### Benchmark
In this section, you will need to provide a clearly defined benchmark result or threshold for comparing across performances obtained by your solution. The reasoning behind the benchmark (in the case where it is not an established result) should be discussed. Questions to ask yourself when writing this section:
- _Has some result or value been provided that acts as a benchmark for measuring performance?_
- _Is it clear how this result or value was obtained (whether by data or by hypothesis)?_


## III. Methodology
_(approx. 3-5 pages)_

### Data Preprocessing
In this section, all of your preprocessing steps will need to be clearly documented, if any were necessary. From the previous section, any of the abnormalities or characteristics that you identified about the dataset will be addressed and corrected here. Questions to ask yourself when writing this section:
- _If the algorithms chosen require preprocessing steps like feature selection or feature transformations, have they been properly documented?_
- _Based on the **Data Exploration** section, if there were abnormalities or characteristics that needed to be addressed, have they been properly corrected?_
- _If no preprocessing is needed, has it been made clear why?_

### Implementation
In this section, the process for which metrics, algorithms, and techniques that you implemented for the given data will need to be clearly documented. It should be abundantly clear how the implementation was carried out, and discussion should be made regarding any complications that occurred during this process. Questions to ask yourself when writing this section:
- _Is it made clear how the algorithms and techniques were implemented with the given datasets or input data?_
- _Were there any complications with the original metrics or techniques that required changing prior to acquiring a solution?_
- _Was there any part of the coding process (e.g., writing complicated functions) that should be documented?_

### Refinement
In this section, you will need to discuss the process of improvement you made upon the algorithms and techniques you used in your implementation. For example, adjusting parameters for certain models to acquire improved solutions would fall under the refinement category. Your initial and final solutions should be reported, as well as any significant intermediate results as necessary. Questions to ask yourself when writing this section:
- _Has an initial solution been found and clearly reported?_
- _Is the process of improvement clearly documented, such as what techniques were used?_
- _Are intermediate and final solutions clearly reported as the process is improved?_


## IV. Results
_(approx. 2-3 pages)_

### Model Evaluation and Validation
In this section, the final model and any supporting qualities should be evaluated in detail. It should be clear how the final model was derived and why this model was chosen. In addition, some type of analysis should be used to validate the robustness of this model and its solution, such as manipulating the input data or environment to see how the model’s solution is affected (this is called sensitivity analysis). Questions to ask yourself when writing this section:
- _Is the final model reasonable and aligning with solution expectations? Are the final parameters of the model appropriate?_
- _Has the final model been tested with various inputs to evaluate whether the model generalizes well to unseen data?_
- _Is the model robust enough for the problem? Do small perturbations (changes) in training data or the input space greatly affect the results?_
- _Can results found from the model be trusted?_

### Justification
In this section, your model’s final solution and its results should be compared to the benchmark you established earlier in the project using some type of statistical analysis. You should also justify whether these results and the solution are significant enough to have solved the problem posed in the project. Questions to ask yourself when writing this section:
- _Are the final results found stronger than the benchmark result reported earlier?_
- _Have you thoroughly analyzed and discussed the final solution?_
- _Is the final solution significant enough to have solved the problem?_


## V. Conclusion
_(approx. 1-2 pages)_

### Free-Form Visualization
In this section, you will need to provide some form of visualization that emphasizes an important quality about the project. It is much more free-form, but should reasonably support a significant result or characteristic about the problem that you want to discuss. Questions to ask yourself when writing this section:
- _Have you visualized a relevant or important quality about the problem, dataset, input data, or results?_
- _Is the visualization thoroughly analyzed and discussed?_
- _If a plot is provided, are the axes, title, and datum clearly defined?_

### Reflection
In this section, you will summarize the entire end-to-end problem solution and discuss one or two particular aspects of the project you found interesting or difficult. You are expected to reflect on the project as a whole to show that you have a firm understanding of the entire process employed in your work. Questions to ask yourself when writing this section:
- _Have you thoroughly summarized the entire process you used for this project?_
- _Were there any interesting aspects of the project?_
- _Were there any difficult aspects of the project?_
- _Does the final model and solution fit your expectations for the problem, and should it be used in a general setting to solve these types of problems?_

### Improvement
In this section, you will need to provide discussion as to how one aspect of the implementation you designed could be improved. As an example, consider ways your implementation can be made more general, and what would need to be modified. You do not need to make this improvement, but the potential solutions resulting from these changes are considered and compared/contrasted to your current solution. Questions to ask yourself when writing this section:
- _Are there further improvements that could be made on the algorithms or techniques you used in this project?_
- _Were there algorithms or techniques you researched that you did not know how to implement, but would consider using if you knew how?_
- _If you used your final solution as the new benchmark, do you think an even better solution exists?
