# Opponent Modelling Poker Project (instructions for running)
Most of the computation for this project is about preparing the data. In order to do that, the following steps should be taken:
1. Run `data/test/data_raw/getRawData.sh` to download ~40GB of text logs
2. Run `data/lib/fileReaders.py` to parse these into structured data
3. Run `data/lib/buildDataset.py` to engineer the feature set

These scripts require the following libraries:
- Pandas
- Numpy
- MySQLdb

Once the data is collected, `report/lib/runAll.sh` will run `classifiers.py`, which generates the classifiers, `regressors.py`, which generates the regressors, and a few other small scripts that generate the data and tables in the report. It will also produce the report as a PDF.

The `tools` directory is irrelevant to this project, as these are scripts that will be used to build a reinforcement learner later on, which is beyond the scope of this project.

I would not recommend that this project be run by a reviewer, as the feature engineering alone can take up to a full 24 hour day.

The PDF report is in `docs/CapstoneReport.pdf`.
