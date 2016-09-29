jupyter nbconvert --to markdown docs/CapstoneReport.ipynb
dimTable=$(<data/dims.txt)
labelTable=$(<data/labels.txt)
classifierTable=$(<data/classifiers.txt)
regressorTable=$(<data/regressors.txt)
#classGridTable=$(<data/classifierGS.txt)
perl -pi -e "s/labelTableHere/$labelTable/g" docs/CapstoneReport.md
perl -pi -e "s/dimTableHere/$dimTable/g" docs/CapstoneReport.md
perl -pi -e "s/classifierTableHere/$classifierTable/g" docs/CapstoneReport.md
#perl -pi -e "s/classifierGridSearchHere/$classGridTable/g" docs/CapstoneReport.md
perl -pi -e "s/regressorTableHere/$regressorTable/g" docs/CapstoneReport.md
pandoc docs/CapstoneReport.md -o docs/CapstoneReport.pdf
rm docs/CapstoneReport.md
google-chrome docs/CapstoneReport.pdf
