jupyter nbconvert --to markdown docs/CapstoneReport.ipynb
dimTable=`cat data/dims.txt`
labelTable=`cat data/labels.txt`
perl -pi -e "s/labelTableHere/$labelTable/g" docs/CapstoneReport.md
perl -pi -e "s/dimTableHere/$dimTable/g" docs/CapstoneReport.md
pandoc docs/CapstoneReport.md -o docs/CapstoneReport.pdf
rm docs/CapstoneReport.md
google-chrome docs/CapstoneReport.pdf
