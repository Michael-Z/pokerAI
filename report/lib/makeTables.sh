# get dimensions of every subset (rows, cols)
touch data/dims.csv
rm data/dims.csv
for f in $( echo ../data/test/data_engineered/subsets/classifier/*.csv )
    do 
    fName=${f%.*}
    fName=${fName##*/}
    nRows=$(cat $f | wc -l)
    nCols=$(head -1 $f | sed 's/[^,]//g' | wc -c)
    echo $fName,$nRows,$nCols >> data/dims.csv
done

# write header in first
d=$(<data/dims.csv)
echo -e "Filename,NumRows,NumCols\n$d" > data/dims.csv

# get label breakdown table
python lib/getLabels.py

# custom sort dims
i=0
for s in "Filename" "Preflop" "Flop" "Turn" "River"
    do
    perl -pi -e "s/$s/$i$s/g" data/dims.csv
    ((i+=1))
done
sort data/dims.csv -o data/dims.csv
cut -c2- data/dims.csv > data/dims2.csv
mv data/dims2.csv data/dims.csv

# CSV to markdown
python lib/CSVtoMD.py data/dims.csv > data/dims.txt
python lib/CSVtoMD.py data/labelBreakdown.csv > data/labels.txt

python lib/CSVtoMD.py data/classifierResults.csv > data/classifiers.txt
python lib/CSVtoMD.py data/regressorResults.csv > data/regressors.txt
python lib/CSVtoMD.py data/classifierGridSearch.csv > data/classGS.txt
python lib/CSVtoMD.py data/regressorGridSearch.csv > data/regressGS.txt

# create visualizations
Rscript lib/exploreViz.R
