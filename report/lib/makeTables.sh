# get dimensions of every subset (rows, cols)
touch data/dims.csv
rm data/dims.csv
for f in $( echo ../data/test/data_engineered/subsets/*.csv )
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
    perl -pi -e "s/$s/$i$s/g" dims.csv
    ((i+=1))
done
cut -c2- dims.csv > dims.csv

# CSV to markdown
python lib/CSVtoMD.py data/dims.csv > data/dims.txt
python lib/CSVtoMD.py data/labelBreakdown.csv > data/labels.txt

# create visualizations
Rscript lib/exploreViz.R
