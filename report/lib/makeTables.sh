echo Filename,NumRows,NumCols > data/dims.csv
for f in $( echo ../samplefeatures/subsets/*.csv )
    do 
    fName=${f%.*}
    fName=${fName##*/}
    nRows=$(cat $f | wc -l)
    nCols=$(head -1 $f | sed 's/[^,]//g' | wc -c)
    echo $fName,$nRows,$nCols >> data/dims.csv
done
python lib/getLabels.py
python lib/CSVtoMD.py data/dims.csv > data/dims.txt
python lib/CSVtoMD.py data/labelBreakdown.csv > data/labels.txt
rm data/dims.csv data/labelBreakdown.csv
