i=0
for s in "Filename" "Preflop" "Flop" "Turn" "River"
    do
    perl -pi -e "s/$s/$i$s/g" dims.csv
    ((i+=1))
done
cut -c2- dims.csv > dims.csv
