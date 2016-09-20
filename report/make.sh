if ! [ -n "$(ls -A data)" ]
then
    . lib/makeTables.sh
fi
. lib/genReport.sh
