declare -a sources=("ABS" "FTP" "ONG" "PS" "PTY")
declare -a stakes=("50" "100" "200" "400" "600" "1000")
for sr in "${sources[@]}"
  do 
    for st in "${stakes[@]}"
      do 
        wget "http://web.archive.org/web/20110205042259/http://static.handhq.com/hands/obfuscated/$sr-2009-07-01_2009-07-23_${st}NLH_OBFU.zip"
      done
  done
unzip \*.zip
