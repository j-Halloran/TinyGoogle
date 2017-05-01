firstCom = "hdfs dfs -rmr /websearch"
secondCom = "/usr/local/hadoop/bin/hadoop jar ~/TinyGoogle/TinyGoogleMR.jar TinyGoogleMapReduce /TinyGoogle/books /web $1"
thirdCom = "rm ../results/results.json"
fourthCom = "cp ./results.json ../results/results.json"

eval $firstCom
echo "1"