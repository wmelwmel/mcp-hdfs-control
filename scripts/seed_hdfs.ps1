# Creates /data/raw and uploads two sample files into HDFS via namenode container.

docker exec -it namenode hdfs dfs -mkdir -p /data/raw | Out-Null

docker exec -it namenode bash -lc "echo 'hello hdfs' > /tmp/a.txt"
docker exec -it namenode bash -lc "printf 'id,value\n1,foo\n2,bar\n' > /tmp/sample.csv"

docker exec -it namenode hdfs dfs -put -f /tmp/a.txt /data/raw/a.txt
docker exec -it namenode hdfs dfs -put -f /tmp/sample.csv /data/raw/sample.csv

docker exec -it namenode hdfs dfs -ls /data/raw
