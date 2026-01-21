param(
  [int]$N = 20000,
  [int]$BatchLog = 500,
  [string]$Dir = "/bench/many",
  [int]$NonZeroFirst = 1000,
  [int]$NonZeroBytes = 1024
)

Write-Host "Creating $N files in $Dir"
Write-Host "  - first $NonZeroFirst files will be $NonZeroBytes bytes"
Write-Host "  - remaining files will be empty (touchz)"

docker exec namenode hdfs dfs -mkdir -p $Dir | Out-Null

$sw = [System.Diagnostics.Stopwatch]::StartNew()

for ($i=1; $i -le $N; $i++) {

  if ($i -le $NonZeroFirst) {
    docker exec namenode bash -lc "head -c $NonZeroBytes /dev/zero > /tmp/f_$i.bin" | Out-Null
    docker exec namenode hdfs dfs -put -f "/tmp/f_$i.bin" "$Dir/f_$i" | Out-Null
  } else {
    docker exec namenode hdfs dfs -touchz "$Dir/f_$i" | Out-Null
  }

  if ($i % $BatchLog -eq 0) {
    Write-Host "  created $i/$N"
  }
}

$sw.Stop()
Write-Host "Create time: $($sw.Elapsed.TotalSeconds) sec"

Write-Host "Sanity check:"
docker exec namenode hdfs dfs -count $Dir
