param(
  [int]$N = 20000,
  [int]$BatchLog = 500,
  [string]$Dir = "/bench/many"
)

Write-Host "Creating $N empty files in $Dir"

docker exec namenode hdfs dfs -mkdir -p $Dir | Out-Null

$sw = [System.Diagnostics.Stopwatch]::StartNew()

for ($i=1; $i -le $N; $i++) {
  docker exec namenode hdfs dfs -touchz "$Dir/f_$i" | Out-Null
  if ($i % $BatchLog -eq 0) {
    Write-Host "  created $i/$N"
  }
}

$sw.Stop()
Write-Host "Create time: $($sw.Elapsed.TotalSeconds) sec"

Write-Host "Sanity check:"
docker exec namenode hdfs dfs -count $Dir
