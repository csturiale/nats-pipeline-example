for s in $(nats stream ls -n); do
  echo $s
  nats stream --timeout 30s rm $s -f
done

find output -maxdepth 1 -name 'my-pipeline-*' -delete
find logs -maxdepth 1 -name 'my-pipeline-*' -delete
find timings -maxdepth 1 -name 'my-pipeline-*' -delete
for f in output/*; do  rm -f "$f"; done
for f in logs/*; do  rm -f "$f"; done
for f in timings/*; do  rm -f "$f"; done
rm -rf kubernetes/*.log
rm -rf kubernetes/kubernetes
rm -rf git/*.log
rm -rf git/git
rm -rf remote-executor/*.log
rm -rf remote-executor/remote-executor
for p in $(ps -ef | grep "./kubernetes" | grep -v grep | awk '{print $2}');do
  echo $p
  kill -9 $p
done
for p in $(ps -ef | grep "./git" | grep -v grep | awk '{print $2}');do
  kill -9 $p
done
for p in $(ps -ef | grep "./remote-executor" | grep -v grep | awk '{print $2}');do
  kill -9 $p
done
