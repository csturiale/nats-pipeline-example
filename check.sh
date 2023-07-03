correct=output/my-pipeline-0
for f in output/*;do
  cmp --silent $correct $f || echo "$f file is different"
done
