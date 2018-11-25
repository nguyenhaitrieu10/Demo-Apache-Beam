vim ~/.profile

PROJECT=[Your Project ID]
BUCKET=gs://dataflow-$PROJECT

cd ~/beam/examples/java8

export GOOGLE_APPLICATION_CREDENTIALS="/home/nguyenhaitrieu10/Apache Beam-d5a924605e0a.json"

mvn compile exec:java  -Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector \
-Dexec.args="$PROJECT game none"

mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.complete.game.LeaderBoard \
-Dexec.args="--runner=DataflowRunner \
  --project=$PROJECT \
  --tempLocation=$BUCKET/temp/ \
  --output=$BUCKET/leaderboard \
  --dataset=game \
  --topic=projects/$PROJECT/topics/game"
