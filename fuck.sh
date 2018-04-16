sbt package
scp target/scala-2.11/association_rules_2.11-1.0.jar ccp:~/dog-testing/ar.jar
ssh ccp "(cd dog-testing; pwd; bash -c ./run.sh)"
