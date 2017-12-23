#!/bin/bash
sbt package
mv ~/Documents/cloud-computing-skill-competition/target/scala-2.11/association_rules_2.11-1.0.jar ~/Documents/cloud-computing-skill-competition/target/scala-2.11/association_rules.jar
scp ~/Documents/cloud-computing-skill-competition/target/scala-2.11/association_rules.jar uniquesc@202.114.10.172:
ssh -t uniquesc@202.114.10.172 screen ./run.sh AR.Main association_rules.jar
