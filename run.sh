#!/bin/bash
sbt package
mv ~/Documents/cloud-computing-skill-competition/target/scala-2.11/association_rules_2.11-1.0.jar ~/Documents/cloud-computing-skill-competition/target/scala-2.11/association_rules.jar
scp ~/Documents/cloud-computing-skill-competition/target/scala-2.11/association_rules.jar root@router:
ssh root@router scp association_rules.jar node307:
ssh -t root@router ssh -t node307 screen ./run.sh AR.Main association_rules.jar
