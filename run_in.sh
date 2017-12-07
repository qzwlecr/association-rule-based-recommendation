#!/bin/bash
mv ~/Documents/cloud-computing-skill-competition/target/scala-2.10/association_rules_2.10-1.0.jar ~/Documents/cloud-computing-skill-competition/target/scala-2.10/association_rules.jar
exp P@ssw0rd810 scp ~/Documents/cloud-computing-skill-competition/target/scala-2.10/association_rules.jar uniquesc@202.114.10.172:
exp P@ssw0rd810 ssh -t uniquesc@202.114.10.172 screen ./run.sh AR.Main association_rules.jar
