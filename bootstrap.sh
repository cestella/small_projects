#!/bin/bash

mvn archetype:generate -DarchetypeGroupId=org.apache.maven.archetypes -DgroupId=com.hortonworks.sample -DartifactId=$1 -DinteractiveMode=false -DarchetypeArtifactId=maven-archetype-quickstart
