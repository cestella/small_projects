#!/bin/bash
mvn install:install-file -Dfile=./hadoop-core-1.0.3.16.jar -DgroupId=third_party \
    -DartifactId=hadoop-core -Dversion=1.0.3.16 -Dpackaging=jar
mvn install:install-file -Dfile=./hadoop-scidata.jar -DgroupId=third_party \
    -DartifactId=hadoop-scidata -Dversion=1.0 -Dpackaging=jar
mvn install:install-file -Dfile=./mahout-core-0.7.jar -DgroupId=third_party \
    -DartifactId=mahout-core -Dversion=0.7 -Dpackaging=jar
mvn install:install-file -Dfile=./mahout-examples-0.7.jar -DgroupId=third_party \
    -DartifactId=mahout-examples -Dversion=0.7 -Dpackaging=jar
mvn install:install-file -Dfile=./mahout-integration-0.7.jar -DgroupId=third_party \
    -DartifactId=mahout-integration -Dversion=0.7 -Dpackaging=jar
mvn install:install-file -Dfile=./mahout-math-0.7.jar -DgroupId=third_party \
    -DartifactId=mahout-math -Dversion=0.7 -Dpackaging=jar
mvn install:install-file -Dfile=./netcdfAll-4.2.jar -DgroupId=third_party \
    -DartifactId=netcdfAll -Dversion=4.2 -Dpackaging=jar
