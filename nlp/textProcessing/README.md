#Introduction

This is a simple demonstration of how to use Tika to extract text
and metadata from binary files using Pig. This is done via a UDF. 

Also, in order to read files
from HDFS, a WholeFileLoader is created, which takes a directory and
loads generates tuples containing the location and the binary content.
This is obviously just for demonstration purposes to read files in; 
generally you would want to load many files into an aggregate file
format (i.e. sequence file via Sqoop or Mahout).

A demonstration pig script is provided to illustrate dumping a
directory's file contents along with metadata.

#Building

Building requires maven, but is self contained.  Requires only:
	
	mvn clean package

This will create a tarball in the target directory called textProcessing-1.0-SNAPSHOT-archive.tar.gz


#Usage

Usage is as follows, where the assumptions are that you are on a unix
machine and you have a directory in HDFS called input_dir with files in
it and wish to extract those files into output_dir, a directory on HDFS:
	
	tar xzvf textProcessing-1.0-SNAPSHOT-archive.tar.gz
	pig -param input=input_dir -param output=output_dir pig/dump.pig

#Caveats

This works best and on Hortonworks distribution of Hadoop as the maven
POM file uses the hortonworks distro.  Also, the assumption is that the
versions of pig and hadoop are at those of HDP 2.1.  That being said, it
SHOULD work on any hadoop installation post 2.0 since I do not package
pig or hadoop jar contents with the shaded jar.
