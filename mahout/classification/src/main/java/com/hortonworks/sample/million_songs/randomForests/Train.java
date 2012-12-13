package com.hortonworks.sample.million_songs.randomForests;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.df.DFUtils;
import org.apache.mahout.classifier.df.DecisionForest;
import org.apache.mahout.classifier.df.ErrorEstimate;
import org.apache.mahout.classifier.df.builder.DefaultTreeBuilder;
import org.apache.mahout.classifier.df.data.Data;
import org.apache.mahout.classifier.df.data.DataLoader;
import org.apache.mahout.classifier.df.data.Dataset;
import org.apache.mahout.classifier.df.data.MillionSongDataClassifierDataset;
import org.apache.mahout.classifier.df.mapreduce.Builder;
import org.apache.mahout.classifier.df.mapreduce.MapredOutput;
import org.apache.mahout.classifier.df.mapreduce.partial.PartialBuilder;
import org.apache.mahout.classifier.df.mapreduce.partial.TreeID;
import org.apache.mahout.classifier.df.node.Node;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uncommons.maths.Maths;

public class Train 
{
	private static final Logger log = LoggerFactory
			.getLogger(Train.class);
	static DecisionForest trainForest( int m
									 , int numTrees
									 , Path dataPath
									 , Path datasetPath
									 , long seed
									 , Configuration conf
									 , FileSystem hdfs
									 , boolean recover
									 ) throws IOException, ClassNotFoundException, InterruptedException
	{
		DefaultTreeBuilder treeBuilder = new DefaultTreeBuilder();
		treeBuilder.setM(m);

		Builder forestBuilder;

		
		forestBuilder = new PartialBuilder(treeBuilder,
											dataPath,
											datasetPath,
											seed, conf);
		DecisionForest forest = null;
		if(!recover)
		{
			log.info("Building the forest...");
			long time = System.currentTimeMillis();
	
			forest = forestBuilder.build(numTrees);
			
			time = System.currentTimeMillis() - time;
			log.info("Build Time: {}", DFUtils.elapsedTime(time));
		}
		else
		{
			System.out.println("Recovering...");
			Path outputPath = new Path("output");
			Path[] outfiles = DFUtils.listOutputFiles(hdfs, outputPath);
			//TreeID[] keys = new TreeID[numTrees];
			Node[] trees = new Node[numTrees];
		    // read all the outputs
		    int index = 0;
		    for (Path path : outfiles) {
		      for (Pair<TreeID,MapredOutput> record : new SequenceFileIterable<TreeID, MapredOutput>(path, conf)) {
		        TreeID key = record.getFirst();
		        MapredOutput value = record.getSecond();
//		        if (keys != null) {
//		          keys[index] = key;
//		        }
		        if (trees != null) {
		          trees[index] = value.getTree();
		        }
		        index++;
		      }
		    }
		    forest = new DecisionForest(Arrays.asList(trees));
		}
		return forest;
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] argv) throws IOException, ClassNotFoundException, InterruptedException 
	{
		
		DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
	    ArgumentBuilder abuilder = new ArgumentBuilder();
	    GroupBuilder gbuilder = new GroupBuilder();
	    
	    Option trainOpt = obuilder.withLongName("training_data")
	    						 .withShortName("t")
	    						 .withRequired(false)
	    						 .withArgument(abuilder.withName("path")
	    								 			   .withMinimum(1)
	    								 			   .withMaximum(1)
	    								 			   .create()
	    								 	  )
	    						 .withDescription("The input path (can be a glob)")
	    						 .create();
	    Option testOpt = obuilder.withLongName("testing_data")
				 				  .withShortName("e")
				 				  .withRequired(false)
								 .withArgument(abuilder.withName("path")
										 			   .withMinimum(1)
										 			   .withMaximum(1)
										 			   .create()
										 	  )
								 .withDescription("The input path (can be a glob)")
								 .create();
	    Option modelOpt = obuilder.withLongName("model_dir")
					 				.withShortName("m")
					 				.withRequired(false)
					 				.withArgument(abuilder.withName("path")
					 									  .withMinimum(1)
					 									  .withMaximum(1)
					 									  .create()
					 							 )
					 				.withDescription("The random forest to use")
					 				.create();
	   
	    Option treeOpt = obuilder.withLongName("numTrees")
				 				.withShortName("n")
				 				.withRequired(false)
				 				.withArgument(abuilder.withName("num")
				 									  .withMinimum(1)
				 									  .withMaximum(1)
				 									  .create()
				 							 )
				 				.withDescription("The number of trees to use")
				 				.create();
	    Option helpOpt = obuilder.withLongName("help")
	    						 .withDescription("Print out help")
	    						 .withShortName("h")
	    						 .create();
	        
	    Group group = gbuilder.withName("Options")
	    					  .withOption(trainOpt)
	    					  .withOption(testOpt)
	    					  .withOption(treeOpt)
	    					  .withOption(modelOpt)
	    					  .withOption(helpOpt)
	    					  .create();
	    CommandLine cmdLine = null;
	    try {
	        Parser parser = new Parser();
	        parser.setGroup(group);
	        cmdLine = parser.parse(argv);
	    }
	    catch(OptionException oe)
	    {
	    	System.err.println("Invalid options, usage:");
	    	CommandLineUtil.printHelp(group);
	    	System.exit(-1);
	    }
	    if (cmdLine.hasOption(helpOpt))
	    {
	        CommandLineUtil.printHelp(group);
	        System.exit(-1);
	    }
	    DecisionForest forest = null;
	    Configuration config = new Configuration();
	    config.set("mapred.submit.replication", "3");
	    FileSystem hdfs = FileSystem.get(config);
	    Path modelLocation = new Path(cmdLine.getValue(modelOpt) + "/model.dat");
	    Path datasetPath = new Path(cmdLine.getValue(modelOpt) + "/dataset.dat");
	    Dataset dataset = null;
	    
	    if(cmdLine.hasOption(trainOpt))
	    {
	    	Path trainingSetPath = new Path((String) cmdLine.getValue(trainOpt));
	    	
	    	dataset = new MillionSongDataClassifierDataset();
	    	MillionSongDataClassifierDataset.persistIfDoesNotExist(datasetPath, hdfs, dataset);
	    	//train...
	    	int numTrees = Integer.parseInt((String)cmdLine.getValue(treeOpt));
	    	int m = (int) Math.floor(Maths.log(2, dataset.nbAttributes()) + 1);
	    	forest = trainForest(m, numTrees, trainingSetPath, datasetPath, 0, config, hdfs, hdfs.exists(new Path("output")));
	    	//persist
	    	
	    	if(hdfs.exists(modelLocation))
	    	{
	    		if(!hdfs.isFile(modelLocation))
		    	{
		    		throw new IllegalStateException("You tried to write to a model that was a directory..weird..don't do that.");
		    	}
	    		hdfs.delete(modelLocation, false);
	    	}
	    	
	    	FSDataOutputStream modelOutStream = hdfs.create(modelLocation);
	    	forest.write(modelOutStream);
	    	modelOutStream.flush();
	    	modelOutStream.close();
	    }
	    else if(hdfs.exists(modelLocation))
	    {
	    	//read from disk
	    	forest = DecisionForest.load(config, modelLocation);
	    	dataset = Dataset.load(config, datasetPath);
	    }
	    else
	    {
	    	throw new IllegalStateException("You neither specified training *nor* gave me a valid model location..this isn't good in the slightest.");
	    }
	    
	    if(cmdLine.hasOption(testOpt))
	    {
	    	//Evaluate on the testing set...
	    	Path testingSetPath = new Path((String) cmdLine.getValue(testOpt));
	    	Data testData = DataLoader.loadData(dataset, hdfs, testingSetPath);
	    	System.out.println("Read " + testData.size() + " entries...");
	    	System.out.println("Each data point has " + dataset.nbAttributes() + " attributes and " + dataset.nblabels() + " labels...");
			double[] testLabels = testData.extractLabels();
			double[] predictions = new double[testData.size()];
			forest.classify(testData, predictions);
			ErrorEstimate.errorRate(testLabels, predictions);
			System.out.println("Computing error rate: " + ErrorEstimate.errorRate(testLabels, predictions));
	    }
	    
	}

}
