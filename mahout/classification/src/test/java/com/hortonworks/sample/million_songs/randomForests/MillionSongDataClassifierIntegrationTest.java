package com.hortonworks.sample.million_songs.randomForests;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.df.DecisionForest;
import org.apache.mahout.classifier.df.data.MillionSongDataClassifierDataset;

public class MillionSongDataClassifierIntegrationTest extends TestCase 
{
	public static final String DATA_DIR = "src/test/data/million_songs";
	public static final String MODEL_DIR = DATA_DIR + "/meta";
	public static final String TRAINING_DATA = DATA_DIR + "/train.sample";
	public static final String TESTING_DATA  = DATA_DIR + "/test.sample";
	private Configuration conf;
	private FileSystem hdfs;
	private MillionSongDataClassifierDataset dataset = new MillionSongDataClassifierDataset();
	private Path datasetPath = new Path(MODEL_DIR + "/dataset.dat");
	@Override
	protected void setUp() throws Exception {
		conf = new Configuration();
		hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path(MODEL_DIR)))
		{
			//remove the model and dataset dir
			hdfs.delete(new Path(MODEL_DIR), true);
		}
		//create the dataset..
		MillionSongDataClassifierDataset.persistIfDoesNotExist(datasetPath, hdfs, dataset);
	}
	
	public void testTraining() throws Exception
	{
		Path outputPath = new Path("output");
		Path trainingDatasetPath = new Path(TRAINING_DATA);
		Path testingDatasetPath = new Path(TESTING_DATA);
		int numTrees = 35;
		if(hdfs.exists(outputPath))
		{
			hdfs.delete(outputPath);
		}
		
		DecisionForest forest = Train.trainForest(numTrees,trainingDatasetPath, datasetPath, 0, conf, hdfs, false, dataset);
		double[] errorRate = Train.evaluateForest(forest, testingDatasetPath, dataset, hdfs);
		System.out.println("Raw error rate: " + errorRate[0]);
    	System.out.println("Error expected from random chance: " + (1.0 - 1.0/10.0));
		System.out.println("Computed for " + numTrees + " trees...");
		
		
	}
}
