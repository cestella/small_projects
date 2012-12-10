package com.hortonworks.sample.randomForests;

import java.io.BufferedWriter;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.df.DFUtils;
import org.apache.mahout.df.DecisionForest;
import org.apache.mahout.df.ErrorEstimate;
import org.apache.mahout.df.builder.DefaultTreeBuilder;
import org.apache.mahout.df.callback.ForestPredictions;
import org.apache.mahout.df.data.Data;
import org.apache.mahout.df.data.Dataset;
import org.apache.mahout.df.data.Instance;
import org.apache.mahout.df.mapreduce.Builder;
import org.apache.mahout.df.mapreduce.inmem.InMemBuilder;
import org.apache.mahout.df.mapreduce.partial.PartialBuilder;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class RandomForestTest extends TestCase 
{
	private static final Logger log = LoggerFactory.getLogger(RandomForestTest.class);
	public static String DATA_DIR="src/main/data";
	public static String TRAINING_SET = DATA_DIR + "/scratch/iris.training";
	public static String TESTING_SET = DATA_DIR + "/scratch/iris.testing";
	
	private static class RandomForestConfig
	{
		private boolean isPartial;
		private boolean isOob;
		private Path dataPath;
		private Path datasetPath;
		private int m;
		private int nbTrees;
		private long seed = 0;
		
		
		/**
		 * A configuration object for setting up and executing a random forest
		 * training attempt.
		 * 
		 * @param isPartial Use the Partial Data implementation
		 * @param isOob Estimate the out-of-bag error
		 * @param m Number of variables to select randomly at each tree-node
		 * @param nbTrees Number of trees to grow
		 * @param seed Random seed
		 * @param dataName data path
		 * @param datasetName dataset path
		 */
		public RandomForestConfig( boolean isPartial
								 , boolean isOob
								 , int m
								 , int nbTrees
								 , long seed
								 , String dataName
								 , String datasetName
								 )
		{
			this.isPartial = isPartial;
			this.isOob = isOob;
			this.m = m;
			this.nbTrees = nbTrees;
			this.seed = seed;
			this.dataPath = new Path(dataName);
			this.datasetPath = new Path(datasetName);
		}
		
		public Path getDataPath() {
			return dataPath;
		}
		public Path getDatasetPath() {
			return datasetPath;
		}
		public int getM() {
			return m;
		}
		public int getNbTrees() {
			return nbTrees;
		}
		public long getSeed() {
			return seed;
		}
		public boolean isOob() {
			return isOob;
		}
		public boolean isPartial() {
			return isPartial;
		}
	}
	
	
	
	private static class IrisDataset extends Dataset
	{
		private String location;
		public static enum Label
		{
			IrisSetosa("Iris-setosa")
		   ,IrisVersicolor("Iris-versicolor")
		   ,IrisVirginica("Iris-virginica")
		   ;
//			static Map<String, Label> nameToLabel = new HashMap<String, Label>();
//			static
//			{
//				for(Label l : Label.values())
//				{
//					nameToLabel.put(l.getLabelName(), l);
//				}
//			}
			private String labelName;
			Label(String labelName)
			{
				this.labelName = labelName;
			}
			public String getLabelName() {
				return labelName;
			}
			public static String getLabelName(int labelIndex)
			{
				if(labelIndex < 0)
				{
					return null;
				}
				return Label.values()[labelIndex].getLabelName();
			}
			public static List<String> getLabelNames()
			{
				List<String> ret = new ArrayList<String>();
				for(Label l : Label.values())
				{
					ret.add(l.getLabelName());
				}
				return ret;
			}
		}
		
		public IrisDataset(String location) 
		{
			super( new Attribute[]
					{
					  Attribute.NUMERICAL //sepal length in cm
					, Attribute.NUMERICAL //sepal width in cm
					, Attribute.NUMERICAL //petal length in cm
					, Attribute.NUMERICAL //petal width in cm
					, Attribute.LABEL //class
					}
				 , new List[] {
							 Collections.emptyList() // numerical, so no variety of data...
						   , Collections.emptyList() // ditto
						   , Collections.emptyList() // ditto
						   , Collections.emptyList() //ditto
						   , Label.getLabelNames()
							  }
				 , 150
				 );
			this.location = location;
		}
		
		public void persistIfDoesNotExist() throws IOException
		{
			File loc = new File(location);
			if(loc.exists())
			{
				return;
			}
			FileOutputStream s = new FileOutputStream(loc);
			DataOutput out = new DataOutputStream(s);
			this.write(out);
			s.flush();
			s.close();
		}
	}
	
	private DecisionForest buildForest(Dataset dataset, Configuration conf, RandomForestConfig randomForestConfig) throws IOException, ClassNotFoundException, InterruptedException {
		    DefaultTreeBuilder treeBuilder = new DefaultTreeBuilder();
		    treeBuilder.setM(randomForestConfig.getM());
		    
		    
		    
		    ForestPredictions callback = randomForestConfig.isOob() ? new ForestPredictions(dataset.nbInstances(), dataset.nblabels())
		        : null;
		    
		    Builder forestBuilder;
		    
		    if (randomForestConfig.isPartial()) {
		      log.info("Partial Mapred implementation");
		      forestBuilder = new PartialBuilder( treeBuilder
		    		  							, randomForestConfig.getDataPath()
		    		  							, randomForestConfig.getDatasetPath()
		    		  							, randomForestConfig.getSeed()
		    		  							, conf
		    		  							);
		    } else {
		      log.info("InMem Mapred implementation");
		      forestBuilder = new InMemBuilder( treeBuilder
		    		  						  , randomForestConfig.getDataPath()
		    		  						  , randomForestConfig.getDatasetPath()
		    		  						  , randomForestConfig.getSeed()
		    		  						  , conf
		    		  						  );
		    }
		    
		    log.info("Building the forest...");
		    long time = System.currentTimeMillis();
		    
		    DecisionForest forest = forestBuilder.build(randomForestConfig.getNbTrees(), callback);
		    
		    time = System.currentTimeMillis() - time;
		    log.info("Build Time: {}", DFUtils.elapsedTime(time));
		    
		    if (randomForestConfig.isOob()) {
		      Random rng = RandomUtils.getRandom(randomForestConfig.getSeed());

		      FileSystem fs = randomForestConfig.getDataPath().getFileSystem(conf);
		      int[] labels = Data.extractLabels(dataset, fs, randomForestConfig.getDataPath());
		      
		      log.info("oob error estimate : {}",
		               ErrorEstimate.errorRate(labels, callback.computePredictions(rng)));
		    }
		    
		    return forest;
		  }
	
	public void createDatasets(double percentTraining) throws Exception
	{
		File scratchDir = new File(DATA_DIR + "/scratch");
		if(scratchDir.exists())
		{
			//remove everything if it exists..want to recreate
			Files.deleteRecursively(scratchDir);
		}
		scratchDir.mkdir();
		List<String> data = Files.readLines(new File(DATA_DIR + "/iris.data"), Charsets.US_ASCII);
		Collections.shuffle(data);
		PrintWriter trainingFileWriter = new PrintWriter(new BufferedWriter(new FileWriter(new File(TRAINING_SET))));
		PrintWriter testingFileWriter = new PrintWriter(new BufferedWriter(new FileWriter(new File(TESTING_SET))));
		Iterator<String> it = data.iterator();
		int trainingBarrier = (int) Math.floor(percentTraining * data.size());
		for(int i = 0;i < data.size();++i)
		{
			(i < trainingBarrier ? trainingFileWriter:testingFileWriter).println(it.next());
		}
		trainingFileWriter.flush();
		testingFileWriter.flush();
		trainingFileWriter.close();
		testingFileWriter.close();
	}
	
	public double computeAccuracy(DecisionForest forest) throws IOException
	{
		List<String> data = Files.readLines(new File(TESTING_SET), Charsets.US_ASCII);
		int id = 0;
		int n = 0;
		int numWrong = 0;
		Random rng = new Random(0);
		for(String datum : data)
		{
			String[] tokens = datum.split(",");
			double[] features = new double[4];
			for(int i = 0;i < features.length;++i)
			{
				features[i] = Double.valueOf(tokens[i]);
			}
			Vector featureVector = new DenseVector(features);
			Instance inst = new Instance(++id, featureVector, -1 );
			int labelIdx = forest.classify(rng, inst);
			Assert.assertNotSame("Unable to classify", labelIdx, -1);
			String expectedLabel = tokens[4];
			String computedLabel = IrisDataset.Label.getLabelName(labelIdx);
			if(!expectedLabel.equals(computedLabel))
			{
				numWrong++;
			}
			++n;
		}
		return 1.0 - (1.0*numWrong)/(1.0*n);
	}
	
	public void testIrisDataset() throws Exception
	{
		Configuration conf = new Configuration();
		createDatasets(.8);
		String datasetLocation = DATA_DIR + "/iris.dataset";
		RandomForestConfig rfConfig = new RandomForestConfig(false, true, 10, 10, 0, TRAINING_SET, datasetLocation);
		
		IrisDataset dataset = new IrisDataset(datasetLocation);
		dataset.persistIfDoesNotExist();
		//Data data = DataLoader.loadData(dataset, fs, rfConfig.getDataPath());
		DecisionForest f = buildForest(dataset, conf, rfConfig);
		System.out.println("Computing accuracy: " + computeAccuracy(f));
	}
}
