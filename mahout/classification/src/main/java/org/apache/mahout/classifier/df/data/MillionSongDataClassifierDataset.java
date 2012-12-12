package org.apache.mahout.classifier.df.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MillionSongDataClassifierDataset extends Dataset {

	public MillionSongDataClassifierDataset() 
	{
		super( getAttributes()
			 , getValues()
			 , 463715
			 , false
			 );
	}
		
	/**
	 * 90 numerical attributes and 1 label
	 * @return
	 */
	private static Attribute[] getAttributes()
	{
		
		Attribute[] ret = new Attribute[91];
		ret[0] = Attribute.LABEL;
		for(int i = 0;i < 90;++i)
		{
			ret[i+1] = Attribute.NUMERICAL;
		}
		return ret;
	}
	private static List<String>[] getValues()
	{
		List<String>[] ret = new ArrayList[91];
		ret[0] = new ArrayList<String>();
		for(int i = 1922;i <= 2011;++i)
		{
			ret[0].add("" + i);
		}
		for(int i = 0;i < 90;++i)
		{
			ret[i + 1] = Collections.emptyList();
		}
		return ret;
	}
	
	public static void persistIfDoesNotExist(Path datasetPath, FileSystem fs, Dataset dataset) throws IOException
	{
		if(fs.exists(datasetPath) && fs.isFile(datasetPath))
		{
			fs.delete(datasetPath, false);
		}
		FSDataOutputStream oStream = fs.create(datasetPath);
		dataset.write(oStream);
		oStream.flush();
		oStream.close();
	}
}
