package org.apache.mahout.classifier.df.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Function;

public class MillionSongDataClassifierDataset extends Dataset {

	public MillionSongDataClassifierDataset() 
	{
		super( getAttributes()
			 , getValues()
			 , 463715
			 , false
			 );
	}
		
	public static Function<String, String> CATEGORY_CONVERTER = new Function<String, String>()
			{
				@Override
				public String apply(String input) {
					int year = Integer.parseInt(input);
					return 10*Math.round( (year - 1900) / 10) + "";
				}
			};
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
		Set<String> categories = new HashSet<String>();
		for(int i = 1922;i <= 2011;++i)
		{
			categories.add(CATEGORY_CONVERTER.apply("" + i));
		}
		ret[0] = new ArrayList<String>(categories);
		for(int i = 0;i < 90;++i)
		{
			ret[i + 1] = new ArrayList<String>();
		}
		return ret;
	}
	
	public static void persistIfDoesNotExist(Path datasetPath, FileSystem fs, Dataset dataset) throws IOException
	{
		if(!fs.exists(datasetPath))
		{
			FSDataOutputStream oStream = fs.create(datasetPath);
			dataset.write(oStream);
			oStream.flush();
			oStream.close();
		}
		
	}
}
