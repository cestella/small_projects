package com.hortonworks.sample.million_songs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.classifier.df.data.MillionSongDataClassifierDataset;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

public class ConvertDataset {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException 
	{
		BufferedReader reader = new BufferedReader(new FileReader(args[0]));
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(args[1])));
		String line = null;
		while( ( line = reader.readLine()) != null)
		{
			Iterable<String> tokens = Splitter.on(',').split(line);
			List<String> reconstructedString = new ArrayList<String>();
			int tokenIdx = 0;
			for(String token : tokens)
			{
				if(tokenIdx++ == 0)
				{
					token = MillionSongDataClassifierDataset.CATEGORY_CONVERTER.apply(token);
				}
				reconstructedString.add(token);
			}
			writer.println(Joiner.on(',').join(reconstructedString));
		}
		reader.close();
		writer.flush();
		writer.close();
	}

}
