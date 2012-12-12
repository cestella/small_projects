package org.apache.mahout.classifier.df.data;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IrisDataset extends Dataset
	{
		
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
				for(IrisDataset.Label l : Label.values())
				{
					ret.add(l.getLabelName());
				}
				return ret;
			}
		}
		private String location;
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
				 , false
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