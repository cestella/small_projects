package com.hortonworks.sample;

import java.io.IOException;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.CommandLineUtil;

import ucar.ma2.Array;
import edu.ucsc.srl.damasc.netcdf.io.ArraySpec;
import edu.ucsc.srl.damasc.netcdf.io.input.NetCDFFileInputFormat;

public class ExtractAttributes extends Configured implements Tool
{

	public static class ExtractionMapper extends Mapper< edu.ucsc.srl.damasc.netcdf.io.ArraySpec
													 , ucar.ma2.Array
													 , Text
													 , NullWritable
													 >
	{
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			
		}
		
		@Override
		protected void map( ArraySpec key
						  , Array value
						  , Context context
						  )
				throws IOException, InterruptedException 
		{
			//extract data and write it out
		}
	}
	public Job configureJob(String inputPath, String outputPath) throws IOException
	{
		Job job = new Job();
	    job.setInputFormatClass(NetCDFFileInputFormat.class);
	    job.setMapperClass(ExtractionMapper.class);
	    job.setNumReduceTasks(0);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(NullWritable.class);
	    job.setJobName("Extraction Preprocessing");
	    job.setMapSpeculativeExecution(false);
	    job.setJarByClass(ExtractAttributes.class);
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    return job;
	}
	
	@Override
	public int run(String[] argv) throws Exception 
	{
		DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
	    ArgumentBuilder abuilder = new ArgumentBuilder();
	    GroupBuilder gbuilder = new GroupBuilder();
	    
	    Option inOpt = obuilder.withLongName("input")
	    						 .withShortName("i")
	    						 .withRequired(true)
	    						 .withArgument(abuilder.withName("path")
	    								 			   .withMinimum(1)
	    								 			   .withMaximum(1)
	    								 			   .create()
	    								 	  )
	    						 .withDescription("The input path (can be a glob)")
	    						 .create();
	    Option outOpt = obuilder.withLongName("output")
				 				.withShortName("o")
				 				.withRequired(true)
				 				.withArgument(abuilder.withName("path")
				 									  .withMinimum(1)
				 									  .withMaximum(1)
				 									  .create()
				 							 )
				 				.withDescription("The output path")
				 				.create();
	    Option helpOpt = obuilder.withLongName("help")
	    						 .withDescription("Print out help")
	    						 .withShortName("h")
	    						 .create();
	        
	    Group group = gbuilder.withName("Options")
	    					  .withOption(inOpt)
	    					  .withOption(outOpt)
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
	    	return -1;
	    }
	    if (cmdLine.hasOption("help"))
	    {
	        CommandLineUtil.printHelp(group);
	        return -1;
	    }
	    String inputPath = cmdLine.getValue(inOpt).toString();
	    String outputPath = cmdLine.getValue(inOpt).toString();
	    
	    Job job = configureJob(inputPath, outputPath);
	    job.waitForCompletion(true);
		return 0;
	}

	public static void main(String... argv) throws Exception
	{
		ToolRunner.run(new Configuration(), new ExtractAttributes(), argv);
	}
}
