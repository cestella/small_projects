import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class OutputCompressed 
{

	public static class CompressionMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text>
	{
		public int getKey(Text key)
		{
			return (key.hashCode() & Integer.MAX_VALUE) % 20;
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(getKey(value)), value);
		}
	}
	
	public static class CompressionReducer extends Reducer<LongWritable, Text, Text, NullWritable>
	{
		@Override
		protected void reduce(LongWritable arg0, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException 
		{
			StringBuffer buff = new StringBuffer();
			int cntr = 0;
			for(Text value : values)
			{
				cntr = (cntr + 1)%200;
				if(cntr == 0)
				{
					context.write(new Text(buff.toString()), NullWritable.get());
					buff = new StringBuffer();
				}
				buff.append(value.toString());
			}
		}
	}
	
	public static class NoopMapper extends org.apache.hadoop.mapreduce.Mapper<Text, NullWritable, NullWritable, NullWritable>
	{
		@Override
		protected void map(Text key, NullWritable value,
				Context context)
				throws IOException, InterruptedException {
			
		}
	}
	
	
	
	public static enum CompressionType
	{
		BZIP(BZip2Codec.class),
		GZIP(GzipCodec.class),
		SNAPPY(SnappyCodec.class),
		NONE(null)
		;
		private Class<? extends CompressionCodec> clazz;
		
		public Class<? extends CompressionCodec> getClazz() {
			return clazz;
		}
		
		private CompressionType(Class<? extends CompressionCodec> clazz) {
			this.clazz = clazz;
		}
	}
	
	public static Job getCompressionJob(CompressionType type, String input, String output) throws IOException
	{
		Job job = new Job( new Configuration(), "Outputting Compressed - " + type);
		job.getConfiguration().set("mapred.submit.replication", "3");
		job.setJarByClass(OutputCompressed.class);
		job.setMapperClass(CompressionMapper.class);
		job.setReducerClass(CompressionReducer.class);
		
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(20);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(input));
		
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputPath(job, new Path(output));
		if(type.getClazz() == null)
		{
			SequenceFileOutputFormat.setCompressOutput(job, false);
		}
		else
		{
			SequenceFileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.setOutputCompressorClass(job, type.getClazz());
		}
		return job;
		
	}
	
	public static Job getNoopJob(String input, CompressionType type) throws IOException
	{
		Job job = new Job(new Configuration(), "Noop - " + type);
		job.getConfiguration().set("mapred.submit.replication", "3");
		job.setJarByClass(OutputCompressed.class);
		job.setMapperClass(NoopMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path(input));
		
		TextOutputFormat.setOutputPath(job, new Path("wc_out_" + type));
		
		
		return job;
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException 
	{
		CompressionType type = null;
		String output = null;
		Job job = null;
		if(args[0].equalsIgnoreCase("skip"))
		{
			type = CompressionType.valueOf(args[1]);
			output = args[2] + "_" + type;
		}
		else
		{
			type = CompressionType.valueOf(args[0]);
			output = args[2] + "_" + type;
			String input = args[1];
		
			job = getCompressionJob(type, input, output);
			job.waitForCompletion(true);
		}
		job = getNoopJob(output + "/part-r-*", type);
		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		System.out.println("noop for " + type + " took " + (end - start)/1000.0 + " seconds");

	}

}
