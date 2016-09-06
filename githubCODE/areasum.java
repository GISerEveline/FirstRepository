package com.hadoop.esri.stats;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class areasum {
	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>{
		   public void map(LongWritable key, Text value, Context context) 
		        throws IOException, InterruptedException { 
					String line = value.toString(); 
					String [] values = line.split(",");
					String strArea = values[0];
					String strName =values[6];
					Text name = new Text(strName);
					float strAreafloat= Float.parseFloat(strArea); 
					context.write(name, new FloatWritable(strAreafloat)); 
				}
		}

		public static class Reduce extends  Reducer<Text, FloatWritable, Text, FloatWritable> { 

		  private FloatWritable result = new FloatWritable();

		  public void reduce(Text key, Iterable<FloatWritable> values,  Context context) throws IOException, InterruptedException {
		    float sum = (float) 0.0;
		      for (FloatWritable val : values) {
		        sum += val.get();
		      }
		      result.set(sum);
		      context.write(key, result); 
		}

		}

		public static void main(String[] args) throws Exception {
				 Configuration conf = new Configuration(); 
				 if (args.length <  2) {
				     System.err.println("Usage: areasum error");
				      System.exit(2);
				   }
				        Job job =new Job(conf,"AreaSum");
					job.setJarByClass(areasum.class);
					
					job.setMapperClass(Map.class);
					job.setCombinerClass(Reduce.class); 
					job.setReducerClass(Reduce.class);
					
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(FloatWritable.class);
					
					job.setInputFormatClass(TextInputFormat.class); 
					job.setOutputFormatClass(TextOutputFormat.class); 
					
					FileInputFormat.addInputPath(job, new Path(args[0]));
					FileOutputFormat.setOutputPath(job, new Path(args[1]));
					
					System.exit(job.waitForCompletion(true) ? 0 : 1);

		}
}
