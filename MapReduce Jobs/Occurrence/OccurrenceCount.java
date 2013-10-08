/**  
 * OccuranceCount.java - It the MapReduce Driver Class  
 * @author  Tarini Dash
 * @version 1.0 
 */ 
package StanfordOccurrence;

import java.io.IOException;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.FileInputFormat; 
import org.apache.hadoop.mapred.FileOutputFormat; 
import org.apache.hadoop.mapred.JobClient; 
import org.apache.hadoop.mapred.JobConf;

public class OccurrenceCount { 

	public static void main(String[] args) throws IOException { 
		if (args.length != 3) { 
			System.err.println("Usage: Occurance-String  <input dir> <output dir>"); 
			System.exit(-1); 
		} 

		JobConf conf = new JobConf(OccurrenceCount.class); 
		conf.setJobName("Occurrence Count");
		conf.set("InOccurrence", args[0].toString());

		FileInputFormat.setInputPaths(conf, new Path(args[1])); 
		FileOutputFormat.setOutputPath(conf, new Path(args[2])); 

		conf.setMapperClass(OccurrenceMapper.class); 
		conf.setReducerClass(OccurrenceReducer.class); 
		conf.setMapOutputKeyClass(Text.class); 
		conf.setMapOutputValueClass(IntWritable.class); 

		conf.setOutputKeyClass(Text.class); 
		conf.setOutputValueClass(IntWritable.class); 

		JobClient.runJob(conf);

	}    

} 


