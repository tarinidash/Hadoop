/**  
 * OccuranceCount.java - It the MapReduce Mapper Class to find an Occurrence Count 
 * @author  Tarini Dash
 * @version 1.0 
 */

package StanfordOccurrence;

import java.io.IOException;
import java.util.regex.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class OccurrenceMapper extends MapReduceBase implements
Mapper<LongWritable, Text, Text, IntWritable> {

	private static String OccurrenceIn;
	public void configure(JobConf job) {
		OccurrenceIn = job.get("InOccurrence").toString();
	}
	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
					throws IOException {
		String s = value.toString();
		Pattern p = Pattern.compile(OccurrenceIn);
		Matcher m = p.matcher(s);
		while (m.find()){
			output.collect(new Text(OccurrenceIn), new IntWritable(1));
		}
	}
}
