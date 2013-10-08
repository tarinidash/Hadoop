/**  
 * OccurrenceReducer.java - It the MapReduce Reduce Class to find an Occurrence Count 
 * @author  Tarini Dash
 * @version 1.0 
 */

package StanfordOccurrence;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class OccurrenceReducer extends MapReduceBase implements
Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
					throws IOException {

		int wordCount = 0;
		while (values.hasNext()) {
			IntWritable value = values.next();
			wordCount += value.get();
		}
		output.collect(key, new IntWritable(wordCount));
	}
}
