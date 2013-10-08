/**  
 * DeleteLine.java - Class to Delete a complete line of data at a specified line number.  
 * @author  Tarini Dash
 * @version 1.0 
 */ 

package StanfordDeleteLine;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

public class DeleteLine{
	public static void main (String [] args) throws Exception{
		if (args.length != 3) { 
			System.err.println("Usage: <Line# to Delete> <input file> <output file>"); 
			System.exit(-1); 
		} 

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		int lnToBeDeleted = Integer.parseInt(args[0]);
		Path inFile = new Path(args[1]);
		Path outFile = new Path(args[2]);
		if (!fs.exists(inFile)){
			System.out.println("Input file not found");
			System.exit(0);
		}
		if (!fs.isFile(inFile)){
			System.out.println("Input should be a file");
			System.exit(0);	
		}
		if (fs.exists(outFile)){
			System.out.println("Output already exists");
			System.exit(0);
		}				
		FSDataInputStream in = fs.open(inFile);
		BufferedReader reader=new BufferedReader(new InputStreamReader(in));
		FSDataOutputStream out = fs.create(outFile);
		BufferedWriter writer  =new BufferedWriter(new OutputStreamWriter(out));
		int i=1;
		String line = "";
		while ((line = reader.readLine()) != null) {
			if(lnToBeDeleted != i){
				writer.write(line + "\n");
			}	
			i++;
		}
		if(lnToBeDeleted > i){
			System.out.println("The # is greater than the lines available in the file." + "\n");
			System.out.println("No Lines Deleted.");
		}

		reader.close();
		writer.close();
		reader = null;
		in.close();
		out.close();
		fs.close();
	}
}
