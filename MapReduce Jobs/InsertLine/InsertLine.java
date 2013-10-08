/**  
 * InsertLine.java - Class to insert a complete line of data (80 characters) at a specified line number.  
 * @author  Tarini Dash
 * @version 1.0 
 */ 

package StanfordInsertLine;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

public class InsertLine{
	public static void main (String [] args) throws Exception{
		if (args.length != 4) { 
			System.err.println("Usage: <Line #> <Insert String> <Input File> <Output File>"); 
			System.exit(-1); 
		} 
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		int lnNo = Integer.parseInt(args[0]);
		String newLine = args[1].toString();
		Path inFile = new Path(args[2]);
		Path outFile = new Path(args[3]);
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

			if(lnNo == i){
				writer.write(newLine + "\n");
			}
			writer.write(line + "\n");
			i++;
		}
		if(lnNo > i){
			writer.write(newLine + "\n");
		}
		reader.close();
		writer.close();
		reader = null;
		in.close();
		out.close();
		fs.close();
	}
}
