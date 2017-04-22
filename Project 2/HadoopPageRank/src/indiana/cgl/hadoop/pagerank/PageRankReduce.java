package indiana.cgl.hadoop.pagerank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.lang.StringBuffer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
 
public class PageRankReduce extends Reducer<LongWritable, Text, LongWritable, Text>{
	public void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		double sumOfRankValues = 0.0;
		String targetUrlsList = "";
		
		int sourceUrl = (int)key.get();
		int numUrls = context.getConfiguration().getInt("numUrls",1);
		
		for (Text value: values){
			String[] strArray = value.toString().split("#");

			// If length > 1, then link relation tuple
			// Concatenate links for the same key
			// <3,#0#1> and <3,#2#4> should generate <3,#0#1#2#4>
			if (strArray.length > 1){
				targetUrlsList = targetUrlsList.concat(value.toString());
			}
			// else rank value tuple
			// sum up PR values for the same key
			else if (value.toString().length() > 1) {
				sumOfRankValues += Double.parseDouble(value.toString());
			}
			
		} // end for loop
		sumOfRankValues = 0.85*sumOfRankValues+0.15*(1.0)/(double)numUrls;
		System.out.println("Reduce phase: ");
		System.out.println("<" + sourceUrl + "," + sumOfRankValues + targetUrlsList + ">");
		context.write(key, new Text(sumOfRankValues+targetUrlsList));
	}
}
