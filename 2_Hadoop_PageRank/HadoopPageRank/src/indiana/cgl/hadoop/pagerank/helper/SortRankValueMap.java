package indiana.cgl.hadoop.pagerank.helper;

/*
 * collect the page rank results from previous computation.
 */

import indiana.cgl.hadoop.pagerank.RankRecord;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SortRankValueMap extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {
		
			// Submit same key with all values to the Reduce task
			context.write(new LongWritable(1), value);
		
		}
	
}
