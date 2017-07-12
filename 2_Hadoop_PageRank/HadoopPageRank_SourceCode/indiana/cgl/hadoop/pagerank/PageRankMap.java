package indiana.cgl.hadoop.pagerank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
 
public class PageRankMap extends Mapper<LongWritable, Text, LongWritable, Text> {

// each map task handles one line within an adjacency matrix file
// key: file offset
// value: <sourceUrl PageRank#targetUrls>
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			int numUrls = context.getConfiguration().getInt("numUrls",1);
			String line = value.toString();
			StringBuffer sb = new StringBuffer();
			// instance an object that records the information for one webpage
			RankRecord rrd = new RankRecord(line);
			int targetUrl;
			// double rankValueOfSrcUrl;
			if (rrd.targetUrlsList.size()<=0){
				// there is no out degree for this web page; 
				// scatter its rank value to all other URLs
				double rankValuePerUrl = rrd.rankValue/(double)numUrls;
				for (int i=0;i<numUrls;i++){
				context.write(new LongWritable(i), new Text(String.valueOf(rankValuePerUrl)));
				}
			} else {
				// P(targetURL) = P(sourceURL)/out degree
				
				// For each target web page, create pair <targetUrl, rankValueTargetUrl>
				for (int i=0;i<rrd.targetUrlsList.size();i++){
					double rankValueTargetUrl = rrd.rankValue/rrd.targetUrlsList.size();
					targetUrl = rrd.targetUrlsList.get(i);
					System.out.println("Map Phase: ");
					System.out.println("<" + targetUrl + "," + rankValueTargetUrl + ">");
					context.write(new LongWritable(targetUrl), new Text(String.valueOf(rankValueTargetUrl)));
				}
			} 
			// For each target webpage, create pair <sourceUrl, #targetUrls>
			for (int i=0;i<rrd.targetUrlsList.size();i++){
				targetUrl = rrd.targetUrlsList.get(i);
				sb.append("#"+String.valueOf(targetUrl));
			}
			
			context.write(new LongWritable(rrd.sourceUrl), new Text(sb.toString()));
		} // end map

}
