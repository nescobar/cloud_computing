package indiana.cgl.hadoop.pagerank.helper;

import java.io.IOException;

import indiana.cgl.hadoop.pagerank.RankRecord;
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

public class SortRankValueReduce extends Reducer<LongWritable, Text, LongWritable, DoubleWritable>{

	public void reduce(LongWritable key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {
		
		HashMap<Integer,Double> rankValueHMap = new HashMap<Integer,Double>();
		
		// Store all nodes and rank values in HashMap
		for (Text value: values){
			String strLine = value.toString();
			RankRecord rrd = new RankRecord(strLine);
			rankValueHMap.put(rrd.sourceUrl,rrd.rankValue);
		}
		// Get sorted list of rank values
		List<Map.Entry<Integer,Double>> sortedHashMap = rankSortedByValues(rankValueHMap);
		 
		// Iterate through the list of HashMap entries
		for (Map.Entry<Integer, Double> entry : sortedHashMap)
		{
			System.out.println(entry.getKey() + "/" + entry.getValue());
			context.write(new LongWritable(entry.getKey()), new DoubleWritable(entry.getValue()));
		}
		
	}
	

// Implement Comparable to sort values of Hashmap
static <K,V extends Comparable<? super V>> 
	List<Map.Entry<K, V>> rankSortedByValues(Map<K,V> hashmap) {
	
	List<Map.Entry<K,V>> sortedMapList = new ArrayList<Map.Entry<K,V>>(hashmap.entrySet());
	
	// Sort HashMap using new Comparator
	Collections.sort(sortedMapList, 
		new Comparator<Map.Entry<K,V>>() {
		    @Override
		    public int compare(Map.Entry<K,V> val1, Map.Entry<K,V> val2) {
		        return val2.getValue().compareTo(val1.getValue());
		        // Applies descending order
		    }
		}
	);
	// Returned sorted HashMap
	return sortedMapList;
	}

}

