import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MapReduceStats {

  public static class Map 
            extends Mapper<LongWritable, Text, Text, FloatWritable>{

    private Text word = new Text();   // Output key for mapper, not used in this exercise
    private Float number;             // Output value for mapper
	
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	number = Float.parseFloat(value.toString());	// Convert string to Float
    	context.write(word, new FloatWritable(number)); // create a pair <keyword, value> 
    }
  }
  
  public static class Reduce
       extends Reducer<Text,FloatWritable,Text,MultipleWritable> {

    private MultipleWritable result = new MultipleWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	int count = 0;
    	float sum = 0;
    	float max = 0;
    	float min = 999999;
    	float avg = 0;
    	double std_dev = 0;
    	float aux = 0;
    	  
    	List<Float> flist = new ArrayList<Float>();
    	
    	for (FloatWritable val : values) {
          aux = val.get();
    	  max = Math.max(max, aux); // Calculate max value
    	  min = Math.min(min, aux); // Calculate min value
    	  sum += aux;  // Sum up values to be used later in avg calculation
    	  count += 1;  // Count number of values to be used later in avg calculation
    	  flist.add(aux); // Add values to list 
      }
    	avg = (float)sum/count;	// Calculate average/mean
    	
    	double stdsum = 0;
        double variance = 0;
    	
    	for (Float val : flist) {
    		// Calculate deviation of each value from the mean and square result
    		stdsum += Math.pow((Math.abs(val-avg)), 2);  
    	}
      
      variance = stdsum/count;       // Calculate variance
      std_dev = Math.sqrt(variance); // Calculate standard deviation
      
      // Set results into Composite variables
      result.val1 = max;
      result.val2 = min;
      result.val3 = avg;
      result.val4 = std_dev;
      
      context.write(key, result);
    }
  }

  // Composite class to return multiple values in Reduce stage
  public static class MultipleWritable implements Writable {
	  float val1;
	  float val2;
	  float val3;
	  double val4;
	  
	  public MultipleWritable() {}
	   
	   @Override
	   public void readFields(DataInput in) throws IOException {
	        val1 = in.readFloat();
	        val2 = in.readFloat();
	        val3 = in.readFloat();
	        val4 = in.readDouble();
	    }

	    @Override
	    public void write(DataOutput out) throws IOException {
	        out.writeFloat(val1);
	        out.writeFloat(val2);
	        out.writeFloat(val3);
	        out.writeDouble(val4);
	    }

	    @Override
	    public String toString() {
	        return "Max: " + this.val1 + "\tMin: " + this.val2 + "\tAvg: " + this.val3  + "\tStdDev: " + this.val4;
	    }
  }

  // Driver program
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(); 
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
    if (otherArgs.length != 2) {
      System.err.println("Usage: MapReduceStats <in> <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "mapreducestats");
    job.setJarByClass(MapReduceStats.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);    
    
    job.setOutputKeyClass(Text.class); // Set output key type   
    job.setOutputValueClass(MultipleWritable.class); // Set output value type
    job.setMapOutputValueClass(FloatWritable.class);  // Set Map output value type
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));   // Set the HDFS path of the input data
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); // Set the HDFS path for the output
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);  // Wait until job completion
  }
}
