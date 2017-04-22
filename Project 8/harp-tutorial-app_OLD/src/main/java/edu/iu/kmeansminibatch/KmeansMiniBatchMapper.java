package edu.iu.kmeansminibatch;

import edu.iu.harp.example.DoubleArrPlus;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.Table;
import edu.iu.harp.resource.DoubleArray;
import edu.iu.kmeans.common.KMeansConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.CollectiveMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

public class KmeansMiniBatchMapper extends CollectiveMapper<String, String, Object, Object> {

	private int numMappers;
	private int vectorSize;
	private int iteration;
	private int batchSize;
	private int numCentroids;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		LOG.info("start setup" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()));
		long startTime = System.currentTimeMillis();
		Configuration configuration = context.getConfiguration();
    	batchSize = configuration.getInt(KMeansMiniBatchConstants.BATCH_SIZE, 100);
		numMappers = configuration.getInt(KMeansMiniBatchConstants.NUM_MAPPERS, 10);
    	vectorSize = configuration.getInt(KMeansMiniBatchConstants.VECTOR_SIZE, 20);
    	iteration = configuration.getInt(KMeansMiniBatchConstants.NUM_ITERATIONS, 1);
    	numCentroids = configuration.getInt(KMeansMiniBatchConstants.NUM_CENTROIDS, 10);
    	long endTime = System.currentTimeMillis();
    	LOG.info("config (ms) :" + (endTime - startTime));
	}

	protected void mapCollective( KeyValReader reader, Context context) throws IOException, InterruptedException {
	    long startTime = System.currentTimeMillis();
	    List<String> fileNames = new ArrayList<String>();
	    // Iterate over key/value pairs
	    while (reader.nextKeyValue()) {
	    	String key = reader.getCurrentKey();
	    	String value = reader.getCurrentValue();
	    	LOG.info("Key: " + key + " value: " + value);
			fileNames.add(value);
	    }
	    Configuration conf = context.getConfiguration();

	    // Execute main MiniBatch KMeans
	    runMBKmeans(fileNames, conf, context);
	}

	//
	private void runMBKmeans(List<String> fileNames, Configuration conf, Context context) throws IOException {

		// Load data from HDFS
		ArrayList<DoubleArray> dataPoints = loadData(fileNames, conf);

		// Load Centroids from HDFS and create Partitions
		Table<DoubleArray> centroidsTable = new Table<>(0, new DoubleArrPlus());
		if (this.isMaster()) {
			//loadCentroids(centroidsTable, conf.get(KMeansConstants.CFILE), conf);
            generateCentroids(centroidsTable, dataPoints);
		}

		// Broadcast centroids to all workers
		broadcastCentroids(centroidsTable);

		// Check centroids after broadcasting
        System.out.println("Check centroids after broadcasting");
		printTable(centroidsTable);

        Table<DoubleArray> prevCentroidsTable = null;

		// Loop over defined iterations
		for(int iter=0; iter < iteration; iter++){

		    // Get sample data points
            ArrayList<DoubleArray> sampleDataPoints = loadSampleData(dataPoints);

            prevCentroidsTable =  centroidsTable;
			centroidsTable = new Table<>(0, new DoubleArrPlus());

			System.out.println("Iteration No."+iter);

			// Find nearest centroid
            nearestCentroid(centroidsTable, prevCentroidsTable, sampleDataPoints);

			// Collect all local centroid calculations from workers
			allreduce("main", "allreduce_"+iter, centroidsTable);

			// Calculate final centroid based on MB
			calculateMBCentroids(centroidsTable, prevCentroidsTable);

			printTable(centroidsTable);


		}
		//output results
		if(this.isMaster()){
			outputCentroids(centroidsTable,  conf,   context);
		}

	}

    private ArrayList<DoubleArray> loadSampleData (ArrayList<DoubleArray> dataPoints){

        // Sample b/W data points
        int sampleSize = batchSize/numMappers;

        System.out.println("Sample size: " + sampleSize);
        System.out.println("Total data size: " + dataPoints.size());

	    // Load sample data b/W
        int dataSize = dataPoints.size();
        ArrayList<Integer> randomVectorList = new ArrayList<Integer>();
        ArrayList<DoubleArray> sampleDataPoints = new  ArrayList<DoubleArray>();
        for (int i=0; i<sampleSize; i++){
            boolean randomOk = false;
            Random random = new Random();
            int randomNo = random.nextInt(dataSize);
            // Check that random number is selected for the first time
            while (!randomOk) {
                if (!randomVectorList.contains(randomNo)) {
                    sampleDataPoints.add(dataPoints.get(randomNo));
                    randomVectorList.add(randomNo);
                    randomOk = true;
                }
                else {
                    randomNo = random.nextInt(dataSize);
                }
            }
        }

        System.out.println("Size of sample data points array" + sampleDataPoints.size());
        return sampleDataPoints;

    }

	private void broadcastCentroids( Table<DoubleArray> cenTable) throws IOException{
		 //broadcast centroids 
		  boolean isSuccess = false;
		  try {
			  isSuccess = broadcast("main", "broadcast-centroids", cenTable, this.getMasterID(),false);
		  } catch (Exception e) {
		      LOG.error("Fail to bcast.", e);
		  }
		  if (!isSuccess) {
		      throw new IOException("Fail to bcast");
		  }
	 }

	 /* Calculate nearest centroid based on centroids table and data points */
	 private void nearestCentroid(Table<DoubleArray> cenTable, Table<DoubleArray> previousCenTable, ArrayList<DoubleArray> sampleDataPoints){
		double err=0;
		// for x E Mw do
		 for(DoubleArray dataPoint: sampleDataPoints){
				double minDist = -1;
				double tempDist = 0;
				int nearestPartitionID = -1;
				// For each Partition from previous Centroid Table
				for(Partition ap: previousCenTable.getPartitions()){
					DoubleArray aCentroid = (DoubleArray) ap.get();

					// Calculate euclidean distance between each data point and centroid
					tempDist = calcEucDistSquare(dataPoint, aCentroid);

					// If this the minimum distance so far
					if(minDist == -1 || tempDist < minDist){
						// Calculate new minimum distance
					    minDist = tempDist;
						// Get ID of nearest partition
						nearestPartitionID = ap.id();
					}						
				}
				// Calculate error
				err+=minDist;
				
				// After finding nearest centroid for a data point
                // We create new vector to store data point
				double[] dataPointVec = new double[vectorSize+1];
				for(int j=0; j < vectorSize; j++){
                    dataPointVec[j] = dataPoint.get()[j];
				}
				// Last element will store the # of points clustered around this centroid (V[c])
                // V[c] = 1
                dataPointVec[vectorSize]=1;

                // If the new centroid table does not contain the partition ID, then we create one with the given data point and count
                // C[c] = x
                if(cenTable.getPartition(nearestPartitionID) == null){
					Partition<DoubleArray> tmpAp = new Partition<DoubleArray>(nearestPartitionID, new DoubleArray(dataPointVec, 0, vectorSize+1));
					cenTable.addPartition(tmpAp);
					 
				}
				// If the new centroid table already contains the partition ID, then we add to to current
				else{
					 Partition<DoubleArray> nearCenPartition = cenTable.getPartition(nearestPartitionID);
					 for(int i=0; i < vectorSize +1; i++){
                         // C[c] = C[c] + x (i<vectorSize)
                         // V[c] = V[c] = 1 (i==vectorSize)
                         nearCenPartition.get().get()[i] += dataPointVec[i];
					 }
				}
			  }

         // cenTable will contain the list of centroids with the accumulated data points (C[c]+x)
         // the last element of the DoubleArray will store the # of points clustered around the ID/centroid
		 System.out.println("Errors: "+err);
	 }
	

	  //output centroids
	  private void outputCentroids(Table<DoubleArray>  cenTable,Configuration conf, Context context){
		  String output="";
		  for( Partition<DoubleArray> ap: cenTable.getPartitions()){
			  double res[] = ap.get().get();
			  for(int i=0; i<vectorSize;i++)
				 output+= res[i]+"\t";
			  output+="\n";
		  }
			try {
				context.write(null, new Text(output));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  }
	  
	  // Get per-center learning rate and take gradient step
	  private void calculateMBCentroids(Table<DoubleArray> cenTable, Table<DoubleArray> prevCenTable){

	      // For each centroid partition
	      for(Partition<DoubleArray> curItPartition: cenTable.getPartitions()){

	          // Get previous iteration Centroid Partition for the current Centroid Partition ID
		      Partition<DoubleArray> prevItCenPart = prevCenTable.getPartition(curItPartition.id());

              double[] curItCenDouble = curItPartition.get().get();
		      double[] prevItCenDouble = prevItCenPart.get().get();

		      // V[c] = V[c]+V-1[c]
              curItCenDouble[vectorSize] += prevItCenDouble[vectorSize];

              // Iterate over centroid double array
			  for(int i = 0; i < vectorSize; i++){

			      // C[c] = (C-1[c]*V-1[c]+C[c])/V[c]
                  curItCenDouble[i] = (prevItCenDouble[i]*prevItCenDouble[vectorSize]+curItCenDouble[i])
                                        /curItCenDouble[vectorSize];

			  }
              curItCenDouble[vectorSize] = 0;
		  }
		  System.out.println("After calculation of new centroids");
		  printTable(cenTable);
	  }
	  
	  // Generate random centroids based on NUMBER_CENTROIDS
	  private void generateCentroids(Table<DoubleArray> cenTable, ArrayList<DoubleArray> dataPoints){
	      int partitionId = 0;
	      for (int i=0; i<numCentroids; i++){
              Random random = new Random();
              int randomIndex = random.nextInt(dataPoints.size());

              System.out.println("Random index " + randomIndex);

              double[] newCentroid = new double[vectorSize+1];
              double[] randomPoint = dataPoints.get(randomIndex).get();

              for (int j=0; j<vectorSize; j++){
                  newCentroid[j] = randomPoint[j];
              }

              newCentroid[vectorSize] = 0;

              Partition<DoubleArray> cenPart = new Partition<DoubleArray>(partitionId, new DoubleArray(newCentroid, 0, vectorSize+1));
              cenTable.addPartition(cenPart);
              partitionId++;
          }
          System.out.println("Done with Centroid Generation");

      }

	  // Calculate Euclidean Distance
	  private double calcEucDistSquare(DoubleArray aPoint, DoubleArray bPoint){
		  double dist=0;
		  for(int i=0; i < vectorSize; i++){
			  dist += Math.pow(aPoint.get()[i]-bPoint.get()[i],2);
		  }
		  return Math.sqrt(dist);
	  }
	  
	 // Create centroid partitions from HDFS
	 private void loadCentroids( Table<DoubleArray> centroidsTable, String cFileName, Configuration configuration) throws IOException{
		 Path cPath = new Path(cFileName);
		 FileSystem fs = FileSystem.get(configuration);
		 FSDataInputStream in = fs.open(cPath);
		 BufferedReader br = new BufferedReader(new InputStreamReader(in));
		 String line="";
		 String[] vector=null;
		 int partitionId=0;
		 while((line = br.readLine()) != null){
			 vector = line.split(",");
			 if(vector.length != vectorSize){
				  System.out.println("Errors while loading centroids .");
				  System.exit(-1);
			  }else{
				  double[] cenVec = new double[vectorSize+1];
				  
				  for(int i=0; i<vectorSize; i++){
					  cenVec[i] = Double.parseDouble(vector[i]);
				  }
				 cenVec[vectorSize]=0;
				  Partition<DoubleArray> ap = new Partition<DoubleArray>(partitionId, new DoubleArray(cenVec, 0, vectorSize+1));
				 centroidsTable.addPartition(ap);
				  partitionId++;
			  }
		 }
	 }
	  //load data form HDFS
	  private ArrayList<DoubleArray>  loadData(List<String> fileNames, Configuration conf) throws IOException{
		  ArrayList<DoubleArray> data = new  ArrayList<DoubleArray> ();
		  for(String filename: fileNames){
			  FileSystem fs = FileSystem.get(conf);
			  Path dPath = new Path(filename);
			  FSDataInputStream in = fs.open(dPath);
			  BufferedReader br = new BufferedReader( new InputStreamReader(in));
			  String line="";
			  String[] vector=null;
			  while((line = br.readLine()) != null){
				  vector = line.split(",");
				  vectorSize = vector.length;

				  if(vectorSize <= 0){
					  System.out.println("Errors while loading data.");
					  System.exit(-1);
				  }else{
                      double[] aDataPoint = new double[vectorSize];
				      for(int i=0; i<vectorSize; i++){
						  aDataPoint[i] = Double.parseDouble(vector[i]);
					  }
					  DoubleArray da = new DoubleArray(aDataPoint, 0, vectorSize);
					  data.add(da);
				  }
			  }
		  }
		  return data;
	  }
	  
	  //for testing
	  private void printTable(Table<DoubleArray> dataTable){
		  for( Partition<DoubleArray> ap: dataTable.getPartitions()){
			  
			  double res[] = ap.get().get();
			  System.out.print("ID: "+ap.id() + ":");
			  for(int i=0; i<res.length;i++)
				  System.out.print(res[i]+"\t");
			  System.out.println();
		  }
	  }
}