package edu.iu.kmeansminibatch;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import edu.iu.fileformat.MultiFileInputFormat;

/**
 * MiniBatch Kmeans Harp Implementation
 * Nicolas Escobar and Harsh Reddy
 */
public class KmeansMiniBatchMapCollective extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new KmeansMiniBatchMapCollective(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception{
        if (args.length < 5) {
            System.err.println("Usage: KmeansMiniBatchMapCollective <batchSize> "
                    + "<number of map tasks> <number of iterations> <number of centroids> <workDir>\n");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        int batchSize = Integer.parseInt(args[0]);
        int numOfMapTasks = Integer.parseInt(args[1]);
        int numOfIterations = Integer.parseInt(args[2]);
        int numOfCentroids = Integer.parseInt(args[3]);
        String workDir = args[4];

        System.out.println("Launching KmeansMiniBatch..");

        launch(batchSize, numOfMapTasks, numOfIterations, numOfCentroids, workDir);

        System.out.println("KmeansMiniBatch Completed");


        return 0;

    }

    void launch(int batchSize, int numOfTasks, int numOfIterations, int numOfCentroids, String workDir)
            throws IOException, URISyntaxException, InterruptedException, ExecutionException, ClassNotFoundException {

        Configuration configuration = getConf();
        Path workDirPath = new Path(workDir);
        FileSystem fs = FileSystem.get(configuration);
        Path dataDir = new Path(workDirPath, "data");
        Path outDir = new Path(workDirPath, "out");

        // If folder exists, delete and recreate
        if (fs.exists(outDir)){
            fs.delete(outDir,true);
        }
        fs.mkdirs(outDir);

        int jobId = 0;

        long startTime = System.currentTimeMillis();

        //
        runMbKMeans(batchSize, numOfIterations, numOfCentroids, jobId, numOfTasks, configuration, workDirPath, dataDir, outDir);

        long endTime = System.currentTimeMillis();
        System.out.println("MB Kmeans Execution Time: "+ (endTime - startTime));

    }

    private void runMbKMeans(int batchSize, int numOfIterations, int numOfCentroids,
                           int JobID, int numOfMapTasks, Configuration configuration,
                           Path workDirPath, Path dataDir, Path outDir)
            throws IOException,URISyntaxException, InterruptedException,ClassNotFoundException {


        System.out.println("Starting Job");
        long jobSubmitTime;
        boolean jobSuccess = true;
        int jobRetryCount = 0;

        do {
            jobSubmitTime = System.currentTimeMillis();
            System.out.println("Start Job#" + JobID + " "+ new SimpleDateFormat("HH:mm:ss.SSS").format(Calendar.getInstance().getTime()));

            // Configure Job
            Job kmeansJob = configureMBKMeansJob(batchSize, numOfMapTasks, numOfCentroids,
                    configuration, workDirPath, dataDir, outDir, JobID, numOfIterations);

            System.out.println("| Job#"+ JobID+ " configure in "+ (System.currentTimeMillis() - jobSubmitTime)+ " miliseconds |");

            jobSuccess = kmeansJob.waitForCompletion(true);

            System.out.println("end Jod#" + JobID + " "
                    + new SimpleDateFormat("HH:mm:ss.SSS")
                    .format(Calendar.getInstance().getTime()));
            System.out.println("| Job#"+ JobID + " Finished in "
                    + (System.currentTimeMillis() - jobSubmitTime)
                    + " miliseconds |");

            // ---------------------------------------------------------
            if (!jobSuccess) {
                System.out.println("MB KMeans Job failed. Job ID:"+ JobID);
                jobRetryCount++;
                if (jobRetryCount == 3) {
                    break;
                }
            }else{
                break;
            }
        } while (true);

    }



    private Job configureMBKMeansJob(int batchSize,
                                   int numMapTasks, int numOfCentroids, Configuration configuration,Path workDirPath, Path dataDir,
                                   Path outDir, int jobID, int numIterations) throws IOException, URISyntaxException {

        Job job = Job.getInstance(configuration, "mbkmeans_job_"+ jobID);
        Configuration jobConfig = job.getConfiguration();
        Path jobOutDir = new Path(outDir, "mbkmeans_out_" + jobID);

        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(jobOutDir)) {
            fs.delete(jobOutDir, true);
        }
        FileInputFormat.setInputPaths(job, dataDir);
        FileOutputFormat.setOutputPath(job, jobOutDir);

        // Set Job Configuration Constants
        jobConfig.setInt(KMeansMiniBatchConstants.JOB_ID, jobID);
        jobConfig.setInt(KMeansMiniBatchConstants.NUM_ITERATIONS, numIterations);
        jobConfig.setInt(KMeansMiniBatchConstants.NUM_CENTROIDS, numOfCentroids);
        jobConfig.setInt(KMeansMiniBatchConstants.BATCH_SIZE, batchSize);
        jobConfig.set(KMeansMiniBatchConstants.WORK_DIR,workDirPath.toString());
        jobConfig.setInt(KMeansMiniBatchConstants.NUM_MAPPERS, numMapTasks);

        // Set Job Input Format
        job.setInputFormatClass(MultiFileInputFormat.class);

        // Set Jar by Class
        job.setJarByClass(KMeansMiniBatchConstants.class);

        //use different kinds of mappers
        job.setMapperClass(edu.iu.kmeansminibatch.KmeansMiniBatchMapper.class);

        JobConf jobConf = (JobConf) job.getConfiguration();
        jobConf.set("mapreduce.framework.name", "map-collective");
        jobConf.setNumMapTasks(numMapTasks);
        jobConf.setInt("mapreduce.job.max.split.locations", 10000);
        job.setNumReduceTasks(0);
        return job;
    }

}
