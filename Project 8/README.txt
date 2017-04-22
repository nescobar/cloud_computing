——————————————————————————————————
README - Project 8 Harp MB KMeans
——————————————————————————————————

Nicolas Escobar
Harsh Reddy

———————————————————————————
1. Mini Batch KMeans Report
———————————————————————————

The detailed report can be found in the document: escobarn_HarpMBKmeans_report
It includes the details of the implementation and an analysis of the output clusters

———————————————————————————
2. Bonus Points Report
———————————————————————————

The Bonus Points report can be found in the document: escobarn_Project8_BonusPoints
It includes the details of the experiments done on local VM and FutureSystems nodes

———————————————————————————
3. Source Code and JAR
———————————————————————————

Source code for our implementation can be found in source/src/main/java/edu/iu/kmeansminibatch

Files are:
1. KmeansMiniBatchMapCollective.java
2. KmeansMiniBatchMapper.java
3. KMeansMiniBatchConstants.java

Compiled jar can be found in source/harp-tutorial-app/target/harp-tutorial-app-1.0-SNAPSHOT.jar

Parameters for running our application are the following:

hadoop jar harp-tutorial-app-1.0-SNAPSHOT.jar edu.iu.kmeansminibatch.KmeansMiniBatchMapCollective 
                <batchSize> <numOfMappers> <numOfIterations> <numOfCentroids> <workDir>

———————————————————————————
4. Input data set
———————————————————————————

Input data can be found in data folder (small and large data sets that were used in our tests were included)
It includes anonymized customers migrations (post-paid to pre-paid) from a Telco company

Columns and descriptions
————————————————————————
AGEING: Number of days since the customer activated the line in post-paid
AVG_REV_3M_POS: Average revenue for last 3 months in post-paid
AVG_MB_3M_POS: Average data traffic for last 3 months in post-paid	
AVG_MOU_3M_POS: Average voice traffic for last 3 months in post-paid
AVG_PAQUETIGOS_3M: Average package consumption for last 3 months in post-paid
RCHRG_QTY: Recharge quantity for the last month
MOST_FREQ_RCHRG_HIGH: Most frequent (highest) recharge amount
MOST_FREQ_RCHRG_LOW: Most frequent (lowest) recharge amount
MOU_OUTGOING: Total voice traffic outgoing
MOU_INCOMING: Total voice traffic incoming
AVG_REV_3M_PRE: Average revenue for first three months in prepaid
AVG_MB_3M_PRE: Average data traffic for first three months in prepaid
AVG_MOU_3M_PRE: Average voice traffic for first three months in prepaid

———————————————————————————
5. Output
———————————————————————————

The output data can be found in the output folder
It includes the output collected from HDFS and also in an Excel file for better understanding
The detailed analysis of the output can be found in the report (escobarn_HarpMBKmeans_report)

