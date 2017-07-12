if [ ! -d classes ]; then
        mkdir classes;
fi

# Compile Project1
javac -classpath $HADOOP_HOME/hadoop-core-1.1.2.jar:$HADOOP_HOME/lib/commons-cli-1.2.jar -d ./classes MapReduceStats.java

# Create the Jar
jar -cvf mapreducestats.jar -C ./classes/ .
 
# Copy the jar file to the Hadoop distributions
cp mapreducestats.jar $HADOOP_HOME/bin/ 

