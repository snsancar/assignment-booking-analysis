# Commercial Booking Analysis

Provide detailed information on your product and how to run it here


## Dependencies

- Spark 3.x.x
- java 8
- Scala 2.12.x
- mvn (I'm using pom.xml to build the source code)

## Technology decisions

- Using Spark for reading and processing data ( because Spark is distributed, comes with very well tested functions)
- Using Scala as a API ( but the same can be written on Pyspark as well with the same performance, because not using any UDFs)
- Using only Spark built-in functions ( so Pyspark and Scala API give same performance, because of run time code generation)
- To build the code using Maven (i also used sbt , but I'm more familiar with Maven)


## Build / Run

SparkJobs should be scala objects that extend App and SparkJob. This makes them runnable from IDE, CLI and on cluster
eg. `object [MainClass] extends App with SparkJob`

Run the maven command to compile the source code and create an artifact(fat jar)

`mvn clean package`

### Run from IDE (IntelliJ)
Update the `application.conf` file in the resources directory with booking, airports data path also start and end dates to filter the bookings.
Go to the scala directory and run the main class `org.klm.assignment.TopDestinations`

### Run from Cluster

- Deploy the fat jar on any cluster and run the spark-submit command by pointing to main class(`org.klm.assignment.TopDestinations`)
- Could be also deployed on Kubernetes managed cluster ( using Helm chart , images, container registry)

### Unit test
- Copied couple of sample bookings data from the provided bookings json 
( I usually create test data using case class and create spark dataframe out of it. This takes time) 
- Only covered happy scenario for testing total no of passengers count and corresponding adult vs child ratio.

### Exception handling
- For exception handling, I'm using Scala Try block to cover the code.
- Exception handling is right now simple, but it works on Production env as well
- Explicit Logging is not used , but can be used to debug/troubleshoot the applications.
- Log level is set to WARN on the executors to minimise the logging.

### Assumptions

- Season is calculated in standard way (March, April and May as Spring etc)
- 

### Functional requirements covered as part of the solution
- Output shows the Spark Dataframe with Total no of Passengers count grouped with Destination Country, Season and Day of the week
- Also, Adult and Child count for the same grouping
- Using Spark to read the input files (so, it can read from local path, hdfs, dbfs, azure blob, s3 etc, just point the HDFS root path in the application.conf)
- Only confirmed bookings are considered
- Only KLM Airlines departing from The Netherlands are considered
- Filtered unknown airport codes from Airports dataset ( for example airport codes with "\N" value)

### Does NFR's covered in this solution?
- Using Spark to read the input files (so, it can scale for any volume of data, just point the HDFS root path in the application.conf)
- Solution should be runnable on both local and cluster ( since distributed computing used to achieve Horizontal scaling)

