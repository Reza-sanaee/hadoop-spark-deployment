## Processing model and Spark abstractions

Batch processing operates on a bounded dataset and suits historical analysis, ETL, and offline training. Streaming operates on an unbounded input and continuously produces updated results, often through micro-batches. Latency depends on ingestion, trigger interval, computation, and scheduling; calling a pipeline streaming does not establish a millisecond deadline. An RDD is an immutable partitioned distributed collection with lineage for recovery. Transformations construct a lazy computation graph; actions trigger execution. RDDs remain useful for custom low-level transformations and assignment constraints. They do not require the entire dataset to reside in RAM, and a lost partition can often be recomputed from its lineage. DataFrames expose named columns and a schema, allowing query optimization. Datasets add a typed interface in languages such as Scala and Java; Python does not have the same typed Dataset API. Type safety is not a guarantee against every runtime error, and optimization benefits depend on the operation used. The driver constructs the application, plans work, and coordinates jobs, stages, and tasks. The cluster manager allocates resources. Executors run tasks and may cache partitions. A shuffle redistributes data by key and can incur network transfer, serialization, disk I/O, and skew. reduceByKey can combine values locally before shuffle, whereas groupByKey usually transfers the raw grouped values.

## Persistence, memory, and execution optimization

Caching is useful when a dataset is reused, especially in iterative processing. MEMORY_ONLY favors memory speed but may need recomputation when partitions do not fit. MEMORY_AND_DISK keeps overflow on disk. Serialized storage trades CPU overhead for a smaller representation in supported runtimes, while replicated levels retain additional copies. Python RDD objects are already serialized for storage; a serialized label does not mean a guaranteed compression ratio. RDD cache defaults and DataFrame defaults differ, so they should not be treated as one universal setting. Executor OOM may result from a large partition, skewed keys, excessive caching, oversized broadcasts, object overhead, or insufficient memory relative to concurrent tasks. Remedies include repartitioning, handling skew, reducing collected/broadcast data, unpersisting unused datasets, and adjusting resources. Increasing memory alone may leave the underlying skew unresolved. AQE uses runtime statistics to coalesce shuffle partitions, change suitable join strategies, and handle supported skewed joins. It does not optimize every operation automatically. Tungsten reduces overhead through compact representations and execution optimizations; whole-stage code generation can fuse supported operators into generated JVM code. These mechanisms do not mean that all computation becomes native machine code or that every garbage-collection cost disappears.

## Deployment, file formats, and resource sharing

Standalone is suitable for a dedicated, relatively simple Spark cluster. YARN integrates with an existing Hadoop resource environment. Kubernetes offers container-oriented scheduling, but requires operational configuration for resources, storage, networking, and security. No deployment mode provides correct scaling or security merely by being selected. Parquet and ORC support column pruning, compression, and statistics-based data skipping; they are especially useful when reading selected columns from large analytical datasets. Delta Lake adds transactional table management and historical versions, subject to retained logs/data. Z-ordering can improve data locality and skipping; it is not an authorization mechanism. Warehouse versus lakehouse selection depends on governance, workload, infrastructure, and cost rather than a strict old-versus-new distinction. FIFO and Fair scheduling concern jobs sharing a Spark context; cluster-manager allocation between applications is a separate layer. Fair pools can specify weights and minimum shares. Tasks already running are normally not preempted simply because another pool submits work. Pool properties must be set in the appropriate submitting thread. Dynamic allocation requests or releases executors according to workload and configured limits; it requires a compatible way to preserve shuffle data and is not an instantaneous fairness guarantee.

## Part A: installing and configuring Hadoop

The supplied installation record follows hadoop_installation.md. Hadoop 3.4.1 was downloaded, extracted, and placed under /opt/hadoop. Environment variables were added to ~/.bashrc. Linux paths are case-sensitive. Use the actual /opt/hadoop and /data/hadoop directories consistently in commands and configuration. Moving files under /opt and configuring protected directories requires appropriate ownership or sudo. The core configuration sets fs.defaultFS to hdfs://localhost:9000. The HDFS configuration uses replication=1 for the single-machine laboratory and separate NameNode/DataNode directories under /data/hadoop. Directory ownership must permit the Hadoop account to access them. JAVA_HOME was configured for the Java installation used by Hadoop. The source describes Java 8 for this stage and Java 11 for a later Spark stage; each component's environment must be checked explicitly rather than assuming one shell value works for all services. After configuring the worker as localhost, the new NameNode was formatted once. The first start-dfs.sh attempt failed because an SSH service was absent. Installing openssh-server addressed that problem. A later passwordless-SSH failure was addressed by generating a local key and adding its public key to authorized_keys. Existing keys should be retained rather than overwritten when adapting the procedure. Once SSH access was configured, start-dfs.sh launched the services and hdfs dfsadmin -report displayed the DataNode status and capacity. 

## HDFS upload test

The report creates sample.txt containing three lines, creates /user/reza/lab/, uploads the file, and lists the directory. The listing confirms that sample.txt appears in HDFS. This is a functional upload/listing test; it does not establish replication across machines because the documented topology has one DataNode.

## Part B: Spark installation and shell tests

The Spark guide initially referred to version 3.5.4. A download attempt failed, and the report switched to Spark 3.5.7. All subsequent extraction paths and example JAR names must therefore use 3.5.7 consistently. SPARK_HOME and PATH were added to ~/.bashrc. start-master.sh launched the master and start-worker.sh spark://localhost:7077 registered the worker. The browser interface on port 8080 showed the deployment. A Scala shell counted the list Hello, World, Hello, Spark. A further Scala test loaded sample.txt from HDFS and applied flatMap, filter, map, reduceByKey, and sorting. The correct sample counts are spark=3, hello=2, hdfs=1. 

## Python environment troubleshooting

The initial global pip installation was refused because the operating system manages that Python environment. The submission used --break-system-packages after installing setuptools. For a reproducible setup, use an isolated environment and a matching PySpark version instead of modifying the managed interpreter:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools
python -m pip install pyspark==3.5.7
```

 The reported PySpark shell test parallelizes [1,2,3,4,5], squares each entry, and prints [1,4,9,16,25]. That confirms the basic Python-to-Spark computation for the tested environment.

## Master/worker configuration and final integration

spark-env.sh and workers were created from their templates. The workers file contains localhost because the master and worker share one machine. A hosts entry maps master to the local address. The master UI on port 8080, worker UI on port 8081, and the master's JSON endpoint were used for inspection. spark-defaults.conf sets spark.master to spark://master:7077 and a local driver address. SPARK_LOCAL_IP=127.0.0.1 is appropriate only to this single-host example; a multi-host deployment needs reachable addresses. The final SparkPi example requests 1 GB for the driver and executor and uses 10 samples/partitions as stated by the example invocation. The source reports Pi is roughly 3.1406631406631407. The consistent command for the installed version is:

```bash
spark-submit --class org.apache.spark.examples.SparkPi \
 --master spark://master:7077 --executor-memory 1G \
 --driver-memory 1G \
 /opt/spark/examples/jars/spark-examples_2.12-3.5.7.jar 10
```

The installation evidence supports a working classroom HDFS/Spark integration on the documented host. It does not establish multi-node throughput, fault tolerance, authentication, or production readiness. All deployment screenshots and the full troubleshooting sequence are retained.
