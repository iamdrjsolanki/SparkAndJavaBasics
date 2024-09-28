Spark Overview
- Spark is a flexible alternative for MapReduce, while MapReduce only works with data stored in HDFS format only, Spark can transform data which is stored in HDFS, AWS S3, MongoDB, HBase, etc.
- Spark can process 100x faster than MapReduce, as MapReduce writes each data to disk after map & reduce operation, whereas Spark keeps the data in memory and only writes on disk if the memory is full.
- Spark creates a cluster where each Worker Node processes the data
- Spark Context, also known as Driver Program, is responsible to manage the cluster & worker nodes

RDD
- Resilient: Fault Tolerant and is capable of rebuilding data on failure
- Distributed: Distributed data among different nodes
- Dataset: Collection of partitioned data with values
- RDD is replicated on each node and can be rebuilt from any node in case of failure
- Fundamental Data Structure of Spark, and is immutable collection
- Each Dataset is logically partitioned to be computed on different nodes of the cluster
- In memory computations
- Lazily evaluated - the transformations are applied to RDD but the output is not generated instead it is logged
- Parallel operation - since it is partitioned among cluster nodes

Spark, on local, always runs on stand alone mode, which schedules tasks in FIFO algorithm.

Spark has a default port as 4040.

Create a quick  running spark application?
- Create SparkSession object using SparkSession builder by setting appName() & master()
- master() takes value as
- local - executes all tasks on single core
- local[*] - executes all tasks on all available cores on the system, we can also specify integer in place of “*”
- Create JavaSparkContext object using the sparkSession object
- Create JavaRDD object by parallelizing the collection data
- Create action on the JavaRDD object to execute the tasks, example: reduce(), count(), collect(), etc.
