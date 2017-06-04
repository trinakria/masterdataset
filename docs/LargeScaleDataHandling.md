Think back on the implementations you made in the first section (1.1, 1.2, 1.3).

* Would these change once the data size gets larger? If so, how?

The implementation does not take into account available space on disk. With the data size getting larger the generation
 process could fail since no space is available. 
 The writing process is sequential. This could not meet eventual performance constraints in case of a large dataset.

* How would you handle a data size that becomes too large to hold on a single machine on local
  disks?
  
  You will need to use a distributed file system which allows storage of large files across multiple machines, provides
  high availability and fault tolerance.
  
* Would there be libraries or tools that you could use to help out? If so, which ones and how could
  they help?
  
  The Hadoop framework and the underlying HDFS file system could be used in this case. Hadoop provides a JAVA/REST
  API to allow multiple clients to interact with the HDFS file system and the map/reduce programming model for data
  processing.