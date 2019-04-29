name := "scala-graph-partitioners"

version := "0.1"

scalaVersion := "2.12.8"

// https://mvnrepository.com/artifact/redis.clients/jedis
libraryDependencies += "redis.clients" % "jedis" % "3.0.0"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.1.1"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.1.1"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs-client
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs-client" % "3.1.1" % "provided"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.1.1"