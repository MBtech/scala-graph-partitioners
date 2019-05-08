package graph.partitioning

import java.net.URI
import java.util.concurrent.{Executors, Future, FutureTask, TimeUnit}
import java.util.concurrent.locks.Lock

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable
import scala.io.Source

object PerformPartition {
  def readEdges(inputPath: String, masterIP: String): List[Edge] ={
//    val edges: Array[Edge] = Array()
    val separator = "\t"
//    val hdfs = FileSystem.get(new URI("hdfs://"+masterIP+":9000/"), new Configuration())
//    val path = new Path(inputPath)
//    val stream = hdfs.open(path)
//    val source = Source.fromInputStream(stream)
//    val lines = source.getLines().toList
    val lines = Source.fromFile(inputPath).getLines()
    val edges = lines.map(line => {
      new Edge(line.split(separator)(0).toInt, line.split(separator)(1).toInt)
    })
    edges.toList
  }

  def main(args: Array[String]): Unit ={

    val inputPath = args(0)
    val numPartitions = args(1).toInt
    val strategy = args(2)
    val lambda = args(3).toFloat
    val loadFactor = args(4).toDouble
    val masterIP = args(5)
    val processors = if (args.size >= 7) args(6).toInt else Runtime.getRuntime.availableProcessors()
    val path = "hdfs://"+masterIP+":9000/tmp/"

    val state = new CoordinatedPartitionState(numPartitions)
//    val processors = Runtime.getRuntime.availableProcessors()
    println(processors)
    val executor = Executors.newFixedThreadPool(processors)

    val dataset = readEdges(inputPath, masterIP)
    val n = dataset.size
    val subSize = n / processors + 1
//    val dataset = List(new Edge(1, 2), new Edge(1, 3), new Edge(2, 3), new Edge(3, 4))

    var algorithm : PartitionStrategy = new DBH(state)
    if(strategy.equals("hash")){
      algorithm = new Hash(state)
    }else if (strategy.equals("dbh")){
      algorithm = new DBH(state)
    }else if (strategy.equals("hdrf")){
      algorithm = new HDRF(lambda, numPartitions, state)
    }else if (strategy.equals("bhdrf")){
      algorithm = new BalancedHDRF(lambda, numPartitions, n, loadFactor, state)
    }
    var t = 0
    val writer = new HDFSWriter(masterIP, numPartitions, path)
    val futureSet = new mutable.HashSet[Future[Array[mutable.Set[Edge]]]]
    var pid2Edges : Array[Array[Edge]] =  new Array(numPartitions)
    for (i<- 0 to numPartitions-1) {
      pid2Edges(i) = Array[Edge]()
    }

    while (t < processors) {
      val iStart = t * subSize
      val iEnd = Math.min((t + 1) * subSize, n)
      if (iEnd >= iStart) {
        val list = dataset.slice(iStart, iEnd)
        val x = new PartitionerThread(list, state, algorithm, numPartitions, t, writer)
        val future = executor.submit(x)
        futureSet.add(future)
      }
      t += 1
    }
    try {
      for (future <- futureSet){
        val results = future.get(60, TimeUnit.HOURS)
        for (i<- 0 to numPartitions-1){
          pid2Edges(i) = pid2Edges(i) ++ results(i)
        }

      }
      executor.shutdown()
//      executor.awaitTermination(60, TimeUnit.DAYS)
    } catch {
      case ex: InterruptedException =>
        System.out.println("InterruptedException " + ex)
        ex.printStackTrace()
    }

    println("Edge Distribution")
    pid2Edges.foreach(arr => {
      print(arr.length + " ")
    })
    println()

    println("Vertex Distribution")
    pid2Edges.foreach(arr => {
      var v : mutable.Set[Long] = mutable.Set()
      arr.foreach(e => {
        v.add(e.getSrc())
        v.add(e.getDst())
      })
      print(v.size+ " ")
    })
    println()

    println("Writing to HDFS now")
    for (i <- 0 to numPartitions-1){
      println("Writing to partition: "+ i.toString)
      writer.write(pid2Edges(i).toArray, i)
    }

//    System.exit(0)
  }

}
