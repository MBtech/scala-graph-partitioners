package graph.partitioning

import graph.partitioning.PartitionState
import graph.partitioning.PartitionStrategy
import java.util
import java.util.concurrent.Callable

import graph.partitioning.Edge

import scala.collection.mutable
import scala.collection.mutable.HashMap

class PartitionerThread(edgeList: List[Edge], state: PartitionState, algorithm: PartitionStrategy,
                        numPartitions : Int, partitionerID: Int, writer: HDFSWriter) extends Callable[Array[mutable.Set[Edge]]] {

  val pid2Edges : Array[mutable.Set[Edge]] =  new Array(numPartitions)
  for (i <- 0 to numPartitions-1){
    pid2Edges(i) = mutable.Set()
  }
  override def call(): Array[mutable.Set[Edge]] = {

    println("Starting the partitioning process")
    for (t <- edgeList) {
      val pid = algorithm.getPartition(t.getSrc(), t.getDst(), numPartitions)
//      println(pid)
      pid2Edges(pid).add(t)
//      if (pid2Edges.keySet.contains(pid)){
//        pid2Edges.update(pid, pid2Edges(pid):+t)
//      }else{
//        pid2Edges.put(pid, Array(t))
//      }

    }

//    println("Writing to HDFS now")
//    for (pid <- 0 to pid2Edges.size-1){
//      writer.write(pid2Edges(pid).toArray, pid)
//    }
    pid2Edges
  }

}
