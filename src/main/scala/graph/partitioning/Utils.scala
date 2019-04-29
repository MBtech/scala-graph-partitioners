package graph.partitioning

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

object Utils {
  type PartitionID = Int
  type VertexId = Long

}
class Edge(src: Long, dst: Long){

  def getSrc(): Long ={
    src
  }

  def getDst(): Long ={
    dst
  }


}

class HDFSWriter(masterIP: String, numPartitions: Int, path: String){
  val conf = new Configuration()
  conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
  conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
  conf.set("fs.defaultFS", "hdfs://"+masterIP+":9000/")
  val fs= FileSystem.get(conf)
  var output = Array[FSDataOutputStream]()
  for(i<- 0 to numPartitions-1){
    val filename = "part-"+"%05d".format(i)
    output = output :+ fs.create(new Path(path+filename))
  }



  //conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020")

  def write(edges: Array[Edge], pid: Int): Unit ={
    this.synchronized({
//      val filename = "part-"+"%05d".format(pid)
//      val append = fs.append(new Path(path+filename))
      val writer = new PrintWriter(output(pid))
      for (edge <- edges){
        writer.write(edge.getSrc().toString+"\t"+edge.getDst().toString)
      }

      writer.close()
    })
//    val filename = "part-"+"%05d".format(pid)
//    val output = fs.append(new Path(path+filename))

  }
}

