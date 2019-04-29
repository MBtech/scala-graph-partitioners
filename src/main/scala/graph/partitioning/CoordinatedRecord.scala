package graph.partitioning

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
object CoordinatedRecord{
  def intersection(x: CoordinatedRecord, y: CoordinatedRecord): mutable.TreeSet[Byte] ={
    val result: mutable.TreeSet[Byte] = x.partitions.clone()
    result.intersect(y.partitions)
  }
}

class CoordinatedRecord extends Serializable with Record {

  val partitions : mutable.TreeSet[Byte] = new mutable.TreeSet()
  val lock: AtomicBoolean = new AtomicBoolean(true)
  var degree: Int = 0



  override def getPartitions: Iterator[Byte] = {
    partitions.iterator
  }

  override def addPartition(m: Int) = {
    if(m > 0){
      partitions.add(m.toByte)
    }
  }

  override def hasReplicaInPartition(m: Int): Boolean = {
    partitions.contains(m.toByte)
  }


  override def getLock(): Boolean={
    lock.compareAndSet(true, false)
  }

  def releaseLock: Boolean = {
    lock.compareAndSet(false, true)
  }
  def getReplicas: Int = {
    partitions.size
  }
  def getDegree: Int = {
    degree
  }
  def incrementDegree(): Unit = {
    this.degree += 1
  }


}
