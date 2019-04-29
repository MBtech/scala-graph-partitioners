package graph.partitioning

import java.util.concurrent.atomic.{AtomicInteger, AtomicIntegerArray}

import scala.collection.{SortedSet, mutable}

class CoordinatedPartitionState(numPartitions: Int) extends PartitionState{
  val recordMap: mutable.HashMap[Int, CoordinatedRecord] = new mutable.HashMap()
  val machineLoadEdges : AtomicIntegerArray = new AtomicIntegerArray(numPartitions)
  val machineLoadVertices : AtomicIntegerArray = new AtomicIntegerArray(numPartitions)
  var maxLoad : Int = 0


  def incrementMachineLoadVertices(m: Int ){
    machineLoadVertices.incrementAndGet(m)

  }

  def getMachinesLoadsVertices(): Array[Int] = {
    val load : Array[Int] = new Array[Int](numPartitions)
    for (i <- 0 to numPartitions-1){
      load(i) = machineLoadVertices.get(i)
    }
    load
  }

  override def getRecord(x: Int): CoordinatedRecord = {
    this.synchronized({
      if(!recordMap.contains(x)){
      recordMap.put(x, new CoordinatedRecord)
    }
      return recordMap(x)
    })
  }

  override def getMachineLoad(m: Int): Int = {
    machineLoadEdges.get(m)
  }

  override def incrementMachineLoad(m: Int): Unit = {
    this.synchronized({
      val newVal = machineLoadEdges.incrementAndGet(m)
      if (newVal > maxLoad){
        maxLoad = newVal
      }
    })

  }

  override def getMinLoad: Int = {
    var minLoad = Int.MaxValue
    this.synchronized({
      for( i <- 0 to numPartitions-1){
        val load = machineLoadEdges.get(i)
        if (load < minLoad){
          minLoad = load
        }
      }
      return minLoad
    })
  }

  override def getMaxLoad: Int = {
    maxLoad
  }

  override def getMachinesLoads(): Array[Int] = {
    val load : Array[Int] = new Array[Int](numPartitions)
    for (i <- 0 to numPartitions-1){
      load(i) = machineLoadEdges.get(i)
    }
    load
  }

  override def getTotalReplicas: Int = {
    var replicas = 0
    recordMap.keySet.foreach( x => {
      if (recordMap(x).getReplicas > 0){
        replicas += recordMap(x).getReplicas
      }else{
        replicas +=1
      }
    })
    replicas
  }

  override def getNumVertices: Int = {
    recordMap.size
  }

  override def getVertexIds: SortedSet[Int] = {
//    recordMap.keySet
    return new mutable.TreeSet[Int]()
  }
}