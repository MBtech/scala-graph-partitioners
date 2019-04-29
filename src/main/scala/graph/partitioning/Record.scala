package graph.partitioning

trait Record {
  def getPartitions: Iterator[Byte]
  def addPartition(m: Int): Unit
  def hasReplicaInPartition(m: Int): Boolean
  def getLock: Boolean
  def releaseLock: Boolean
  def getReplicas: Int
  def getDegree: Int
  def incrementDegree(): Unit
}