package graph.partitioning

import scala.collection.SortedSet

trait PartitionState {
  def getRecord(x: Int): Record
  def getMachineLoad(m: Int): Int
  def incrementMachineLoad(m: Int): Unit
  def getMinLoad: Int
  def getMaxLoad: Int
  def getMachinesLoads: Array[Int]
  def getTotalReplicas: Int
  def getNumVertices: Int
  def getVertexIds: SortedSet[Int]
}