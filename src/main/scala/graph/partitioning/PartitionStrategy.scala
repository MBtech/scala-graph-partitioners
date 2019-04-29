package graph.partitioning

import graph.partitioning.Utils.{PartitionID, VertexId}

trait PartitionStrategy extends Serializable {
  /** Returns the partition number for a given edge. */
  def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID
}
