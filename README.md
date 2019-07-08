## Graph Partitioners
The repository contains implementations of different graph partitioning algorithms in scala
as well as models to get average-case value for replication factor.

### How to run
sbt "runMain graph.partitioning.PerformPartition <inputPath> <numPartitions> <strategy> \
        <lambda> <load factor> <masterIP> <number of processors>"

Strategies available are: hash, dbh, hdrf, bdhrf