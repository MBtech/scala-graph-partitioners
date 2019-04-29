package graph.partitioning

import scala.collection.mutable
import graph.partitioning.Utils.PartitionID
import graph.partitioning.Utils.VertexId
import redis.clients.jedis.Jedis
import graph.partitioning.Record

import scala.util.Random

class ImbalanacedPartitioner(pid: PartitionID) extends PartitionStrategy{
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    pid
  }

}
class Hash (state: CoordinatedPartitionState) extends PartitionStrategy {
  //  var map : mutable.HashMap[VertexId, Long] = mutable.HashMap()

  val seed = Math.random()
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
//    val recordSrc: Record = state.getRecord(src.toInt)
//    val recordDst: Record = state.getRecord(dst.toInt)
//
//    var sleep = 1024
//    while (!recordSrc.getLock) {
//      try
//        Thread.sleep(sleep)
//      catch {
//        case ex: Exception =>
//
//      }
//      sleep = Math.pow(sleep, 2).toInt
//    }
//    sleep = 2
//    while (!recordDst.getLock) {
//      try
//        Thread.sleep(sleep)
//      catch {
//        case ex: Exception =>
//
//      }
//      sleep = Math.pow(sleep, 2).toInt
//      if (sleep > 10) {
//        recordSrc.releaseLock
//        return getPartition(src, dst, numParts)
//        //TO AVOID DEADLOCK}
//      }
//    }

    val pid = math.abs((src, dst).hashCode()) % numParts
//    println(pid)

//    state.incrementMachineLoad(pid)
//    //UPDATE RECORDS
//    if (state.getClass eq classOf[CoordinatedPartitionState]) {
//      val cord_state: CoordinatedPartitionState = state.asInstanceOf[CoordinatedPartitionState]
//      //NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
//      if (!(recordSrc.hasReplicaInPartition(pid))) {
//        recordSrc.addPartition(pid)
//        cord_state.incrementMachineLoadVertices(pid)
//      }
//      if (!(recordDst.hasReplicaInPartition(pid))) {
//        recordDst.addPartition(pid)
//        cord_state.incrementMachineLoadVertices(pid)
//      }
//    }
//    else { //1-UPDATE RECORDS
//      if (!(recordSrc.hasReplicaInPartition(pid))) {
//        recordSrc.addPartition(pid)
//      }
//      if (!(recordDst.hasReplicaInPartition(pid))) {
//        recordDst.addPartition(pid)
//      }
//    }
//
//    //3-UPDATE DEGREES
//    recordSrc.incrementDegree
//    recordDst.incrementDegree
//
//    //*** RELEASE LOCK
//    recordSrc.releaseLock
//    recordDst.releaseLock

    pid
  }

}

class DBH (state: CoordinatedPartitionState) extends PartitionStrategy {
  //  var map : mutable.HashMap[VertexId, Long] = mutable.HashMap()

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    val recordSrc: Record = state.getRecord(src.toInt)
    val recordDst: Record = state.getRecord(dst.toInt)

    var sleep = 1024
    while (!recordSrc.getLock) {
      try
        Thread.sleep(sleep)
      catch {
        case ex: Exception =>

      }
      sleep = Math.pow(sleep, 2).toInt
    }
    sleep = 2
    while (!recordDst.getLock) {
      try
        Thread.sleep(sleep)
      catch {
        case ex: Exception =>

      }
      sleep = Math.pow(sleep, 2).toInt
      if (sleep > 10) {
        recordSrc.releaseLock
        return getPartition(src, dst, numParts)
        //TO AVOID DEADLOCK}
      }
    }


      val dSrc = recordSrc.getDegree
      val dDst = recordDst.getDegree

      var pid = 0
      if (dSrc < dDst) pid = math.abs(src.hashCode()) % numParts else pid = math.abs(dst.hashCode()) % numParts

      state.incrementMachineLoad(pid)
    //UPDATE RECORDS
    if (state.getClass eq classOf[CoordinatedPartitionState]) {
      val cord_state: CoordinatedPartitionState = state.asInstanceOf[CoordinatedPartitionState]
      //NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
      if (!(recordSrc.hasReplicaInPartition(pid))) {
        recordSrc.addPartition(pid)
        cord_state.incrementMachineLoadVertices(pid)
      }
      if (!(recordDst.hasReplicaInPartition(pid))) {
        recordDst.addPartition(pid)
        cord_state.incrementMachineLoadVertices(pid)
      }
    }
    else { //1-UPDATE RECORDS
      if (!(recordSrc.hasReplicaInPartition(pid))) {
        recordSrc.addPartition(pid)
      }
      if (!(recordDst.hasReplicaInPartition(pid))) {
        recordDst.addPartition(pid)
      }
    }

    //3-UPDATE DEGREES
    recordSrc.incrementDegree
    recordDst.incrementDegree

    //*** RELEASE LOCK
    recordSrc.releaseLock
    recordDst.releaseLock

    pid
    }

}

class HDRF(lambda: Float, numParts: Int, state: CoordinatedPartitionState) extends PartitionStrategy{
  val epsilon = 1


  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    val recordSrc: Record = state.getRecord(src.toInt)
    val recordDst: Record = state.getRecord(dst.toInt)

    // Get the locks otherwise, sleepy time
    var sleep = 2
    while (!recordSrc.getLock) {
      try
        Thread.sleep(sleep)
      catch {
        case ex: Exception =>

      }
      sleep = Math.pow(sleep, 2).toInt
    }
    sleep = 2
    while (!recordDst.getLock) {
      try
        Thread.sleep(sleep)
      catch {
        case ex: Exception =>

      }
      sleep = Math.pow(sleep, 2).toInt
      if (sleep > 1024) {
        recordSrc.releaseLock
        return getPartition(src, dst, numParts)
        //TO AVOID DEADLOCK}
      }
    }
    // LOCK TAKEN


    val minLoad = state.getMinLoad
    val maxLoad = state.getMaxLoad

    var maxScore = 0.0

    var candidates : List[PartitionID] = List()
    val dSrc = recordSrc.getDegree + 1
    val dDst = recordDst.getDegree + 1

    val dSum = dSrc + dDst

    //    println("Membership information: " + membership.toList.toString())
    //    println("Load information: " + load.toList.toString())
    for (pid <- 0 to numParts-1){

        var fu = 0.0
        var fv = 0.0
        if(recordSrc.hasReplicaInPartition(pid)){
          fu = dSrc
          fu /= dSum
          fu = 1 + (1 - fu)
        }

        if(recordDst.hasReplicaInPartition(pid)){
          fv = dDst
          fv /= dSum
          fv = 1 + (1 - fv)
        }

        val l = state.getMachineLoad(pid)
        var bal = (maxLoad - l).toDouble
        //        println(s"Load of pid ${pid} is ${l}")
        //        println(s"Balance score: ${bal}")
        bal /= (epsilon + maxLoad - minLoad).toDouble
        if (bal < 0.0){
          bal = 0.0
        }

        val score = fu + fv + lambda * bal
        //        println(s"Score for partition: ${pid} is ${score}")
        //        setScore(score)

        if(score > maxScore){
          maxScore = score
          candidates = List()
          candidates = candidates:+pid
        }else if (score == maxScore){
          candidates = candidates:+pid
        }

    }
    val rand = new Random()
//    println(candidates.size)
    val pid = candidates(rand.nextInt(candidates.size))
    //    println("\n" + "Candidates are: " + candidates.toList.toString())
    //
    // Instead of random selection, let's select a deterministic partition. It will make our life easier
//    val pid = candidates(0)

    state.incrementMachineLoad(pid)
    //UPDATE RECORDS
    if (state.getClass eq classOf[CoordinatedPartitionState]) {
      val cord_state: CoordinatedPartitionState = state.asInstanceOf[CoordinatedPartitionState]
      //NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
      if (!(recordSrc.hasReplicaInPartition(pid))) {
        recordSrc.addPartition(pid)
        cord_state.incrementMachineLoadVertices(pid)
      }
      if (!(recordDst.hasReplicaInPartition(pid))) {
        recordDst.addPartition(pid)
        cord_state.incrementMachineLoadVertices(pid)
      }
    }
    else { //1-UPDATE RECORDS
      if (!(recordSrc.hasReplicaInPartition(pid))) {
        recordSrc.addPartition(pid)
      }
      if (!(recordDst.hasReplicaInPartition(pid))) {
        recordDst.addPartition(pid)
      }
    }

    //3-UPDATE DEGREES
    recordSrc.incrementDegree
    recordDst.incrementDegree

    //*** RELEASE LOCK
    recordSrc.releaseLock
    recordDst.releaseLock

    pid
  }

}

class BalancedHDRF(lambda: Float, numParts: Int, totalEdges: Long, loadFactor: Double = 2.0, state: CoordinatedPartitionState)
  extends PartitionStrategy{
  val epsilon = 1
  val maxTolerableLoad = loadFactor*(totalEdges/numParts)


    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val recordSrc: Record = state.getRecord(src.toInt)
      val recordDst: Record = state.getRecord(dst.toInt)

      // Get the locks otherwise, sleepy time
      var sleep = 2
      while (!recordSrc.getLock) {
        try
          Thread.sleep(sleep)
        catch {
          case ex: Exception =>

        }
        sleep = Math.pow(sleep, 2).toInt
      }
      sleep = 2
      while (!recordDst.getLock) {
        try
          Thread.sleep(sleep)
        catch {
          case ex: Exception =>

        }
        sleep = Math.pow(sleep, 2).toInt
        if (sleep > 1024) {
          recordSrc.releaseLock
          return getPartition(src, dst, numParts)
          //TO AVOID DEADLOCK}
        }
      }
      // LOCK TAKEN


      val minLoad = state.getMinLoad
      val maxLoad = state.getMaxLoad

      var maxScore = 0.0

      var candidates : List[PartitionID] = List()
      val dSrc = recordSrc.getDegree + 1
      val dDst = recordDst.getDegree + 1

      val dSum = dSrc + dDst

      //    println("Membership information: " + membership.toList.toString())
      //    println("Load information: " + load.toList.toString())
      for (pid <- 0 to numParts-1){

        var fu = 0.0
        var fv = 0.0
        if(recordSrc.hasReplicaInPartition(pid)){
          fu = dSrc
          fu /= dSum
          fu = 1 + (1 - fu)
        }

        if(recordDst.hasReplicaInPartition(pid)){
          fv = dDst
          fv /= dSum
          fv = 1 + (1 - fv)
        }

        val l = state.getMachineLoad(pid)
        var bal = (maxLoad - l).toDouble
        //        println(s"Load of pid ${pid} is ${l}")
        //        println(s"Balance score: ${bal}")
        bal /= (epsilon + maxLoad - minLoad).toDouble
        if (bal < 0.0){
          bal = 0.0
        }

        val score = fu + fv + lambda * bal
        //        println(s"Score for partition: ${pid} is ${score}")
        //        setScore(score)

        if(score > maxScore){
          maxScore = score
          candidates = List()
          candidates = candidates:+pid
        }else if (score == maxScore){
          candidates = candidates:+pid
        }

      }
    // Instead of random selection, let's select a deterministic partition. It will make our life easier

        val rand = new Random()
    //    println(candidates.size)
        var pid  = candidates(rand.nextInt(candidates.size))
    //    println("\n" + "Candidates are: " + candidates.toList.toString())
    //
//    var pid = candidates(0)

    var l = state.getMachineLoad(pid)
    while(l >= maxTolerableLoad){
      pid += 1
      if(pid >= numParts){
        pid = 0
      }
      l = state.getMachineLoad(pid)
    }

    pid
  }


}

class HDRFRedis(lambda: Float, numPartitions: Int, host: String="localhost") extends PartitionStrategy{
  @transient lazy val jedis = new Jedis(host)
  var scores: Array[Double] = Array()
  var degrees : mutable.HashMap[VertexId, Long] = mutable.HashMap()
  val epsilon = 1
  var membership: mutable.HashMap[Int, Set[VertexId]] = {
    var tmp : mutable.HashMap[Int, Set[VertexId]] = mutable.HashMap()
    for(i <- 0 to numPartitions-1){
      tmp.put(i, Set[VertexId]())
    }
    tmp
  }

  var load: mutable.HashMap[Int, Long] = {
    var tmp : mutable.HashMap[Int, Long] = mutable.HashMap()
    for(i <- 0 to numPartitions-1){
      jedis.rpush("load", 0L.toString)
      tmp.put(i, 0L)
    }
    tmp
  }



  def setScore(score: Double): Unit ={
    scores = scores :+ score
  }


  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    //    jedis = new Jedis(host)
    val minLoad = getMinLoad()
    val maxLoad = getMaxLoad()

    var maxScore = 0.0

    var candidates : List[PartitionID] = List()
    val dSrc = getDegree(src)
    val dDst = getDegree(dst)

    val dSum = dSrc + dDst



    //    println("Membership information: " + membership.toList.toString())
    //    println("Load information: " + load.toList.toString())
    membership.foreach{
      case (pid, vertices) => {

        val pidString = pid.toString
        var fu = 0.0
        var fv = 0.0
        if(jedis.sismember("membership"+ pidString, src.toString)){
          fu = dSrc
          fu /= dSum
          fu = 1 + (1 - fu)
        }

        if(jedis.sismember("membership"+ pidString, dst.toString)){
          fv = dDst
          fv /= dSum
          fv = 1 + (1 - fv)
        }


        val l = jedis.lindex("load", pid.toLong).toInt
        //        val l = jedis.get("load"+pid.toString).toInt
        var bal = (maxLoad - l).toDouble
        //        println(s"Load of pid ${pid} is ${l}")
        //        println(s"Balance score: ${bal}")
        bal /= (epsilon + maxLoad - minLoad).toDouble
        if (bal < 0.0){
          bal = 0.0
        }

        val score = fu + fv + lambda * bal
        //        println(s"Score for partition: ${pid} is ${score}")
        //        setScore(score)

        if(score > maxScore){
          maxScore = score
          candidates = List()
          candidates = candidates:+pid
        }else if (score == maxScore){
          candidates = candidates:+pid
        }

      }
    }
    // Instead of random selection, let's select a deterministic partition. It will make our life easier

    //    val rand = new Random()
    //    println(candidates.size)
    //    candidates(rand.nextInt(candidates.size))
    //    println("\n" + "Candidates are: " + candidates.toList.toString())
    //
    val pid = candidates(0)

    jedis.lset("load", pid.toLong, (jedis.lindex("load", pid.toLong).toInt+1).toString)
    //    if(!members.contains(src)){
    //      load.put(pid, load(pid)+1)
    //    }
    //    if(!members.contains(dst)){
    //      load.put(pid, load(pid)+1)
    //    }
    jedis.sadd("membership"+pid.toString, src.toString)
    jedis.sadd("membership"+pid.toString, dst.toString)
    //    membership.put(pid, membership(pid)+src+dst)
    pid
  }

  def getMinLoad(): Long = {
    var ret = 0L
    //    jedis = new Jedis(host)
    val l = jedis.lrange("load", 0L, numPartitions.toLong-1L).toArray().map(_.toString.toInt)
    if(l.size > 0) {
      ret = l.min
    }
    ret
  }

  def getMaxLoad(): Long = {
    var ret = 0L
    //    jedis = new Jedis(host)
    val l = jedis.lrange("load", 0L, numPartitions.toLong-1L).toArray().map(_.toString.toInt)
    if(l.size>0){
      ret = l.max
    }
    ret
  }

  def getDegree(vertex: VertexId): Long = {
    var ret = 0L
    var d = jedis.get("degree"+vertex.toString)
    if (d == null){
      jedis.set("degree"+vertex, 1.toString)
      return 1.toLong
    }else{
      d = (d.toLong + 1).toString
      jedis.set("degree"+vertex.toString, d)
      return d.toLong
    }
  }

}

class AdaptiveHDRF(lambda: Float, numParts: Int, totalEdges: Long, loadFactor: Double = 2.0) extends PartitionStrategy{
  var scores: Array[Double] = Array()
  var degrees : mutable.HashMap[VertexId, Long] = mutable.HashMap()
  val epsilon = 1
  val maxTolerableLoad = loadFactor*(totalEdges/numParts)
  val processedEdges = 0L
  var membership: mutable.HashMap[Int, Set[VertexId]] = {
    var tmp : mutable.HashMap[Int, Set[VertexId]] = mutable.HashMap()
    for(i <- 0 to numParts-1){
      tmp.put(i, Set[VertexId]())
    }
    tmp
  }

  var load: mutable.HashMap[Int, Long] = {
    var tmp : mutable.HashMap[Int, Long] = mutable.HashMap()
    for(i <- 0 to numParts-1){
      tmp.put(i, 0L)
    }
    tmp
  }

  def setScore(score: Double): Unit ={
    scores = scores :+ score
  }


  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    val minLoad = getMinLoad()
    val maxLoad = getMaxLoad()

    var maxScore = 0.0

    var candidates : List[PartitionID] = List()
    val dSrc = getDegree(src)
    val dDst = getDegree(dst)

    val dSum = dSrc + dDst

    //    println(s"Source ID: ${src} and Destination ID: ${dst}")
    //    println("Membership information: " + membership.toList.toString())
    //    println("Load information: " + load.toList.toString())
    membership.foreach{
      case (pid, vertices) => {
        var fu = 0.0
        var fv = 0.0
        if(vertices.contains(src)){
          fu = dSrc
          fu /= dSum
          fu = 1 + (1 - fu)
        }

        if(vertices.contains(dst)){
          fv = dDst
          fv /= dSum
          fv = 1 + (1 - fv)
        }

        val l = load(pid)
        var bal = (maxLoad - l).toDouble
        //        println(s"Load of pid ${pid} is ${l}")
        //        println(s"Balance score: ${bal}")
        bal /= (epsilon + maxLoad - minLoad).toDouble
        if (bal < 0.0){
          bal = 0.0
        }
        var score = 0.0
        score = fu + fv + lambda * bal
        // If load if more than max tolerable load then terminate
        //        if(l >= maxTolerableLoad){
        //          score = 0.0
        //        }
        //        println(s"Score for partition: ${pid} is ${score}")
        //        setScore(score)

        if(score > maxScore){
          maxScore = score
          candidates = List()
          candidates = candidates:+pid
        }else if (score == maxScore){
          candidates = candidates:+pid
        }

      }
    }
    // Instead of random selection, let's select a deterministic partition. It will make our life easier

    //    val rand = new Random()
    //    println(candidates.size)
    //    candidates(rand.nextInt(candidates.size))
    //    println("\n" + "Candidates are: " + candidates.toList.toString())
    //
    var pid = candidates(0)

    var l = load(pid)
    while(l >= maxTolerableLoad){
      pid += 1
      if(pid >= numParts){
        pid = 0
      }
      l = load(pid)
    }

    var members = membership(pid)

    load.put(pid, load(pid)+1)
    //    if(!members.contains(src)){
    //      load.put(pid, load(pid)+1)
    //    }
    //    if(!members.contains(dst)){
    //      load.put(pid, load(pid)+1)
    //    }
    membership.put(pid, membership(pid)+src+dst)

    pid
  }

  def getMinLoad(): Long = {
    var ret = 0L
    if(load.size > 0){
      ret = load.minBy(_._2)._2
    }
    ret
  }

  def getMaxLoad(): Long = {
    var ret = 0L
    if(load.size>0){
      ret = load.maxBy(_._2)._2
    }
    ret
  }

  def getDegree(vertex: VertexId): Long = {
    var ret = 0L
    if (degrees.contains(vertex)){
      degrees.put(vertex,degrees(vertex)+1)
      ret = degrees(vertex)
    }else{
      degrees.put(vertex, 1L)
      ret = degrees(vertex)
    }
    ret
  }

}
