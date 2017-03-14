package bda.spark.topic.stream

import bda.spark.topic.core._
import bda.spark.topic.utils.Timer
import breeze.linalg.DenseVector
import com.codahale.metrics.Slf4jReporter.LoggingLevel
import org.apache.spark.rdd.RDD
import glint._
import glint.models.client.buffered.BufferedBigMatrix
import glint.models.client.{BigMatrix, BigVector}
import org.apache.spark.Logging

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.util.{Random, Try}

/**
  * Implement according to Alex Smola 2010
  */
class DistributedParallelLDA(
                             val model: PsLdaModel,
                             val max_buff_size: Int = 12800,
                             val semaphore: Int = 16)
  extends LdaTrainer(model.K, model.alpha, model.beta) with Serializable with Logging{

  @transient val client = Client()

  def train(input: RDD[Example], test: RDD[Example], iteration: Int): Unit = {
    var global_corpus = initTopicAssignment(input)
    doTopicStatistic(global_corpus)


    val timer = new Timer()
    println(s"Start trainning, time = ${timer.getReadableRunnningTime()}")

    global_corpus.foreachPartition{
      docsInPartition =>
        var local_corpus = docsInPartition.toArray
        Range(0, iteration).foreach {
          iter =>
            local_corpus = doGibbsSampling(local_corpus, this.model.betaStatsMatrix, this.model.topicStatsVector)
            println(s"iteration: ${iter} time = ${timer.getReadableRunnningTime()}")
        }
    }

    client.stop()
  }

  protected def initTopicAssignment(example: Example): IdDoc= {
    val docInstance = example.instance.asInstanceOf[IdDocInstance]

    docInstance.tokens.map {
      case (term, topic) =>
        (term, Math.abs(Random.nextInt() % K))
    }
  }

  def initTopicAssignment(input: RDD[Example]): RDD[IdDoc] = {
    input.map{
      exmaple =>
        initTopicAssignment(exmaple)
    }
  }

  def doTopicStatistic(docs: RDD[IdDoc]) {
    println("Start topic statistic")

    import scala.concurrent.ExecutionContext.Implicits.global
    client.serverList().map{
      case value =>
        println(value.size)
    }

    println("BigMatrix and BigVector was created")
    //统计(词频,主题)共现矩阵
    docs.foreachPartition{
      docsInPartition =>

        println("Start each partition")
        val termsInPartition = docsInPartition.toSeq.flatMap(_.toSeq)

        val termTopicStats = termsInPartition.groupBy(x=>x).map {
          entry =>
            val (term, topic) = entry._1
            val cnt = entry._2.size
            (term, topic, cnt)
        }

        val topicColIndices = new ArrayBuffer[Long]()
        val topicValues = new ArrayBuffer[Double]()
        termTopicStats.groupBy(_._2).foreach{
          entry =>
            val topic = entry._1
            val cnt = entry._2.map(_._3).sum
            topicColIndices += topic
            topicValues += cnt.toDouble
        }

        println("[INFO] push data to parameter server")
        pushData(model.betaStatsMatrix, termTopicStats)

        Await.result(
          model.topicStatsVector.push(topicColIndices.toArray, topicValues.toArray),
          Duration.Inf
        )
    }
  }

  def doGibbsSampling(docs: Array[IdDoc],
                      termTopicStatsMat: BigMatrix[Double],
                      topicStatsVec: BigVector[Double]): Array[IdDoc] = {

    val termIds = docs.flatMap(_.map(_._1)).distinct
    val (global_ntw, global_nt) = syncGlobalParamter(termIds,
      termTopicStatsMat, topicStatsVec)

    val delta_ntw = global_ntw.map {
      case (wid, vec) =>
        (wid, Array.fill[Int](K)(0))
    }

    val delta_nt = Array.fill[Int](K)(0)

    val newDocArray = mutable.ArrayBuffer[IdDoc]()
    docs.foreach {
      doc =>
        val docTopicStats = Array.fill[Double](K)(0)
        doc.groupBy(_._2).foreach {
          entry =>
            val topic = entry._1
            val cnt = entry._2.size
            docTopicStats(topic) += cnt
        }

        val newDoc = mutable.ArrayBuffer[(Long, Int)]()

        doc.foreach {
          case (wid, topic) =>
            docTopicStats(topic) -= 1
            global_ntw(wid)(topic) -= 1
            global_nt(topic) -= 1
            delta_ntw(wid)(topic) -= 1
            delta_nt(topic) -= 1

            val ntopic: Int = CollapsedGibbsSampler.sample(
              docTopicStats,
              global_ntw(wid),
              global_nt,
              alpha, beta, model.V
            )

            docTopicStats(ntopic) += 1
            global_ntw(wid)(ntopic) += 1
            global_nt(ntopic) += 1
            delta_ntw(wid)(ntopic) += 1
            delta_nt(ntopic) += 1

            newDoc += ((wid, ntopic))
        }
        newDocArray += newDoc
    }

    val indices = delta_ntw.toArray.flatMap{
      case (wid, vec) =>
        vec.zipWithIndex.filter{
          case (value, index) =>
            value != 0
        }.map( x => (wid, x._2, x._1))
    }

    println("[INFO] push data to parameter server")
    pushData(termTopicStatsMat, indices)

    Await.result(
      topicStatsVec.push((0L until K.toLong).toArray, delta_nt.map(_.toDouble)),
      Duration.Inf
    )
    newDocArray.toArray
  }

  private def syncGlobalParamter(termIds: Array[Long],
                                 globalTermTopicMatrix: BigMatrix[Double],
                                 globalTopicMatrix: BigVector[Double]):
  (Map[Long, Array[Double]], Array[Double]) = {

    println("[INFO] pull data from parameter server")
    val global_ntw = pullData(globalTermTopicMatrix, termIds)

    val result_nt = globalTopicMatrix.pull((0L until K.toLong).toArray)

    val global_nt: Array[Double] = Await.result(result_nt, Duration.Inf)

    (global_ntw, global_nt)
  }


  /**
    * pull data from ps matrix
    * @param matrix ps matrix
    * @param rows rows to pull
    * @return
    */
  private def pullData(matrix: BigMatrix[Double],
                       rows: Array[Long]): Map[Long,Array[Double]] = {

    val lock = new java.util.concurrent.Semaphore(semaphore) // maximum of 16 open requests
    assert(K < max_buff_size)

    val batch_rows = max_buff_size / K
    val push_time = rows.size / batch_rows
    val ret = Range(0, push_time + 1).map {
      t =>
        //use parameter server
        val subRows = rows.slice(t * batch_rows, (t + 1) * batch_rows)
        val rowIndices = subRows.flatMap(Array.fill[Long](K)(_))
        val colIndices = subRows.flatMap(x => (0 until K).toList)
        lock.acquire()
        val result = matrix.pull(rowIndices, colIndices)
        result.onComplete{case _ => lock.release()}
        val data: Array[Double] = Await.result(
          result,
          Duration.Inf
        )
        (0 until subRows.size).map {
          row =>
            val ntw = data.slice(row * K, row * K + K)
            (subRows(row), ntw)
        }.toMap

    }.reduce( _ ++ _ )

    lock.acquire(semaphore)
    lock.release(semaphore)
    ret
  }

  private def pushData(matrix: BigMatrix[Double],
                       topicCount: Iterable[(Long, Int, Int)]
                       ): Unit = {
    val lock = new java.util.concurrent.Semaphore(semaphore)
    val rowIndices = new ArrayBuffer[Long]()
    val colIndices = new ArrayBuffer[Int]()
    val values = new ArrayBuffer[Double]()

    topicCount.foreach {
      case (wid, topic, cnt) =>
        rowIndices += wid
        colIndices += topic
        values += cnt
    }

    assert(colIndices.size == rowIndices.size)
    val tot_size = colIndices.size
    val push_time = rowIndices.size / max_buff_size
    println(s"Push time: $push_time")
    Range(0, push_time + 1).foreach {
      t =>
        lock.acquire()
        val result =
          matrix.push(
            rowIndices.slice(t * max_buff_size, (t + 1) * max_buff_size).toArray,
            colIndices.slice(t * max_buff_size, (t + 1) * max_buff_size).toArray,
            values.slice(t * max_buff_size, (t + 1) * max_buff_size).toArray)

        result.onComplete{case _ => lock.release()}
    }

    lock.acquire(semaphore)
    lock.release(semaphore)
  }
}
