package bda.spark.topic.glint

import glint.models.client.{BigMatrix, BigVector}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Roger on 17/3/11.
  */
object Glint {

  var semaphore = 1
  val max_buff_size = 32000

  def pullData(cols: Array[Long],
               vector: BigVector[Double]): Array[Double]= {

    val result_nt = vector.pull(cols)

    val global_nt: Array[Double] = Await.result(result_nt, Duration.Inf)
    return global_nt
  }

  def pullData(row: Long, matrix: BigMatrix[Double]): Array[Double] = {
    val col = matrix.cols
    val result = matrix.pull(Array.fill[Long](col)(row), (0 until col).toArray)
    val ret = Await.result(result, Duration.Inf)
    ret
  }

  def pushData(vector: BigVector[Double],
               delta: Iterable[(Long, Double)]): Unit = {
    val result = vector.push(delta.map(_._1).toArray, delta.map(_._2).toArray)
    Await.result(result, Duration.Inf)
  }

  /**
    * pull data from ps matrix
    * @param matrix ps matrix
    * @param rows rows to pull
    * @return
    */
  def pullData(rows: Array[Long],
               matrix: BigMatrix[Double]
              ): Map[Long,Array[Double]] = {

    val lock = new java.util.concurrent.Semaphore(semaphore) // maximum of 16 open requests
    val K = matrix.cols
    val batch_rows = max_buff_size / K
    val push_time = rows.size / batch_rows
    println(s"Pull Time: " + push_time)
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

  def pushData(row: Long, vec: Array[Double], matrix: BigMatrix[Double]): Unit = {
    val col = matrix.cols
    val rows = Array.fill[Long](col)(row)
    val cols = (0 until col).toArray

    val result = matrix.push(rows, cols, vec)
    Await.result(result, Duration.Inf)
  }

  def pushData(deltaMat: Map[Long, Array[Double]], matrix: BigMatrix[Double]): Unit = {
    val delta = deltaMat.flatMap{
      case (wid, vec) =>
        vec.zipWithIndex.filter(_._1 > 0).map{
          case (value, index)=>
            (wid, index, value)
        }
    }

    pushData(delta, matrix)
  }

  def pushData(deltaVec: Array[Double], vector: BigVector[Double]): Unit ={
    val col = vector.size
    val result = vector.push((0L until col.toLong).toArray, deltaVec)
    Await.result(result, Duration.Inf)
  }
  def pushData(topicCount: Iterable[(Long, Int, Double)],
               matrix: BigMatrix[Double]): Unit = {
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
