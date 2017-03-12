package bda.spark.topic.stream

import bda.spark.topic.core.{CollapsedGibbsSampler, IdDoc, IdDocInstance, PsStreamLdaModel}
import bda.spark.topic.glint.Glint
import bda.spark.topic.stream.io.StreamReader
import breeze.linalg.DenseVector
import glint.models.client.{BigMatrix, BigVector}
import org.apache.spark.streaming.{StreamingContext, Time}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

/**
  * Created by Roger on 17/3/10.
  */
class StreamLdaLearner(val iteration: Int,
                       val model: PsStreamLdaModel) extends Serializable{
  val V = model.V
  val K = model.K

  def update(batch: Seq[IdDocInstance]): Seq[IdDocInstance]= {
   // println("batch: " + batch.take(10).mkString(" "))
    //初始化batch的topic分配
    var docs = initTopicAssignment(batch)
    //统计词batch的n(doc,topic)和n(word,topic)计数
    val (batchNdt, batchNwt) = doTopicStatistic(docs)
    val batchNt = batchNdt.reduce(_ + _)

    val wordIds = docs.flatMap(_.map(_._1)).distinct.toArray
    val priorNwt = Glint.pullData(wordIds, model.priorWordTopicCountMat)
    val priorNt = Glint.pullData((0L until K.toLong).toArray, model.priorTopicCountVec)

    for (i <- 0 until batchNt.length) {
      batchNdt(i) += priorNt(i)
    }
    batchNwt.foreach {
      case (wid, vec) =>
        val pNwt = priorNwt(wid)
        assert(vec.length == pNwt.length)
        for (i <- 0 until vec.length) {
          vec(i) += pNwt(i)
        }
    }
    //println("Initial: " + docs.take(10).mkString(" "))
    //执行iteration次采样使得batch收敛
    for (iter <- 0 until iteration) {
      println(s"Iteration $iter")
      docs = doGibbsSampling(docs, batchNdt, batchNwt, batchNt)
    }

    val wordTopicCounts = docs.flatMap(_.toSeq).groupBy(x => x).map {
      entry =>
        val (term, topic) = entry._1
        val cnt = entry._2.size
        (term, topic, cnt)
    }


    val topicColIndices = new ArrayBuffer[Long]()
    val topicValues = new ArrayBuffer[Double]()
    wordTopicCounts.groupBy(_._2).foreach {
      entry =>
        val topic = entry._1
        val cnt = entry._2.map(_._3).sum
        topicColIndices += topic
        topicValues += cnt.toDouble
    }

    Glint.pushData(model.priorWordTopicCountMat, wordTopicCounts)

    Await.result(
      model.priorTopicCountVec.push(topicColIndices.toArray, topicValues.toArray),
      Duration.Inf
    )

    docs.map(new IdDocInstance(_))
  }


  def doGibbsSampling(docs: Seq[IdDoc],
                      ndt: Array[DenseVector[Double]],
                      nwt: mutable.Map[Long, DenseVector[Double]],
                      nt: DenseVector[Double]): Seq[IdDoc] = {

    val newDocArray = mutable.ArrayBuffer[IdDoc]()
    docs.zipWithIndex.foreach {
      case(doc, docId) =>

        val newDoc = mutable.ArrayBuffer[(Long, Int)]()

        doc.foreach {
          case (wid, topic) =>
            ndt(docId)(topic) -= 1
            nwt(wid)(topic) -= 1
            nt(topic) -= 1

            val ntopic: Int = CollapsedGibbsSampler.sample(
              ndt(docId),
              nwt(wid),
              nt
            )

            ndt(docId)(ntopic) += 1
            nwt(wid)(ntopic) += 1
            nt(ntopic) += 1

            newDoc += ((wid, ntopic))
        }
        newDocArray += newDoc
    }

    newDocArray
  }


  protected def initTopicAssignment(docInstance: IdDocInstance): IdDoc= {

    docInstance.tokens.map {
      case (term, topic) =>
        (term, Math.abs(Random.nextInt() % K))
    }
  }

  def initTopicAssignment(input: Seq[IdDocInstance]): Seq[IdDoc] = {
    input.map{
      exmaple =>
        initTopicAssignment(exmaple)
    }
  }

  private def doTopicStatistic(docs: Seq[IdDoc]):
  (Array[DenseVector[Double]], mutable.HashMap[Long, DenseVector[Double]]) ={

    assert(V != 0)

    //统计local(词频,主题)共现矩阵
    val localWordTopicCounts = new mutable.HashMap[Long, DenseVector[Double]]()
    docs.flatMap(_.toSeq).groupBy(_._1).foreach{
      entry =>
        val wid = entry._1
        val grp = entry._2
        val wordTopicVec = DenseVector.fill[Double](K)(0)
        grp.foreach{
          case (wid, topic) =>
            wordTopicVec(topic) += 1
        }
        localWordTopicCounts += ((wid, wordTopicVec))
    }

    val localDocTopicCounts = docs.map{
      doc =>
        val vec = DenseVector.fill[Double](K)(0)
        doc.foreach{
          case (wid, topic) =>
            vec(topic) += 1
        }
        vec
    }

    (localDocTopicCounts.toArray, localWordTopicCounts)
  }


}
