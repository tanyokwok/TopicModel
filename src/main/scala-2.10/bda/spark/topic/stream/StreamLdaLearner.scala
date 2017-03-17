package bda.spark.topic.stream

import bda.spark.topic.core.{CollapsedGibbsSampler, IdDoc, IdDocInstance, PsStreamLdaModel}
import bda.spark.topic.glint.Glint
import bda.spark.topic.utils.Timer
import breeze.linalg.DenseVector

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
  val rate = model.rate
  def update(batch: Seq[IdDocInstance], token: String, batchTime: Long, word2id: mutable.Map[String, Long]): Seq[IdDocInstance]= {

    val timer = new Timer()
    val wordCount = batch.flatMap(_.tokens.toSeq).size
    println(s"$token: use time ${timer.getReadableRunnningTime()}")
    println(s"$token: totally $wordCount tokens.")
   // println("batch: " + batch.take(10).mkString(" "))
    //初始化batch的topic分配
    var docs = initTopicAssignment(batch)
    //统计词batch的n(doc,topic)和n(word,topic)计数
    val (batchNdt, batchNwt) = doTopicStatistic(docs)
    val batchNt = batchNdt.reduce(_ + _)

    val wordIds = docs.flatMap(_.map(_._1)).distinct.toArray
    val (priorNt, priorNwt) = syncParameter(wordIds, batchTime, token)


    batchNdt.foreach{
      vec =>
        for( i <- 0 until K) {
          vec(i) += priorNt(i)
        }
    }

    batchNwt.foreach{
      case (wid, vec) =>
        val pNwt = priorNwt(wid)
        for (i <- 0 until K) {
          vec(i) += pNwt(i)
        }
    }


    println(s"$token: start gibbs sampling at ${timer.getReadableRunnningTime()}")
    val topicSamples = new ArrayBuffer[Seq[IdDoc]]()
    //执行iteration次采样使得batch收敛
    for (iter <- 0 until iteration) {
      val newDocs: Seq[IdDoc] = doGibbsSampling(docs, batchNdt, batchNwt, batchNt)
      topicSamples += newDocs
      docs = newDocs
    }

    val stableSamples = topicSamples.reverse.take(5)
    val vocab = model.redisVocab.loadVocab
    val id2word = word2id.map(x => (x._2, x._1))
    //val wordTopicCounts = docs.flatMap(_.toSeq).groupBy(x => x).map {
    //取最后5次采样的平均值
    val wordTopicCounts = stableSamples.flatMap(_.toSeq).
      flatMap(_.toSeq).groupBy(x => x).map {
      entry =>
        val (term, topic) = entry._1
        val cnt = entry._2.size
/*
        if (!vocab.contains(term)) {
          val word = id2word(term)
          println(s"${word} ${word2id(word)} $term")
        } else if (vocab(term) == "太行山") {
          println(s"$term ${cnt.toDouble /5 }")
        }*/
        (term, topic, cnt.toDouble / 5)
    }.toSeq

    val docTopicCounts = topicSamples.map{
      docs =>
        val D = docs.length
        Range(0, D).map{
          d =>
            docs(d).map(x => (d, x._2))
        }
    }.flatMap(_.toSeq).flatMap(_.toSeq).groupBy(x=>x).map{
      entry =>
        val (d, topic) = entry._1
        val cnt = entry._2.size
        (d, topic, cnt.toDouble/ 5)
    }


    println(s"$token: update model at ${timer.getReadableRunnningTime()}")
    update(wordIds, wordTopicCounts, batchTime, token)
    println(
      s"/////////////////////////////////////////////////\n" +
      s"Log(likelihood) w/  prior: ${logLikelihood(priorNwt, priorNt, wordTopicCounts, docTopicCounts.toSeq, docs)}\n" +
      s"Log(likelihood) w/o prior: ${logLikelihood(wordTopicCounts, docTopicCounts.toSeq, docs)}\n" +
      s"/////////////////////////////////////////////////")


    println(s"$token: update success at ${timer.getReadableRunnningTime()}")
    docs.map(new IdDocInstance(_))
  }

  private def syncParameter(wordIds: Array[Long], batchTime: Long, token: String):
  (Array[Double], Map[Long, Array[Double]]) = {
    model.lock.fetchLock(token)
    val (lastBatchTime , lastUpdateTimes) = model.redisVocab.getLastUpdateBatchTime(wordIds)
    val priorNwt = Glint.pullData(wordIds, model.priorWordTopicCountMat)
    val priorNt = Glint.pullData((0L until K.toLong).toArray, model.priorTopicCountVec)

    println("priorNt: " + priorNt.mkString(" "))
    val duration = model.duration
    if (lastBatchTime != -1 && lastBatchTime < batchTime) {
      val batchDelta = (batchTime - lastBatchTime) / duration
      println(s"batchDelta: $batchTime - $lastBatchTime / $duration = " + batchDelta)
      val yeta = math.pow(rate , batchDelta)
      // nt = math.pow(rate, batchDelta) * nt
      val deltaNt = priorNt.map(- _ * (1 - yeta))
      Range(0, priorNt.length).foreach(k=> priorNt(k) *= yeta)
      Glint.pushData(deltaNt, model.priorTopicCountVec)
    }
    println("priorNt: " + priorNt.mkString(" "))
    val tempNt = Glint.pullData((0L until K.toLong).toArray, model.priorTopicCountVec)
    println("tempNt: " + tempNt.mkString(" "))

    val deltaNwt = priorNwt.zip(lastUpdateTimes).filter{
      case ((wid, vec), lastTime) =>
        lastTime > 0 && lastTime < batchTime
    }.map{
      case ((wid, vec), lastTime) =>
        val batchDelta = (batchTime - lastBatchTime) / duration
        val yeta = math.pow(rate , batchDelta)
        val ret = vec.zipWithIndex.map(x => (wid, x._2, - x._1 * (1-yeta)) )
        Range(0, vec.length).foreach(k=> vec(k) *= yeta)
        ret
    }.flatMap(_.toSeq)
    Glint.pushData(deltaNwt, model.priorWordTopicCountMat)
    model.redisVocab.updateBatchTime(wordIds, batchTime)
    model.lock.releaseLock(token)
    (priorNt, priorNwt)
  }

  def logLikelihood(wordTopicCounts: Seq[(Long, Int, Double)],
                    docTopicCounts: Seq[(Int, Int, Double)],
                    docs: Seq[IdDoc]): Double ={

    val nwt = mutable.Map[Long, DenseVector[Double]]()
    wordTopicCounts.foreach{
      case (wid, topic, count) =>
        if (! nwt.contains(wid)) {
          nwt += ((wid, DenseVector.fill[Double](K)(model.beta)))
        }
        nwt(wid)(topic) += count
    }

    val ndt = mutable.Map[Int, DenseVector[Double]]()
    docTopicCounts.foreach{
      case (did, topic, count) =>
        if (! ndt.contains(did)) {
          ndt += ((did, DenseVector.fill[Double](K)(model.alpha)))
        }
        ndt(did)(topic) += count
    }

    logLikelihood(nwt, ndt, docs)

  }

  private def logLikelihood(nwt: mutable.Map[Long, DenseVector[Double]],
                            ndt: mutable.Map[Int, DenseVector[Double]],
                            docs: Seq[IdDoc]): Double = {

    var sum:Double = 0
    var count = 0
    docs.zipWithIndex.foreach{
      case (doc, id) =>
        doc.foreach{
          case (wid, topic) =>
            val pw = Range(0, K).map{
              k =>
              nwt(wid)(k) * ndt(id)(k)
            }.sum

            sum += math.log(pw)
        }
        count += doc.size
    }

    sum / count
  }

  def logLikelihood(priorNwt: Map[Long, Array[Double]],
                    priorNt: Array[Double],
                    wordTopicCounts: Seq[(Long, Int, Double)],
                    docTopicCounts: Seq[(Int, Int, Double)],
                    docs: Seq[IdDoc]): Double = {

    val totNwt = DenseVector.fill[Double](K)(0)
    val nwt = mutable.Map[Long, DenseVector[Double]]()
    wordTopicCounts.foreach{
      case (wid, topic, count) =>
        if (! nwt.contains(wid)) {
          nwt += ((wid, DenseVector.fill[Double](K)(model.beta)))
        }
        nwt(wid)(topic) += (priorNwt(wid)(topic) + count )
        totNwt(topic) += count
    }


    for( i <- 0 until K) {
      totNwt(i) += priorNt(i)
    }
    nwt.foreach(_._2 :/= totNwt)

    val ndt = mutable.Map[Int, DenseVector[Double]]()
    docTopicCounts.foreach{
      case (did, topic, count) =>
        if (! ndt.contains(did)) {
          ndt += ((did, DenseVector.fill[Double](K)(model.alpha)))
        }
        ndt(did)(topic) += count
    }


    val totNdt = DenseVector.fill[Double](K)(0)
    ndt.foreach( totNdt += _._2)
    ndt.foreach(_._2 :/= totNdt)

    logLikelihood(nwt, ndt, docs)
  }

  private def update(wordIds: Array[Long],
                     wordTopicCounts: Seq[(Long, Int, Double)],
                     time: Long,
                     token: String): Unit = {

    model.lock.fetchLock(token)

    val wordTopicDelta = wordTopicCounts.map{
      case (wid, topic, cnt) =>
        (wid, topic, cnt * rate)
    }

    Glint.pushData(wordTopicDelta, model.priorWordTopicCountMat)

    val topicDelta = Array.fill[Double](K)(0)
    wordTopicDelta.foreach {
      case (wid, topic, cnt) =>
        topicDelta(topic) += cnt
    }
    Await.result(
      model.priorTopicCountVec.push((0L until K.toLong).toArray, topicDelta),
      Duration.Inf
    )

    println("topicDelta: " + topicDelta.mkString(" "))
    val rest = Glint.pullData((0L until K.toLong).toArray, model.priorTopicCountVec)
    println("after update: " + rest.mkString(" "))
    model.lock.releaseLock(token)
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

object StreamLdaLearner{
  def main(args: Array[String]): Unit = {
    val doc = Seq((1L, 2), (3L,1), (0L, 1))
    val wordTopicCounts = Seq((1L, 2, 1.0), (0L, 1, 1.0), (3L, 1, 1.0))
    val docTopicCounts = Seq((0, 2, 1.0), (0, 1, 2.0))
    val model = new PsStreamLdaModel(4, 10L, 1.0, 1.0, 1.0, "bda07", 30001,60000, 0)
    val learner = new StreamLdaLearner(10, model)
    println(learner.logLikelihood(wordTopicCounts, docTopicCounts, Seq(doc)))
    model.destroy()
  }
}
