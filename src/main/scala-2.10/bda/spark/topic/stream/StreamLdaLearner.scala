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
                       val rate: Double,
                       val model: PsStreamLdaModel) extends Serializable{
  val V = model.V
  val K = model.K

  def update(batch: Seq[IdDocInstance], token: String): Seq[IdDocInstance]= {

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


    println(s"$token: start gibbs sampling at ${timer.getReadableRunnningTime()}")
    val topicSamples = new ArrayBuffer[Seq[IdDoc]]()
    //执行iteration次采样使得batch收敛
    for (iter <- 0 until iteration) {
      val newDocs: Seq[IdDoc] = doGibbsSampling(docs, batchNdt, batchNwt, batchNt)
      topicSamples += newDocs
      docs = newDocs
    }

    val stableSamples = topicSamples.reverse.take(5)

    //val wordTopicCounts = docs.flatMap(_.toSeq).groupBy(x => x).map {
    //取最后5次采样的平均值
    val wordTopicCounts = stableSamples.flatMap(_.toSeq).
      flatMap(_.toSeq).groupBy(x => x).map {
      entry =>
        val (term, topic) = entry._1
        val cnt = entry._2.size
        (term, topic, cnt.toDouble / 5)
    }.toSeq

    println(s"$token: update model at ${timer.getReadableRunnningTime()}")
    update(wordIds, wordTopicCounts, token)
    println(
      s"/////////////////////////////////////////////////\n" +
      s"Log(likelihood): ${logLikelihood(priorNwt, priorNt, wordTopicCounts)}\n" +
      s"/////////////////////////////////////////////////")


    println(s"$token: update success at ${timer.getReadableRunnningTime()}")
    docs.map(new IdDocInstance(_))
  }

  def logLikelihood(priorNwt: Map[Long, Array[Double]],
                    priorNt: Array[Double],
                    wordTopicCounts: Seq[(Long, Int, Double)]): Double = {

    val ndt = Array.fill[Double](K)(model.alpha)
    wordTopicCounts.foreach{
      case (wid, topic, count) =>
        priorNwt(wid)(topic) += count
        priorNt(topic) += count
        ndt(topic) += count
    }

    val nd = ndt.sum
    val theta = ndt.map( _ / nd)

    val pws = priorNwt.map{
      case (wid, vec) =>
        val phi = vec.zip(priorNt).map{case (x,y)=> x / y}
        val pw = phi.zip(theta).map{case (x, y) => x * y }.sum
        (wid, pw)
    }


    var sum = 0.0
    var cnt = 0.0
    wordTopicCounts.foreach{
      case (wid, topic, count) =>
        sum += math.log(pws(wid)) * count
        cnt += count
    }
    sum / cnt
  }

  private def update(wordIds: Array[Long],
                          wordTopicCounts: Seq[(Long, Int, Double)],
                          token: String): Unit = {

    model.lock.fetchLock(token)

    val priorNwt = Glint.pullData(wordIds, model.priorWordTopicCountMat)
    val priorNt = Glint.pullData((0L until K.toLong).toArray, model.priorTopicCountVec)


    val wordTopicDelta = priorNwt.map(e => (e._1, e._2.map(-_*rate)))
    wordTopicCounts.foreach{
      case (wid, topic, cnt) =>
        wordTopicDelta(wid)(topic) += cnt * rate
    }
    Glint.pushData(wordTopicDelta, model.priorWordTopicCountMat)

    val topicDelta = priorNt.map(- _ * rate)
    wordTopicCounts.groupBy(_._2).foreach {
      entry =>
        val topic = entry._1
        val cnt = entry._2.map(_._3).sum
        topicDelta(topic) += cnt * rate
    }
    Await.result(
      model.priorTopicCountVec.push((0L until K.toLong).toArray, topicDelta),
      Duration.Inf
    )

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
