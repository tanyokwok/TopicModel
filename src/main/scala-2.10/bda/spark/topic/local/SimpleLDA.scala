package bda.spark.topic.local

import bda.spark.topic.core._
import bda.spark.topic.linalg.Utils
import bda.spark.topic.utils.Timer
import breeze.collection.mutable.SparseArrayMap
import breeze.linalg.{*, BroadcastedColumns, DenseMatrix, DenseVector, SparseVector, Transpose}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by Roger on 17/2/24.
  */
class SimpleLDA(val K: Int,
                val alpha: Int,
                val beta: Int) {

  var model: SimpleLdaModel = null

  def train(input: Seq[Example], test: Seq[Example], iteration: Int): Unit = {
    val word2id = buildVocabulary(input)
    println( s"The vocabulary size is: ${word2id.size}" )
    var docs = initTopicAssignment(input)
    val termTopicStatsMat = doTopicStatistic(docs, word2id)

    this.model = new SimpleLdaModel(K, beta, alpha, termTopicStatsMat, word2id)
    val sizeOfModel = this.model.estimatedMemUse()
    println( s"About ${sizeOfModel/1024} M used!")

    val timer = new Timer()
    println(s"Start trainning, time = ${timer.getReadableRunnningTime()}")
    val logLike = logLikelihood(test, 2)
    println(s"log(likelihood) = $logLike")

    Range(0, iteration).foreach{
      k =>
        docs = doGibbsSampling(docs, this.model.betaStatsMatrix, this.model.word2id)
        println(s"iteration: ${k} time = ${timer.getReadableRunnningTime()}")
        if ( (1 + k) % 10 == 0) {
          println("===============================")
          this.model.topicWords(10).map {
            queue =>
              val tokens = for (k <- 0 until 10) yield queue.dequeue()
              tokens.reverse.map(e => s"${e._1}(${e._2})").mkString(" ")
          }.foreach(println)

          println( Utils.foldVectors(this.model.betaStatsMatrix).toArray.mkString(" "))

          val logLike = logLikelihood(test, 2)
          println(s"log(likelihood) = $logLike")
          println("===============================")
        }
    }

  }

  def logLikelihood(input: Seq[Example], S: Int): Double ={

    var docs = initTopicAssignment(input)

    val knownTermSize = this.model.vocabulary.size
    val testWord2id = buildVocabulary(input)
    val unkTerms2id = testWord2id.filter{
      case (term, id) =>
      !this.model.vocabulary.contains(term)
    }.map(_._1).zipWithIndex.map{
      case (term, id) =>
        (term, id + knownTermSize)
    }.toMap

    val word2id = unkTerms2id ++ this.model.word2id
    val termTopicStatsMat = doTopicStatistic(docs, word2id)

    Range(0, knownTermSize).foreach{
      row =>
        termTopicStatsMat(row, ::) += this.model.betaStatsMatrix(row, ::)
    }


    var predict = new ArrayBuffer[Seq[DOC]]()
    Range(0, S*2).foreach{
      s =>
      val newDocs = doGibbsSampling(docs, termTopicStatsMat, word2id)
       predict += newDocs
        docs = newDocs
    }

    predict = predict.reverse.take(S)

    val topicAssign = Range(0, S).map{
      s =>
        val docs = predict(s)
        val rows = docs.size
        //println(rows)
        val assignList = Range(0, rows).map{
          row =>
            docs(row).map{
              case (term, topic) =>
                ((s, row, term), topic)
            }
        }.flatMap(_.toSeq)
        assignList
    }.flatMap( _.toSeq )


    val V = testWord2id.size
    val R = docs.size
    val termTopicTensor = Array.fill[DenseMatrix[Int]](S)( DenseMatrix.fill[Int](V, K)(0))
    val docTopicTensor = Array.fill[DenseMatrix[Int]](S)(DenseMatrix.fill[Int](R, K)(0))

    topicAssign.groupBy(_._1).map{
      entry =>
        val (s, row, term) = entry._1
        val group = entry._2
        val wid = testWord2id(term)
        group.map{
          case (key, topic) =>
            termTopicTensor(s)(wid, topic) += 1
            docTopicTensor(s)(row, topic) += 1
        }
    }

    val probs = Range(0, S).map {
      s =>

        val termTopicVec: DenseVector[Double] = Utils.foldVectors(termTopicTensor(s)).map(_.toDouble + V * beta)
        val docTopicVec: DenseVector[Double] = Utils.foldVectors(docTopicTensor(s)).map(_.toDouble + R * alpha)

        val thetaMat: DenseMatrix[Double] = docTopicTensor(s).map(_.toDouble + alpha)
        Range(0, R).foreach {
          r =>
            thetaMat(r, ::).inner :/= docTopicVec
        }

        val phiMat = termTopicTensor(s).map(_.toDouble + beta)
        Range(0, V).foreach {
          v =>
            phiMat(v, ::).inner :/= termTopicVec
        }

        (thetaMat, phiMat)
    }

    Range(0, R).map{
      r =>
        docs(r).map {
          case (term, topic) =>
            val wid = testWord2id(term)
            val product = Range(0, S).map{
              s =>
                val (theta, phi) = probs(s)
                theta(r, ::).inner dot phi(wid, ::).inner
            }.sum

            math.log(product) - math.log(S)
        }
    }.flatMap( _.toSeq ).reduce(_ + _)

  }

  private def initTopicAssignment(example: Example): DOC = {
    val docInstance = example.instance.asInstanceOf[DocInstance]

    var postTopicStatsVec: DenseVector[Int] = null
    if (this.model != null) {
      postTopicStatsVec = Utils.foldVectors(this.model.betaStatsMatrix)
    }
    docInstance.tokens.map {
      case (term, topic) =>
        /*if (this.model != null && this.model.vocabulary.contains(term)) {
          val wid = this.model.word2id(term)
          val pw_z = this.model.betaStatsMatrix(wid, ::).inner.toArray.map(_.toDouble)
          (term, CollapsedGibbsSampler.sample(pw_z))
        } else {
          // 如果模型中没有改词汇的信息
          (term, Math.abs(Random.nextInt()) % K)
        }*/
          (term, Math.abs(Random.nextInt()) % K)
    }
  }

  private def initTopicAssignment(examples: Seq[Example]):Seq[DOC]= {
    examples.map {
      example =>
        val doc =initTopicAssignment(example)
        doc
    }

  }

  private def buildVocabulary(examples: Seq[Example]) = {

    val vocab = examples.map{
      example =>
        val docInstance = example.instance.asInstanceOf[DocInstance]
        docInstance.tokens.map(_._1).toSet
    }.flatMap(_.toSeq).toSet

    vocab.toSeq.sorted.zipWithIndex.toMap
  }

  private def doTopicStatistic(docs: Seq[DOC], word2id: Map[String, Int]):
  DenseMatrix[Int] = {
     val termTopicStatsMat: DenseMatrix[Int] = DenseMatrix.fill[Int](word2id.size, K)(0)
    //统计(词项,主题)共现矩阵
    docs.flatMap(_.toSeq).groupBy(_._1).foreach{
      entry =>
        val term = entry._1
        val group = entry._2
        val statVec = DenseVector.fill[Int](K)(0)
        group.foreach{
          case (t, topic) =>
           statVec(topic) += 1
        }
        val wid = word2id(term)
        termTopicStatsMat(wid, ::).inner += statVec
    }

    termTopicStatsMat
  }

  private def doGibbsSampling(docs: Seq[DOC], betaStatsMatrix: DenseMatrix[Int], word2id: Map[String,Int]):
  ArrayBuffer[DOC] = {

    val corpusTopicStatsVec = Utils.foldVectors(betaStatsMatrix)

    val newDocs = mutable.ArrayBuffer[DOC]()
    docs.foreach{
      doc =>
        val docTopicStats = DenseVector.fill[Int](K)(0)
        doc.groupBy(_._2).foreach{
          entry =>
            val topic = entry._1
            val cnt = entry._2.size
            docTopicStats(topic) += cnt
        }

        val newDoc = mutable.ArrayBuffer[(String, Int)]()
        doc.foreach{
          case (term, topic) =>
            val wid = word2id(term)
            betaStatsMatrix(wid,topic) -= 1
            docTopicStats(topic) -= 1
            corpusTopicStatsVec(topic) -= 1

            val termTopicStatsVec = betaStatsMatrix(wid, ::).inner
            val newTopic = CollapsedGibbsSampler.sample(
              docTopicStats, termTopicStatsVec,
              corpusTopicStatsVec, alpha, beta, word2id.size
            )
            betaStatsMatrix(wid,newTopic) += 1
            docTopicStats(newTopic) += 1
            corpusTopicStatsVec(newTopic) += 1

            newDoc += ((term, newTopic))
        }

        newDocs += newDoc
    }

    newDocs
  }


}
