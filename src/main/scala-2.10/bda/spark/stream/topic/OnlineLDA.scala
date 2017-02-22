package bda.spark.stream.topic

import bda.spark.stream.core.{DOC, DocInstance, Example}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

/**
  * Created by Roger on 17/2/16.
  */
class OnlineLDA(val K: Int,
                val alpha: Int,
                val beta: Int) extends Serializable {
  val model: OnlineLDAModel = new OnlineLDAModel(K, alpha, beta, Array.fill[Int](K)(alpha), mutable.Map())

  /**
    * Train the model based on the algorithm implemented in the learner,
    * from the stream of Examples given for training.
    *
    * @param input a stream of Examples
    */
  def train(input: DStream[Example]): Unit = {

    input.foreachRDD{
      exampleRDD =>
        //获取本次RDD的未知词汇表,并添加到model中
        val localModel = checkLocalVocabulary(exampleRDD, this.model)
        val docs = initTopicAssignment(exampleRDD, localModel)

        val (docTopicStatsMat, termTopicStatsMat) =
          doGibbsSampling(docs, localModel)

        updateModel(docTopicStatsMat, termTopicStatsMat, localModel.K)
    }
  }

  private def assign(input: DStream[Example]): DStream[Example] = {
    return null
  }

  private def doGibbsSampling(docs: RDD[DOC],
                              localModel: OnlineLDAModel):
  (RDD[(DOC, Array[Int])], mutable.Map[String, Array[Int]]) = {

    val (docTopicStatsMat, termTopicStatsMat) = doTopicStatistic(docs, localModel.K)
    val termTopicStatsVec = termTopicStatsMat.values.fold(Array.fill[Int](localModel.K)(0)) {
      (ent1, ent2) =>
        Range(0, ent2.length).foreach {
          k =>
            ent1(k) += ent2(k)
        }
        ent1
    }

    val prioriBetaStatsVector = localModel.prioriBetaStatsVector()
    val newDocs: RDD[DOC] = docTopicStatsMat.map {
      case (doc, docTopicStats) =>
        doc.map {
          case (term, topic) =>
            docTopicStats(topic) -= 1
            termTopicStatsMat(term)(topic) -= 1
            termTopicStatsVec(topic) -= 1

            val newTopic = CollapsedGibbsSampler.sample(
              localModel.prioriAlphaStatsVector,
              prioriBetaStatsVector,
              localModel.prioriBetaStatsMatrix(term),
              docTopicStats,
              termTopicStatsMat(term),
              termTopicStatsVec
            )

            docTopicStats(newTopic) += 1
            termTopicStatsMat(term)(newTopic) += 1
            termTopicStatsVec(newTopic) += 1

            (term, newTopic)
        }
    }

    doTopicStatistic(newDocs, localModel.K)
  }

  private def updateModel(docTopicStatsMat: RDD[(DOC, Array[Int])],
                          termTopicStatsMat: mutable.Map[String, Array[Int]],
                          topicNum: Int): Unit = {

    val docTopicStatsVec =
      foldVectors(docTopicStatsMat.map(_._2), topicNum)

    Range(0, this.model.K).foreach {
      k =>
        this.model.prioriAlphaStatsVector(k) += docTopicStatsVec(k)
    }

    termTopicStatsMat.foreach {
      case (term, topics) =>
        if (this.model.prioriBetaStatsMatrix.contains(term)) {
          this.model.prioriBetaStatsMatrix += ((term, topics))
        } else {
          Range(0, this.model.K).foreach { k =>
            this.model.prioriBetaStatsMatrix(term)(k) += topics(k)
          }
        }
    }
  }

  private def doTopicStatistic(docs: RDD[DOC], topicNum: Int):
  (RDD[(DOC, Array[Int])], mutable.Map[String, Array[Int]]) = {

    val docTopicStatsMat = docs.map {
      doc =>
        val docTopicStatVec = Array.fill[Int](topicNum)(0)
        doc.foreach {
          case (term, topic) =>
            docTopicStatVec(topic) += 1
        }
        (doc, docTopicStatVec)
    }

    val termTopicStatsMat = docs.map {
      doc =>
        val innterTermTopicStatsMat = mutable.Map[String, Array[Int]]()
        doc.foreach {
          case (term, topic) =>
            val termTopicStatVec = innterTermTopicStatsMat.getOrElse(term, Array.fill[Int](topicNum)(0))
            termTopicStatVec(topic) += 1
        }
        innterTermTopicStatsMat
    }.fold(mutable.Map[String, Array[Int]]()) {
      (ent1, ent2) =>
        val x: mutable.Map[String, Array[Int]] = ent1
        ent2.foreach {
          case (term, topicCntRight) =>
            if (ent1.contains(term)) {
              val tpcCntLeft = ent1(term) //ent1.getOrElse(term, Array.fill[Int](topicCntRight.length))
              Range(0, topicCntRight.length).foreach {
                k =>
                  tpcCntLeft(k) += topicCntRight(k)
              }
            } else {
              ent1 += ((term, topicCntRight))
            }
        }
        ent1
    }

    (docTopicStatsMat, termTopicStatsMat)
  }

  private def initTopicAssignment(exampleRDD: RDD[Example],
                                  localModel: OnlineLDAModel): RDD[DOC] = {

    val prioriBetaStatsVector = this.model.prioriBetaStatsVector()
    val docs: RDD[DOC]
    = exampleRDD.map {
      example =>
        val docInstance = example.instance.asInstanceOf[DocInstance]
        val doc: DOC
        = docInstance.tokens.map {
          case (term, oldTopic) =>
            val topic = CollapsedGibbsSampler.assign(
              this.model.prioriAlphaStatsVector,
              prioriBetaStatsVector,
              this.model.prioriBetaStatsMatrix(term))

            (term, topic)
        }
        doc
    }
    docs
  }

  private def checkLocalVocabulary(exampleRDD: RDD[Example],
                                   localModel: OnlineLDAModel): OnlineLDAModel = {
    val global_term_cnt = exampleRDD.map {
      example =>
        val docInstance = example.instance.asInstanceOf[DocInstance]

        val local_term_cnt =
          docInstance.tokens.groupBy(_._1).map {
            case entry =>
              val term = entry._1
              val group = entry._2
              val cnt = group.length

              (term, cnt)
          }
        local_term_cnt
    }.flatMap(_.toSeq).reduceByKey(_ + _)

    val unkVocab = global_term_cnt.map(_._1).filter {
      !localModel.vocabulary.contains(_)
    }.collect()

    unkVocab.foreach(localModel.addNewTerm(_))

    localModel
  }

  private def foldVectors(mat: RDD[Array[Int]], k: Int): Array[Int] = {
    mat.fold(Array.fill[Int](k)(0)) {
      (ent1, ent2) =>
        Range(0, k).foreach {
          i =>
            ent1(i) += ent2(i)
        }
        ent1
    }
  }

}
