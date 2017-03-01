package bda.spark.topic.stream

import bda.spark.topic.core._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Roger on 17/2/16.
  */
class OnlineLDA(val K: Int,
                val alpha: Int,
                val beta: Int) extends Serializable {

  var model: OnlineLdaModel = new OnlineLdaModel(0, K, alpha, beta, Array.fill[Int](K)(alpha), Map())

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
        val docs = initTopicAssignment(exampleRDD, this.model)
        val localModel = checkLocalVocabulary(exampleRDD)
        val (docTopicStatsMat, termTopicStatsMat) =
          doGibbsSampling(docs, localModel)

        this.model = createNewModel(docTopicStatsMat, termTopicStatsMat, localModel)
        println( "===============================" )
        println( s"iteration: ${this.model.iteration}")
        this.model.topicWords(10).map{
          queue =>
            val tokens = for( k <- 0 until 10) yield queue.dequeue()
            tokens.reverse.map( e => s"${e._1}(${e._2})").mkString(" ")
        }.foreach( println )

        this.model.prioriAlphaStatsVector.foreach( println )

        println( "===============================" )
    }
  }

  def assign(input: DStream[Example]): DStream[Example] = {
    input.map{
      example =>
        val doc = initTopicAssignment(example, this.model)
        new Example(new DocInstance(doc))
    }
  }

  private def doGibbsSampling(docs: RDD[DOC],
                              localModel: OnlineLdaModel):
  (RDD[(DOC, Array[Int])], Map[String, Array[Int]]) = {

    val (docTopicStatsMat, termTopicStatsMat) = doTopicStatistic(docs)
    val termTopicStatsVec = termTopicStatsMat.map(_._2).reduce{
      (ent1, ent2) =>

        ent1.zip(ent2).map{
          case (x, y) =>
            x + y
        }
    }

    val newDocs: RDD[DOC] = docTopicStatsMat.map {
      case (doc, docTopicStats) =>
        doc.map {
          case (term, topic) =>
            docTopicStats(topic) -= 1
            termTopicStatsMat(term)(topic) -= 1
            termTopicStatsVec(topic) -= 1

            val newTopic = CollapsedGibbsSampler.sample(
              localModel.prioriAlphaStatsVector,
              localModel.prioriBetaStatsVector,
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

    doTopicStatistic(newDocs)
  }

  private def createNewModel(docTopicStatsMat: RDD[(DOC, Array[Int])],
                          termTopicStatsMat: Map[String, Array[Int]],
                          localModel: OnlineLdaModel): OnlineLdaModel = {


    val docTopicStatsVec =
      foldVectors(docTopicStatsMat.map(_._2))

    val prioriAlpha = docTopicStatsVec.
      zip( localModel.prioriAlphaStatsVector ).map{
      case (x,y) =>
        x + y
    }

    val vocab = localModel.vocabulary


    val prioriBetaKnown = termTopicStatsMat.filter{
      case (term, topicStats) =>
         vocab.contains(term)
    }.map{
      case (term, topicStats) =>
       val newTopicStats = localModel.prioriBetaStatsMatrix(term).zip(topicStats).map{
         case (x,y) =>
           x + y
       }
        (term, newTopicStats)
    }

    val priorBetaUnk = termTopicStatsMat.filter{
      case (term, topicStats) =>
        !vocab.contains(term)
    }

    val newModel =  new OnlineLdaModel(localModel.iteration + 1, K, alpha, beta,
      prioriAlpha, prioriBetaKnown ++ priorBetaUnk )

    newModel
  }

  private def doTopicStatistic(docs: RDD[DOC]):
  (RDD[(DOC, Array[Int])], Map[String, Array[Int]]) = {

    //统计(文档,主题)共现矩阵
    val docTopicStatsMat = docs.map {
      doc =>
        val statVec = Array.fill[Int](K)(0)
        doc.map{
          case (term, topic) =>
            (topic, 1)
        }.groupBy(_._1).foreach{
          entry =>
            val topic = entry._1
            val cnt = entry._2.size
            (topic, cnt)
            statVec(topic) = cnt
        }
        (doc, statVec)
    }

    //统计(词项,主题)共现矩阵
    val termTopicStatsMat: Map[String, Array[Int]] = docs.map {
      doc =>
        doc.map{
          case (term, topic) =>
            ((term, topic) ,1 )
        }
    }.flatMap( _.toSeq).reduceByKey( _ + _ ).map{
      case ((term, topic), cnt) =>
        (term, (topic, cnt))
    }.groupBy(_._1).map{
      entry =>
        val term = entry._1
        val group = entry._2
        val statVec = Array.fill[Int](K)(0)
        group.foreach{
          case (t, (topic, cnt)) =>
           statVec(topic) = cnt
        }
        (term, statVec)
    }.collect().toMap
    (docTopicStatsMat, termTopicStatsMat)
  }

  private def initTopicAssignment(exampleRDD: RDD[Example],
                                  localModel: OnlineLdaModel): RDD[DOC] = {

    val docs: RDD[DOC]
    = exampleRDD.map {
      example =>
        initTopicAssignment(example, localModel)
    }
    docs
  }

  private def initTopicAssignment(example: Example, localModel: OnlineLdaModel): DOC = {
    val docInstance = example.instance.asInstanceOf[DocInstance]
    val doc: DOC
    = docInstance.tokens.map {
      case (term, oldTopic) =>
        if( localModel.prioriBetaStatsMatrix.contains(term)) {
          val topic = CollapsedGibbsSampler.assign(
            localModel.prioriAlphaStatsVector,
            localModel.prioriBetaStatsVector,
            localModel.prioriBetaStatsMatrix(term))

          (term, topic)
        } else {
           val topic = CollapsedGibbsSampler.assign(
            localModel.prioriAlphaStatsVector,
            localModel.prioriBetaStatsVector.map( _ + beta),
            Array.fill[Int](K)(beta))

          (term, topic)
        }

    }
    doc
  }

  private def checkLocalVocabulary(exampleRDD: RDD[Example]): OnlineLdaModel = {
    val global_term_cnt = exampleRDD.map {
      example =>
        val docInstance = example.instance.asInstanceOf[DocInstance]
        docInstance.tokens.map(x=>(x._1,1))
    }.flatMap(_.toSeq).reduceByKey(_ + _)

    val unkVocab = global_term_cnt.filter {
      case (term, cnt) =>
      !this.model.vocabulary.contains(term)
    }.map(_._1).collect()

    val unkBeta = unkVocab.map{
      term =>
        (term, Array.fill[Int](K)(beta))
    }

    new OnlineLdaModel(this.model.iteration, K, alpha, beta,
      this.model.prioriAlphaStatsVector,
      this.model.prioriBetaStatsMatrix ++ unkBeta)
  }

  private def foldVectors(mat: RDD[Array[Int]]): Array[Int] = {
    mat.fold(Array.fill[Int](K)(0)) {
      (ent1, ent2) =>
        Range(0, K).foreach {
          i =>
            ent1(i) += ent2(i)
        }
        ent1
    }
  }

}
