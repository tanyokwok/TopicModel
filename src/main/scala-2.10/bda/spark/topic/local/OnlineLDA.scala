package bda.spark.topic.local

import bda.spark.topic.core._

/**
  * Created by Roger on 17/2/24.
  */
class OnlineLDA(val K: Int,
                val alpha: Int,
                val beta: Int) {

  var model: OnlineLdaModel = new OnlineLdaModel(0, K, alpha, beta, Array.fill[Int](K)(alpha), Map())

  def train(input: Seq[Example]): Unit = {
    val docs = initTopicAssignment(input, this.model)
    val localModel = checkLocalVocabulary(input)
    val (docTopicStatsMat, termTopicStatsMat) =
      doGibbsSampling(docs, localModel)

    this.model = createNewModel(docTopicStatsMat, termTopicStatsMat, localModel)
    println("===============================")
    println(s"iteration: ${this.model.iteration}")
    this.model.topicWords(10).map {
      queue =>
        val tokens = for (k <- 0 until 10) yield queue.dequeue()
        tokens.reverse.map(e => s"${e._1}(${e._2})").mkString(" ")
    }.foreach(println)

    this.model.prioriAlphaStatsVector.foreach(println)

    println("===============================")

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

  private def initTopicAssignment(examples: Seq[Example], localModel: OnlineLdaModel):Seq[DOC]={

    examples.map{
      example =>
        initTopicAssignment(example, localModel)
    }
  }

  private def checkLocalVocabulary(examples: Seq[Example]):OnlineLdaModel = {

    val termCnt = examples.map{
      example =>
        val docInstance = example.instance.asInstanceOf[DocInstance]
        docInstance.tokens.map(x => (x._1, 1))
    }.flatMap(_.toSeq).groupBy(_._1).map{
      case entry =>
        val term = entry._1
        val group = entry._2
        val cnt = group.size
        (term ,cnt)
    }

    val unkVocab = termCnt.filter{
      case (term, cnt) =>
      !this.model.vocabulary.contains(term)
    }.map(_._1)

    val unkBeta = unkVocab.map( (_, Array.fill[Int](K)(beta)))

    new OnlineLdaModel(this.model.iteration, K, alpha, beta,
      this.model.prioriAlphaStatsVector,
      this.model.prioriBetaStatsMatrix ++ unkBeta)
  }

   private def doTopicStatistic(docs: Seq[DOC]):
  (Seq[(DOC, Array[Int])], Map[String, Array[Int]]) = {

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
    val termTopicStatsMat: Map[String, Array[Int]] =
    docs.flatMap(_.toSeq).groupBy(_._1).map{
      entry =>
        val term = entry._1
        val group = entry._2
        val statVec = Array.fill[Int](K)(0)
        group.foreach{
          case (t, topic) =>
           statVec(topic) += 1
        }
        (term, statVec)
    }
    (docTopicStatsMat, termTopicStatsMat)
  }

  private def doGibbsSampling(docs: Seq[DOC], localModel: OnlineLdaModel) = {

    val (docTopicStatsMat, termTopicStatsMat) = doTopicStatistic(docs)
    val termTopicStatsVec = termTopicStatsMat.map(_._2).reduce{
      (ent1, ent2) =>
        ent1.zip(ent2).map{
          case (x, y) =>
            x + y
        }
    }

    val newDocs: Seq[DOC] = docTopicStatsMat.map {
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

  private def createNewModel(docTopicStatsMat: Seq[(DOC, Array[Int])],
                          termTopicStatsMat: Map[String, Array[Int]],
                          localModel: OnlineLdaModel): OnlineLdaModel = {


    val docTopicStatsVec =
      foldVectors(docTopicStatsMat.map(_._2))

    Range(0, K).foreach{
      k=>
      localModel.prioriAlphaStatsVector(k) += docTopicStatsVec(k)
    }

    val vocab = localModel.vocabulary

    val prioriBeta = termTopicStatsMat.foreach{
      case (term, topicStats) =>
        Range(0, K).foreach{
          k =>
            localModel.prioriBetaStatsMatrix(term)(k) += topicStats(k)
        }
    }

    val newModel =  new OnlineLdaModel(localModel.iteration + 1, K, alpha, beta,
      localModel.prioriAlphaStatsVector, localModel.prioriBetaStatsMatrix)

    newModel
  }

  private def foldVectors(mat: Seq[Array[Int]]): Array[Int] = {
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
