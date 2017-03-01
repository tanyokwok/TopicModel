package bda.spark.topic.core

import breeze.linalg.{DenseMatrix, DenseVector}

import scala.util.Random

/**
  */
object CollapsedGibbsSampler extends Serializable {

  /**
    * 根据先验知识分配采样主题
    * @return 采样的主题
    */
  def assign(prioriAlphaTopicStats: Seq[Int],
             prioriBetaTopicStats: Seq[Int],
             prioriBetaTopicStats4Term: Seq[Int]): Int = {

    assert(prioriAlphaTopicStats.length == prioriBetaTopicStats.length)
    assert(prioriBetaTopicStats.length == prioriBetaTopicStats4Term.length)

    val phis = prioriBetaTopicStats4Term.zip(prioriBetaTopicStats).map {
      case (termTopicCnt, topicCnt) =>
        termTopicCnt.toDouble / topicCnt
    }

    val sampleRate = prioriAlphaTopicStats.zip(phis).map {
      case ( alpha, phi) =>
        alpha * phi
    }

    sample(sampleRate)
  }

  /**
    * Gibbs采样算法, 下列参数的均为(K * 1)维的主题统计量列表, 第K维对应到第K个主题
    * @param prioriAlphaTopicStats alpha先验的统计量
    * @param prioriBetaTopicStats beta先验的统计量
    * @param prioriBetaTopicStats4Term 针对某个词项(term)的beta先验统计量
    * @param docTopicStats doc中观测到的主题统计量
    * @param termTopicStats 词项(term)观测到的主题统计量
    * @param topicStats 主题统计量
    * @return 采样得到的主题
    */
  def sample(prioriAlphaTopicStats: Seq[Int],
             prioriBetaTopicStats: Seq[Int],
             prioriBetaTopicStats4Term: Seq[Int],
             docTopicStats: Seq[Int],
             termTopicStats: Seq[Int],
             topicStats: Seq[Int]): Int = {

    assert(prioriAlphaTopicStats.length == prioriBetaTopicStats.length)
    assert(prioriAlphaTopicStats.length == docTopicStats.length)
    assert(prioriBetaTopicStats.length == prioriBetaTopicStats4Term.length)
    assert(prioriBetaTopicStats.length == termTopicStats.length)

    val docTopicStatsPlus = docTopicStats.zip(prioriAlphaTopicStats).map {
      case (cnt, alpha) => cnt + alpha
    }

    val termTopicStatsPlus = termTopicStats.zip(prioriBetaTopicStats4Term).map{
      case (cnt, beta) => cnt + beta
    }

    val topicStatsPlus = topicStats.zip(prioriBetaTopicStats).map{
      case (cnt, beta) => cnt + beta
    }

    val phis = termTopicStatsPlus.zip(topicStatsPlus).map{
      case (termTopicStat, topicStat) =>
        termTopicStat.toDouble / topicStat
    }


    val sampleRate = docTopicStatsPlus.zip(phis).map {
      case (docStat, phi) =>
        docStat* phi
    }

    sample(sampleRate)


  }

  def sample(docTopicStats: DenseVector[Int],
             termTopicStats: DenseVector[Int],
             topicStats: DenseVector[Int],
             alpha: Int, beta: Int, V:Int): Int ={

    val docTopicStatsPlus = docTopicStats + alpha
    val termTopicStatsPlus = termTopicStats + beta
    val topicStatsPlus  = topicStats + V * beta

    val phis =(docTopicStatsPlus :* termTopicStatsPlus).toArray.
      zip(topicStatsPlus.toArray).map{
      case (x, y) =>
        x.toDouble / y
    }

    val topic = sample(phis)
    topic
  }

  /**
    * 根据sampleRate进行维度采样
    * @param sampleRate 一个比例数组,第K维的值表示第K维被采样到的占比
    * @return 采样得到的维度
    */
  def sample(sampleRate: Seq[Double]): Int = {

    val totRate = sampleRate.sum

    val rand = totRate * Random.nextDouble()
    var cusum:Double = 0
    for (k <- 0 until sampleRate.length) {
      cusum += sampleRate(k)
      if (cusum > rand) return k
    }

    return sampleRate.length - 1

  }

  def main(args: Array[String]): Unit = {
    val p = Array(1.0, 9.0, 9.0)

    val cnt = Array(0, 0,0)
    val p2 = Range(0, 10000).map{
      k =>
        val x = sample(p)
        cnt(x) += 1
    }

    cnt.foreach( println )
  }
}


