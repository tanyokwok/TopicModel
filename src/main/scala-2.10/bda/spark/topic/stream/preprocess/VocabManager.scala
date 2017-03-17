package bda.spark.topic.stream.preprocess

import bda.spark.topic.core.{IdDocInstance, Instance, PsStreamLdaModel, TextDocInstance}
import bda.spark.topic.glint.Glint
import bda.spark.topic.redis.{RedisVocab, RedisVocabClient, RedisVocabPipeline}
import bda.spark.topic.utils.Timer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * TODO: 替换单词的同时删除参数服务器中对应的行
  */
class VocabManager(val ldaModel: PsStreamLdaModel,
                  val token: String) {

  val word2id = new mutable.HashMap[String, Long]()
  val redisVocab: RedisVocab = ldaModel.redisVocab

  def transfrom(input: Iterator[Instance], time: Long): Seq[IdDocInstance] = {

    val timer = new Timer()
    val examples = input.toSeq

    println(s"[VocabManager-$time] build vocabulary at time ${timer.getReadableRunnningTime()}")
    buildVocab(examples, time)
    println(s"[VocabManager-$time] build vocabulary success at time ${timer.getReadableRunnningTime()}")

    examples.map {
      example =>
        val textDocInstance = example.asInstanceOf[TextDocInstance]

        val idTokens: Seq[(Long, Int)] = textDocInstance.tokens.map {
          case (term, topic) =>
            val wid: Long = word2id(term)
            (wid, topic)
        }.filter( _._1 >= 0)
        val doc = new IdDocInstance(idTokens)
        doc
    }
  }

  def decode(input: Seq[IdDocInstance]): Seq[Instance]= {
    val id2word = word2id.map(x=>(x._2, x._1)).toMap
    input.map{
      IdDoc =>
        val textDoc = IdDoc.tokens.map{
          case (wid, topic) =>
            (id2word(wid), topic)
        }

        new TextDocInstance(textDoc)
    }
  }

  def relaseUsage(): Unit = {
    redisVocab.fetchLock(token)

    //println( s"before dec: ${word2id.map( x =>(x._1, redisVocab.getUseCount(x._2))).mkString(" ")}" )
    redisVocab.decUseCount(word2id.map(_._1).toArray)

    //println( s"after dec: ${word2id.map( x =>(x._1, redisVocab.getUseCount(x._2))).mkString(" ")}" )
    redisVocab.relaseLock(token)
  }

  private def buildVocab(examples: Seq[Instance], time: Long){

    val timer = new Timer()

    println(s"[Build Vocab] at ${timer.getReadableRunnningTime()}")
    //计算词汇集和词频
    val vocab_freq = examples.map{
      example =>
        val textDocInstance = example.asInstanceOf[TextDocInstance]
        val words = textDocInstance.tokens
        words
    }.flatMap(_.toSeq).groupBy(_._1).map{
      entry =>
        val word: String = entry._1
        val freq = entry._2.size
        (word, freq)
    }.toArray

    val vocab = vocab_freq.map(_._1)

    println(s"[Build Vocab] try to fetch lock at ${timer.getReadableRunnningTime()}")
    //获取词表锁,开始操作词表
    redisVocab.fetchLock(token)
    val word_freq2id = vocab_freq.zip(redisVocab.getTermIds(vocab, time))
    println(word_freq2id.size)

    println(s"[Build Vocab] try to fetch lock at ${timer.getReadableRunnningTime()}")
    val unkWord = word_freq2id.filter( _._2 < 0 )
    //println(s"unkWords: ${unkWord.map(x=>(x._1._1, x._2)).sorted.mkString(" ")}")

    val knwWord = word_freq2id.filter( _._2 >= 0)
    //println(s"knwWords: ${knwWord.map(x=>(x._1._1, x._2)).sorted.mkString(" ")}")

    //添加词汇使用计数
    redisVocab.incUseCount(knwWord.map(_._1._1))

    word2id ++= knwWord.map(x => (x._1._1, x._2)).toMap
    println(s"[Build Vocab] try to push data at ${timer.getReadableRunnningTime()}")


    //获取新词ID,并且添加词汇计数
    if (unkWord.size > 0) {
      val unkWordSortedByFreq = unkWord.sortBy(- _._1._2).map(_._1._1)
      val unkWord2Id = unkWordSortedByFreq.zip(
        redisVocab.addTerms(unkWordSortedByFreq, time)).toMap

      word2id ++= unkWord2Id

      val luckyWord2id = unkWord2Id.filter(_._2 >= 0 ).toArray

      ldaModel.lock.fetchLock(token)
      val matrix = Glint.pullData(luckyWord2id.map(_._2), ldaModel.priorWordTopicCountMat)
      val minusMatrix = matrix.map(x => (x._1, x._2.map(ldaModel.beta - _)))
      Glint.pushData(minusMatrix, ldaModel.priorWordTopicCountMat)

      val minusVec = Array.fill[Double](ldaModel.K)(0)
      minusMatrix.foreach{
        case (wid, vec) =>
          Range(0, vec.length).foreach{
            k =>
              minusVec(k) += vec(k)
          }
      }

      println("add word: " + minusVec.mkString(" "))
      Glint.pushData(minusVec, ldaModel.priorTopicCountVec)
      val temp = Glint.pullData((0L until ldaModel.K.toLong).toArray, ldaModel.priorTopicCountVec)
      println("temp : " + temp.mkString(" "))
      ldaModel.lock.releaseLock(token)
      redisVocab.incUseCount(luckyWord2id.map(_._1))
    }

    println(s"[Build Vocab] try to release lock at ${timer.getReadableRunnningTime()}")
    //val nt = Glint.pullData((0L until 10L).toArray, ldaModel.priorTopicCountVec)
    //println( nt.mkString(" "))

    //println( s"word2id: ${word2id.toList.take(10).sorted.mkString(" ") }")
    redisVocab.relaseLock(token)
  }
}
