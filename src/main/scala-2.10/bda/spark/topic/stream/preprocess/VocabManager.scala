package bda.spark.topic.stream.preprocess

import bda.spark.topic.core.{IdDocInstance, Instance, PsStreamLdaModel, TextDocInstance}
import bda.spark.topic.glint.Glint
import bda.spark.topic.redis.RedisVocabClient

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * TODO: 替换单词的同时删除参数服务器中对应的行
  */
class VocabManager(val redisVocab: RedisVocabClient,
                   val ldaModel: PsStreamLdaModel,
                  val token: String) {

  val word2id = new mutable.HashMap[String, Long]()

  def transfrom(input: Iterator[Instance], time: Long): Seq[IdDocInstance] = {
    val examples = input.toSeq

    buildVocab(examples, time)

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

    word2id.foreach{
      case (word, wid) =>
      redisVocab.decUseCount(wid)
    }

    //println( s"after dec: ${word2id.map( x =>(x._1, redisVocab.getUseCount(x._2))).mkString(" ")}" )
    redisVocab.relaseLock(token)
  }

  private def buildVocab(examples: Seq[Instance], time: Long){

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
    }

    //获取词表锁,开始操作词表
    redisVocab.fetchLock(token)
    val word_freq2id = new ArrayBuffer[((String, Int), Long)]() // thread safe
    vocab_freq.foreach {
      case (word, freq) =>
        val wid = redisVocab.getTerm(word, time)
        word_freq2id += (((word, freq), wid))
    }

    val unkWord = word_freq2id.filter( _._2 < 0 )
    //println(s"unkWords: ${unkWord.map(x=>(x._1._1, x._2)).sorted.mkString(" ")}")

    val knwWord = word_freq2id.filter( _._2 >= 0)
    //println(s"knwWords: ${knwWord.map(x=>(x._1._1, x._2)).sorted.mkString(" ")}")

    //添加词汇使用计数
    knwWord.foreach{
      case ((word, freq), wid) =>
        redisVocab.incUseCount(wid)
    }

    word2id ++= knwWord.map(entry => (entry._1._1, entry._2)).toMap
    //获取新词ID,并且添加词汇计数
    if (unkWord.size > 0) {
      var flag = true
      word2id ++= unkWord.map(_._1).sortBy(-_._2).map{
        case (word, freq) =>
          if (flag) {
            val wid: Long = redisVocab.addTerm(word, time)
            // 如果返回值小于0, 说明表已经满了
            if (wid < 0) {
              //置flag为false, 使得剩下的所有词汇的id为-1
              flag = false
            } else {
              //先删除原有的数据, 新增的单词需要加上先验alpha
              val vec = Glint.pullData(wid, ldaModel.priorWordTopicCountMat)
              val minusVec = vec.map(ldaModel.beta - _)
              Glint.pushData(wid, minusVec, ldaModel.priorWordTopicCountMat)
              Glint.pushData(minusVec, ldaModel.priorTopicCountVec)
              redisVocab.incUseCount(wid)
            }
            (word, wid)
          } else {
            (word, -1L)
          }
      }.toMap
    }

    //val nt = Glint.pullData((0L until 10L).toArray, ldaModel.priorTopicCountVec)
    //println( nt.mkString(" "))

    //println( s"word2id: ${word2id.toList.take(10).sorted.mkString(" ") }")
    redisVocab.relaseLock(token)
  }
}
