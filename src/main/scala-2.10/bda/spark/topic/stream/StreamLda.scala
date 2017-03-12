package bda.spark.topic.stream

import bda.spark.topic.core.Instance
import bda.spark.topic.redis.RedisVocabClient
import bda.spark.topic.stream.preprocess.VocabManager
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Roger on 17/3/10.
  */
class StreamLda(val host: String = "localhost",
                val port: Int = 0,
                val lda: StreamLdaLearner
               ) extends Serializable{

  val maxVocabSize: Long = lda.model.V

  private def train(data: RDD[Instance], time: Long): RDD[Instance] = {
    data.mapPartitionsWithIndex{
      case (index, instancesInPartition) =>

        val id = s"partition-$index at time-$time"
        println(s"train $id")
        //vocabulary manager
        val redisVocab = RedisVocabClient(host, port, maxVocabSize)
        val vocabManager = new VocabManager(redisVocab, lda.model, s"partition-$index+time-$time")

        println(s"$id: transfrom data ")

        //println("text: " + instancesInPartition.take(10).mkString(" "))
        //get batch train data
        val batch = vocabManager.transfrom(instancesInPartition, time)

        println(s"$id: update lda model")
        //asynchronized update lda model
        val assign = lda.update(batch)

        println(s"$id: decode topic assign")
        val ret = vocabManager.decode(assign)

        //release the usage of words
        println(s"$id: release usage")
        vocabManager.relaseUsage()
        redisVocab.close()
        ret.toIterator
    }
  }

  def init(): Unit = {
    RedisVocabClient.clear(host, port)
  }

  def train(data: DStream[Instance]): DStream[Instance] = {
    init()
    data.transform{
      (rdd, time) =>
        val ret: RDD[Instance] = train(rdd, time.milliseconds)
        ret
    }
  }

  def clean(): Unit = {
    RedisVocabClient.clear(host, port)
  }
}
