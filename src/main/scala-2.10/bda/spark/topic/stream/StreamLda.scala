package bda.spark.topic.stream

import bda.spark.topic.core.Instance
import bda.spark.topic.redis.RedisVocabClient
import bda.spark.topic.stream.preprocess.VocabManager
import bda.spark.topic.utils.Timer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Roger on 17/3/10.
  */
class StreamLda(val lda: StreamLdaLearner) extends Serializable{

  val maxVocabSize: Long = lda.model.V

  private def train(data: RDD[Instance], time: Long): RDD[Instance] = {
    data.mapPartitionsWithIndex{
      case (index, instancesInPartition) =>
        val timer = new Timer()
        val id = s"partition-$index at time-$time"
        //println(s"train $id")
        //vocabulary manager
        val vocabManager = new VocabManager(lda.model, s"partition-$index+time-$time")

       // println(s"$id: transfrom data ")
        println(s"[StreamLda-$id] begin transform at time ${timer.getReadableRunnningTime()}")
        //println("text: " + instancesInPartition.take(10).mkString(" "))
        //get batch train data
        val batch = vocabManager.transfrom(instancesInPartition, time)

        println(s"[StreamLda-$id] begin update at time ${timer.getReadableRunnningTime()}")
       // println(s"$id: update lda model")
        //asynchronized update lda model

        val assign = lda.update(batch, id, time, vocabManager.word2id)
       // println(s"$id: decode topic assign")
        println(s"[StreamLda-$id] begin decode at time ${timer.getReadableRunnningTime()}")
        val ret = vocabManager.decode(assign)

        println(s"[StreamLda-$id] begin release at time ${timer.getReadableRunnningTime()}")
        //release the usage of words
       // println(s"$id: release usage")
        vocabManager.relaseUsage()
        ret.toIterator
    }
  }

  def train(data: DStream[Instance]): DStream[Instance] = {

    data.transform{
      (rdd, time) =>
        val ret: RDD[Instance] = train(rdd, time.milliseconds)
        ret
    }
  }

}
