package bda.spark.topic.stream

import bda.spark.topic.core.Instance
import bda.spark.topic.redis.RedisVocabClient
import bda.spark.topic.stream.preprocess.VocabManager
import bda.spark.topic.utils.Timer
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Roger on 17/3/10.
  */
class StreamLda(val lda: StreamLdaLearner) extends Serializable with Logging{

  val maxVocabSize: Long = lda.model.V

  private def train(data: RDD[Instance], time: Long): RDD[Instance] = {
    data.mapPartitionsWithIndex{
      case (index, instancesInPartition) =>
        val timer = new Timer()
        val id = s"partition-$index at time-$time"
        //logInfo(s"train $id")
        //vocabulary manager
        val vocabManager = new VocabManager(lda.model, s"partition-$index+time-$time")

       // logInfo(s"$id: transfrom data ")
        logInfo(s"[StreamLda-$id] begin transform at time ${timer.getReadableRunnningTime()}")
        //logInfo("text: " + instancesInPartition.take(10).mkString(" "))
        //get batch train data
        val batch = vocabManager.transfrom(instancesInPartition, time)

        logInfo(s"[StreamLda-$id] begin update at time ${timer.getReadableRunnningTime()}")
       // logInfo(s"$id: update lda model")
        //asynchronized update lda model

        val assign = lda.update(batch, id, time, vocabManager.word2id)
       // logInfo(s"$id: decode topic assign")
        logInfo(s"[StreamLda-$id] begin decode at time ${timer.getReadableRunnningTime()}")
        val ret = vocabManager.decode(assign)

        logInfo(s"[StreamLda-$id] begin release at time ${timer.getReadableRunnningTime()}")
        //release the usage of words
       // logInfo(s"$id: release usage")
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
