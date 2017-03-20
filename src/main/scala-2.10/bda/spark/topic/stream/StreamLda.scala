package bda.spark.topic.stream

import bda.spark.topic.core.{Instance, TextDocInstance}
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
    val timer = new Timer()
    val id = s"RDD-${data.id}+time-$time"
    val vocabManager = new VocabManager(lda.model, id)
    logInfo(s"[VocabManager-$time] build vocabulary at time ${timer.getReadableRunnningTime()}")

    if (lda.model.setStage(time, lda.model.STAGE_RUNING)) {
      val word2id = vocabManager.buildVocab(data, time)
      logInfo(s"[VocabManager-$time] build vocabulary success at time ${timer.getReadableRunnningTime()}")
      lda.model.lazySyncParameter(word2id.map(_._2).toArray, time, id)

      logInfo(s"[StreamLda-$id] begin transform at time ${timer.getReadableRunnningTime()}")
      val batch = vocabManager.transfrom(data, time)
      logInfo(s"[StreamLda-$id] end transform at time ${timer.getReadableRunnningTime()}")

      val assign = batch.mapPartitionsWithIndex {
        case (index, instancesInPartition) =>
          lda.update(instancesInPartition.toSeq, id + s"-$index", time, word2id).toIterator
      }

      logInfo(s"[StreamLda-$id] begin decode at time ${timer.getReadableRunnningTime()}")
      val ret = vocabManager.decode(assign)
      logInfo(s"[StreamLda-$id] end decode at time ${timer.getReadableRunnningTime()}")
      logInfo(s"[StreamLda-$id] begin release at time ${timer.getReadableRunnningTime()}")
      ret.cache()
      ret.count()

      vocabManager.relaseUsage()
      logInfo(s"[StreamLda-$id] end release at time ${timer.getReadableRunnningTime()}")

      report()

      lda.model.setStage(time, lda.model.STAGE_FINISH)
      ret
    } else {
      null
    }
  }

  def train(data: DStream[Instance]): DStream[Instance] = {

    data.transform{
      (rdd, time) =>
        val ret: RDD[Instance] = train(rdd, time.milliseconds)

        ret
    }
  }

  def report(): Unit = {
    lda.model.topicWords(20).foreach {
      x =>
        val report = x.toArray.sortBy(_._2).reverse.map(x => (x._1, x._2.toInt))

        println("================================")
        println(report.mkString(" "))
        println(report.slice(5, 15).map(_._1).mkString(" "))
        println("================================")
    }
  }

}
