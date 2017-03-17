package bda.spark.topic.task

import bda.spark.topic.core.PsStreamLdaModel
import bda.spark.topic.stream.io.{FileReader, StreamReader}
import bda.spark.topic.stream.preprocess.VocabManager
import bda.spark.topic.stream.{StreamLda, StreamLdaLearner}
import com.github.javacliparser._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Roger on 17/3/11.
  */
class localStreamLdaTask extends Task{

  val hostOpt = new StringOption("host", 'h', "the host of redis master", "bda07")
  val portOpt = new IntOption("port", 'p', "the port of redis master", 6379, 0, Int.MaxValue)
  val vocabSizeOpt = new IntOption("vocab", 'V', "the max size of vocabulary",
    1000000, 1, Int.MaxValue)

  val topicNumOpt = new IntOption("topic", 'K', "number of topics",
    10, 1, Int.MaxValue)

  val alphaOpt = new FloatOption("alpha", 'a', "the hyper parameter alpha of LDA", 1, 0, Float.MaxValue)
  val betaOpt = new FloatOption("beta", 'b', "the hyper parameter beta of LDA", 1, 0, Float.MaxValue)
  val rateOpt = new FloatOption("learn_rate", 'r', "the learning rate for Stream LDA", 0.2, 0.0, 0.5)

  val iterOpt = new IntOption("batch_iter", 'i', "the iteration for each batch", 10, 1, Int.MaxValue)
  val durationOpt = new IntOption("duration", 'd', "the duration for each batch", 60000, 10000, Int.MaxValue)

  val streamReaderOpt = new ClassOption("FileReader", 's', "the producer of stream",
    classOf[FileReader], "FileReader")

  val timeOutOpt = new IntOption("time_out", 't',
    "the time out of spark network", 600, 100, Int.MaxValue)

  override def run() {
    val host = hostOpt.getValue
    val port = portOpt.getValue
    val K = topicNumOpt.getValue
    val V = vocabSizeOpt.getValue
    val alpha = alphaOpt.getValue
    val beta = betaOpt.getValue

    val ldaModel = new PsStreamLdaModel(K, V, alpha, beta, rateOpt.getValue,
      host, port, durationOpt.getValue, timeOutOpt.getValue)
    val learner = new StreamLdaLearner(iterOpt.getValue, ldaModel)

    val streamReader = streamReaderOpt.getValue[FileReader]

    val lda = new StreamLda(learner)
    //configuration and initialization of model
    val conf = new SparkConf().setAppName("StreamLda")
    //   conf.setMaster("local[5]")
    conf.set("spark.network.timeout", s"${timeOutOpt.getValue}s")
    conf.set("spark.executor.heartbeatInterval", s"${timeOutOpt.getValue}s")

    val stream = streamReader.getInstances()

        //println(s"train $id")
        //vocabulary manager
        val vocabManager = new VocabManager(ldaModel, s"partition-time")

        val batch = vocabManager.transfrom(stream.toIterator, 0)

    ldaModel.destroy()
  }
}


