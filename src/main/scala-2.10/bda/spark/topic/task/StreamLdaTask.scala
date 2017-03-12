package bda.spark.topic.task

import bda.spark.topic.core.PsStreamLdaModel
import bda.spark.topic.stream.{StreamLda, StreamLdaLearner}
import bda.spark.topic.stream.io.{FileReader, StreamReader}
import bda.spark.topic.stream.preprocess.Formatter
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Roger on 17/3/11.
  */
object StreamLdaTask {

  def main(args: Array[String]): Unit = {

    val formatter = new Formatter()
    val streamReader = new FileReader(10000, 10000, "data/economy_sent_docs_2016_mini", formatter) // ms
    println("Build lda model")
    val ldaModel = new PsStreamLdaModel(10, 10000000L, 1, 1)
    val learner = new StreamLdaLearner(10, ldaModel)

    val host = "bda07"
    val port = 30001

    println("Build Stream LDA")
    val lda = new StreamLda(host, port, learner)
   //configuration and initialization of model
    val conf = new SparkConf().setAppName("streamDM")
    conf.setMaster("local[5]")

    println("Build stream context")
    val ssc = new StreamingContext(conf, Seconds(10))

    println("Build stream reader")
    val stream = streamReader.getInstances(ssc)

    stream.cache()

    println("Do lda train")
    val output = lda.train(stream)

    output.foreachRDD{
      rdd =>
        println(s"Output topic assign at ${rdd.id}, size : ${rdd.count()}")
        rdd.take(10).foreach(println)
        println("Output topic assign END")
    }
    ssc.start()
    ssc.awaitTermination()
    ldaModel.destroy()
  }
}
