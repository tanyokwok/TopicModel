/*
 * Copyright (C) 2015 Holmes Team at HUAWEI Noah's Ark Lab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package bda.spark.topic.task

import bda.spark.topic.core._
import bda.spark.topic.local.io.{LocalWriter, SimpleFileReader}
import bda.spark.topic.redis.RedisVocabClient
import bda.spark.topic.stream.DistributedParallelLDA
import bda.spark.topic.utils.Timer
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import scala.collection.mutable


/**
  * Created by Roger on 17/2/22.
  */
class DistributedParallelLDATask extends Serializable{

  def run(sc: SparkContext, train_file:String, test_file:String, iteration: Int, buffsize: Int, semaphore: Int): Unit = {

    val reader: SimpleFileReader = new SimpleFileReader(1000, train_file)
    val testReader: SimpleFileReader = new SimpleFileReader(1000, test_file)

    val host = "bda07"
    val port = 30001
    val maxVocabSize = 10000000L

    val timer = new Timer()

    RedisVocabClient.clear(host, port)
    def buildVocab(examples: Seq[Example]): mutable.LinkedHashMap[String, Long] ={
      println("Building vocab for partition " + timer.getReadableRunnningTime())

      val vocab = examples.map{
        example =>
          val textDocInstance = example.instance.asInstanceOf[TextDocInstance]
          val words = textDocInstance.tokens.map(_._1).distinct
          words
      }.flatMap(_.toSeq).distinct
      val redisVocabClient = RedisVocabClient(host, port, maxVocabSize)
      val word2id = mutable.LinkedHashMap[String, Long]() // thread safe
      vocab.foreach{
        word =>
          val wid = redisVocabClient.getTerm(word, 0L)
          word2id += ((word, wid))
      }
      redisVocabClient.close()

      println("Building vocab for partition success at " + timer.getReadableRunnningTime())
      word2id
    }

    def mapText2IdDocFunc(examplesInPartition: Iterator[Example]): Iterator[Example] = {
      println("Extracting word set from training set" + timer.getReadableRunnningTime())
      val examples = examplesInPartition.toSeq
      val word2id = buildVocab(examples)
      val ret = examples.map { // Interator 只能遍历一遍
        example =>
          val textDocInstance = example.instance.asInstanceOf[TextDocInstance]

          val idTokens: Seq[(Long, Int)] = textDocInstance.tokens.map{
            case (term, topic) =>
              val wid: Long = word2id(term)
              (wid, topic)
          }
          new Example(new IdDocInstance(idTokens))
      }
      ret.toIterator
    }

    val source=  sc.parallelize(reader.getAllExamples()).repartition(10)
    val examples: RDD[Example] = source.mapPartitions(mapText2IdDocFunc)

    examples.cache()

    val V: Long = RedisVocabClient.vocabSize(host, port)
    println(V)
    val learner = new DistributedParallelLDA(V, 10, 1,1, buffsize, semaphore)
    learner.train(examples, null, iteration)
    println("End Training")
  }


}

/**
 * The main entry point for testing StreamDM by running tasks on Spark
 * Streaming.
 */
object DistributedParallelLDATask {

  def main(args: Array[String]) {

    //configuration and initialization of model
    val conf = new SparkConf().setAppName("DistributedParallelLDA")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val task = new DistributedParallelLDATask()
    //run task
    if (args.length > 4) {
      task.run(sc, args(0), args(1), args(2).toInt, args(3).toInt, args(4).toInt)
    }
    else {
      //task.run("/home/gty/data/economy_sent_docs_2016_mini")
      task.run(sc, "data/economy_sent_docs_2016_mini", "data/economy_sent_docs_2016_test", 100, 12800, 10)
      //task.run("data/train_data")
    }
    //start the loop
  }
}

