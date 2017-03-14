package bda.spark.topic.redis

import java.util
import java.util.concurrent.Executors

import bda.spark.topic.core.{PsLdaModel, PsStreamLdaModel}
import bda.spark.topic.glint.Glint
import bda.spark.topic.stream.preprocess.{Formatter, VocabManager}

import collection.JavaConversions._
import scala.util.Random

/**
  * Created by Roger on 17/3/10.
  */
object VocabManagerSuite {
  val vocabSize =40L
  val ldaModel = new PsStreamLdaModel(10 , vocabSize, 1, 1, "localhost", 30001)
  def example(time: Long, id : Int): Unit = {

    //println(s"Running at time : $time ")
    val data = Array(Array("hello world", "hello hello world",
      "程序 中 我们 创建 了 固定 大小 为 五个 工作 线程 的 线程 池 。",
      "然后 分配 给 线程 池 十个 工作 ， 因为 线程 池 大小 为 五 ，"),
      Array(
      "它 将 启动 五个 工作 线程 先 处理 五个 工作 ， 其他 的 工作 则 处于 等待 状态 ， ",
      "一旦 有 工作 完成 ， 空闲 下来 工作 线程 就会 捡取 等待 队列 里 的 其他 工作 进行 执行 。",
      "这里 是 以上 程序 的 输出 。 "))

    val formatter = new Formatter()

    val vocabManager = new VocabManager(ldaModel, s"thread-$id-$time")
    val input = data( Math.abs(Random.nextInt() % 2 )).map(formatter.format(_))
    val output = vocabManager.transfrom(input.toIterator, time)
    println(s"Thread $id wait for 2s")
    Thread.sleep(2000)
   /* output.foreach {
      line =>
        println( "line " + line)
    }*/
    vocabManager.relaseUsage()
  }

  def main(args: Array[String]) {

    example(0, 0)
    class TestThread(time: Long, id: Int) extends Runnable{
      override def run(): Unit = {
        example(time, id)
        println(s"exit $id")
      }
    }

    var counter = 0
    val pool = Executors.newFixedThreadPool(5)
    Range(1, 11).foreach{
      k =>
        val thread = new TestThread(k % ( 1 + Math.abs(Random.nextInt() % 10) ), k)
        pool.execute(thread)
        counter += 1
    }
    pool.shutdown()

    while (!pool.isTerminated) {
      Thread.sleep(100)
    }

    ldaModel.destroy()
    val allCounters: util.Map[String, String] = ldaModel.jedis.hgetAll("lda.vocab.count")

    println(allCounters.toMap.mkString(" "))
    assert( allCounters.toMap.map(x=>x._2.toInt).filter( _ != 0 ).size == 0)

  }
}
