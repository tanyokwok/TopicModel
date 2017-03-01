package bda.spark.topic.utils

/**
  * Created by Roger on 17/2/24.
  */
class Timer {

  val startTime = System.currentTimeMillis()

  def getRunningTime(): Long ={
    System.currentTimeMillis() - startTime
  }

  def getReadableRunnningTime(): String ={
    val runningTime = getRunningTime()
    val hh = runningTime / (1000 * 60 * 60)
    val mm = runningTime % (1000 * 60 * 60) / (1000 * 60)
    val ss = (runningTime % (1000 * 60)) / 1000.0

    s"$hh:$mm:$ss"
  }
}
