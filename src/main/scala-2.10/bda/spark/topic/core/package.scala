package bda.spark.topic

/**
  * Created by Roger on 17/2/22.
  */
package object core {
  type TextDoc = Seq[(String,Int)]
  type IdDoc = Seq[(Long, Int)]
}
