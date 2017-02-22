package bda.spark.stream.topic

import org.scalatest.FunSuite

import scala.collection.mutable

/**
  * Created by Roger on 17/2/21.
  */
class LDAModelSuite extends FunSuite{

  test("OnlineLDAModel"){
    val k = 10
    val alpha = 1
    val beta = 1
    val model = new OnlineLDAModel(k, alpha, beta,
      Array.fill[Int](k)(alpha),
      mutable.Map( "a" -> Array.fill[Int](k)(beta),
        "b" -> Array.fill[Int](k)(beta),
        "c" -> Array.fill[Int](k)(beta),
        "d" -> Array.fill[Int](k)(beta))
    )

    assert( model.K == k)
    assert( model.ALPHA == alpha)
    assert( model.BETA == beta)
    assert( model.prioriAlphaStatsVector.sum == k * alpha )
    assert( model.prioriBetaStatsVector.sum == k*beta*4 )
    assert( model.prioriBetaStatsVector()(0) == 4 )
  }

}
