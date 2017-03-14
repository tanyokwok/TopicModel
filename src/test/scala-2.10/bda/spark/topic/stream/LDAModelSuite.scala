package bda.spark.topic.stream

import bda.spark.topic.core.OnlineLdaModel
import org.scalatest.FunSuite

import scala.collection.mutable

/**
  * Created by Roger on 17/2/21.
  */
class LDAModelSuite extends FunSuite{
  test("OnlineLDAModel1"){
    val k = 10
    val alpha = 1
    val beta = 1
    val model = new OnlineLdaModel(0, k, alpha, beta,
      Array.fill[Int](k)(alpha), Map()
    )

    assert( model.K == k)
    assert( model.alpha == alpha)
    assert( model.beta == beta)
    assert( model.prioriAlphaStatsVector.sum == k * alpha )
    assert( model.prioriBetaStatsVector.sum == 0 )
  }

  test("OnlineLDAModel2"){
    val k = 10
    val alpha = 1
    val beta = 1
    val model = new OnlineLdaModel(0, k, alpha, beta,
      Array.fill[Int](k)(alpha),
      Map( "a" -> Array.fill[Int](k)(beta),
        "b" -> Array.fill[Int](k)(beta),
        "c" -> Array.fill[Int](k)(beta),
        "d" -> Array.fill[Int](k)(beta))
    )

    assert( model.K == k)
    assert( model.alpha == alpha)
    assert( model.beta == beta)
    assert( model.prioriAlphaStatsVector.sum == k * alpha )
    assert( model.prioriBetaStatsVector.sum == k*beta*4 )
    assert( model.prioriBetaStatsVector(0) == 4 )
  }

}
