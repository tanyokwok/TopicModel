package bda.spark.topic.linalg

import breeze.linalg.{DenseMatrix, DenseVector}
import org.scalatest.FunSuite

/**
  * Created by Roger on 17/2/26.
  */
class UtilsSuite extends FunSuite{

  test("TestFoldVectors(Iterable)"){
    val vecs = Array(
      DenseVector(1,2,3,4),
      DenseVector(1,1,3,4)
    )

    assert( Utils.foldVectors(vecs, 4).toArray.mkString(" ") == "2 3 6 8")
  }

  test("TestFoldVectors(DenseMatrix)"){
    val vecs = DenseMatrix(
      (1,2,3,4),
      (1,1,3,4)
    )


    assert( Utils.foldVectors(vecs).toArray.mkString(" ") == "2 3 6 8")
  }
}
