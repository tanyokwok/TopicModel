package bda.spark.topic.linalg

import breeze.linalg.{*, DenseMatrix, DenseVector}

/**
  * Created by Roger on 17/2/26.
  */
object Utils {

  def foldVectors(mat: Iterable[DenseVector[Int]], K: Int): DenseVector[Int] = {
    mat.fold(DenseVector.fill[Int](K)(0)) {
      (ent1, ent2) =>
        ent1 + ent2
    }
  }

  def foldVectors(mat: DenseMatrix[Int]): DenseVector[Int] = {
    val K = mat.cols
    val vec = DenseVector.fill[Int](K)(0)

    mat(*, ::).foreach {
      row =>
        vec += row
    }
    vec
  }

}
