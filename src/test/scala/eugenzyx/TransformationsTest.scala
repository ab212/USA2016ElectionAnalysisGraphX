package xyz.eugenzyx

import com.holdenkarau.spark.testing.SharedSparkContext

import org.scalatest.Assertions
import org.scalatest.FunSuite

class TransformationsTest extends FunSuite with SharedSparkContext {

  test("test") {
    val list = List(1, 2, 3, 4)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)
  }
}
