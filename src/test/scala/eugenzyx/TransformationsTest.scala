package xyz.eugenzyx

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge

import org.scalatest.{Assertions, BeforeAndAfter, FunSuite}

class TransformationsTest
    extends FunSuite
    with BeforeAndAfter
    with SharedSparkContext
    with Transformations {

  var inRDD: RDD[TweetSummary] = _

  before {
    inRDD = sc.parallelize(List(("1", "text", "2")))
  }

  test("tweetToIdTextPairRDD") {
    val expectedRDD: RDD[(Long, String)] = sc.parallelize(List((1, "text")))

    val resultRDD: RDD[(Long, String)] = inRDD.map(tweetToIdTextPairRDD)

    assert(None === RDDComparisons.compare(expectedRDD, resultRDD))
  }

  test("responseToIdTextPairRDD") {
    val expectedRDD: RDD[(Long, String)] = sc.parallelize(List((2, "")))

    val resultRDD: RDD[(Long, String)] = inRDD.map(responseToIdTextPairRDD)

    assert(None === RDDComparisons.compare(expectedRDD, resultRDD))
  }

  test("extractEdges") {
    val expectedRDD: RDD[Edge[String]] =
      sc.parallelize(List(Edge(1, 2, "Replies")))

    val resultRDD: RDD[Edge[String]] = inRDD map extractEdges

    assert(None === RDDComparisons.compare(expectedRDD, resultRDD))
  }

  test("onlyValidRecords") {
    val inRDD: RDD[TweetSummary] = sc.parallelize(
      List(
        ("1", "text", "2"),
        ("er", "r", "or"),
        ("2", "text", "three")
      ))

    val expectedRDD: RDD[TweetSummary] =
      sc.parallelize(List(("1", "text", "2")))

    val resultRDD: RDD[TweetSummary] = inRDD.filter(onlyValidRecords)

    assert(None === RDDComparisons.compare(expectedRDD, resultRDD))
  }
}
