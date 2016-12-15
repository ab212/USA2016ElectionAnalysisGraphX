package xyz.eugenzyx

import scala.util.Try

import org.apache.spark.graphx.Graph
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import org.neo4j.driver.v1._

object TwitterGraph extends TweetUtils with Transformations {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("TwitterGraph")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val inputDataFrame = sqlContext.read.load( args(0) ) // path to Tweets.parquet

    val englishTweetsRDD =
      inputDataFrame
        .where("lang = \"en\"")
        .map(toTweetSummary)
        .filter(onlyValidRecords)

    englishTweetsRDD.cache()

    val tweetsRDD = englishTweetsRDD map tweetToIdTextPairRDD
    val responsesRDD = englishTweetsRDD map responseToIdTextPairRDD

    val vertices = tweetsRDD union responsesRDD
    val edges = englishTweetsRDD map extractEdges
    val none = "none" // defining a defaul vertex
    val graph = Graph(vertices, edges, none) // defining a graph of tweets

    val popularTweetsIds = graph
      .inDegrees
      .sortBy(getCount, descending)
      .take(20)
      .map(getIds)

    val popularTriplets = graph
      .triplets
      .filter(triplet => popularTweetsIds.contains(triplet.dstId))

    val mostRepliedTweet = popularTweetsIds.head // tweet with maximum number of replies

    val driver = GraphDatabase.driver("bolt://localhost/", AuthTokens.basic("neo4j", "admin"))

    popularTriplets.collect().foreach { triplet =>
      val session = driver.session()

      val query = s"""
        |MERGE (t1: ${ getTweetType(triplet.srcAttr) } {text:'${ sanitizeTweet(triplet.srcAttr) }', id:'${ triplet.srcId }'})
        |MERGE (t2: ${ getTweetType(triplet.dstAttr) } {text:'${ sanitizeTweet(triplet.dstAttr) }', id: '${ triplet.dstId }', isMostPopular: '${ triplet.dstId == mostRepliedTweet }'})
        |CREATE UNIQUE (t1)-[r:REPLIED]->(t2)""".stripMargin

      Try(session.run(query))

      session.close()
    }
    driver.close()
  }
}
