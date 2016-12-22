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
      .filter(popularTweetsIds contains _.dstId)

    popularTriplets.cache()

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

    englishTweetsRDD.unpersist() // we don't need this anymore

    val trumpMostPopular = 796315640307060738L // id of the most popular tweets were taken from the graph
    val clintonMostPopular = 796169187882369024L

    val trumpTotalRepliesCount = popularTriplets.filter(triplet => triplet.dstId == trumpMostPopular).count
    val clintonTotalRepliesCount = popularTriplets.filter(triplet => triplet.dstId == clintonMostPopular).count

    val trumpOffensiveRepliesCount = popularTriplets.filter(triplet => triplet.dstId == trumpMostPopular && isCurseTweet(triplet.srcAttr)).count
    val clintonOffensiveRepliesCount = popularTriplets.filter(triplet => triplet.dstId == clintonMostPopular && isCurseTweet(triplet.srcAttr)).count

    println(s"Total replies to Trump's most popular tweet: $trumpTotalRepliesCount, number of tweets containing curses: $trumpOffensiveRepliesCount, ratio: ${ trumpOffensiveRepliesCount.toFloat / trumpTotalRepliesCount }")
    println(s"Total replies to Clinton's most popular tweet: $clintonTotalRepliesCount, number of tweets containing curses: $clintonOffensiveRepliesCount, ratio: ${ clintonOffensiveRepliesCount.toFloat / clintonTotalRepliesCount }")
  }
}
