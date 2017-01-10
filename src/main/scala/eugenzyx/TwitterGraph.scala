package xyz.eugenzyx

import scala.util.Try

import org.apache.spark.graphx.Graph
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import org.neo4j.driver.v1._

object TwitterGraph extends TweetUtils with Transformations {
  def main(args: Array[String]) {
    require(!args.isEmpty, "A path to tweets should be specified")

    val sparkConf = new SparkConf().setAppName("TwitterGraph")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val inputDataFrame = sqlContext.read.load(args(0)) // path to Tweets.parquet

    val englishTweetsRDD = inputDataFrame
      .where("lang = \"en\"")
      .map(toTweetSummary)
      .filter(onlyValidRecords)

    englishTweetsRDD.cache()

    val tweetsRDD = englishTweetsRDD.map(tweetToIdTextPairRDD)
    val responsesRDD = englishTweetsRDD.map(responseToIdTextPairRDD)

    val vertices = tweetsRDD.union(responsesRDD)
    val edges = englishTweetsRDD.map(extractEdges)
    val none = "none" // defining a defaul vertex
    val graph = Graph(vertices, edges, none) // defining a graph of tweets

    graph.cache()

    val popularInDegrees = graph // this will be used later
    .inDegrees
      .sortBy(getCount, descending)
      .take(20)

    val popularTweetsIds = popularInDegrees.map(getIds)

    val popularTriplets = graph.triplets
      .filter(triplet => popularTweetsIds.contains(triplet.dstId))

    popularTriplets.cache()

    val mostRepliedTweet = popularTweetsIds.head // tweet with maximum number of replies

    val driver = GraphDatabase.driver("bolt://localhost/",
                                      AuthTokens.basic("neo4j", "admin"))

    popularTriplets.collect().foreach { triplet =>
      val session = driver.session()

      val query =
        s"""
        |MERGE (t1: ${getTweetType(triplet.srcAttr)} {text:'${sanitizeTweet(
             triplet.srcAttr)}', id:'${triplet.srcId}'})
        |MERGE (t2: ${getTweetType(triplet.dstAttr)} {text:'${sanitizeTweet(
             triplet.dstAttr)}', id: '${triplet.dstId}', isMostPopular: '${triplet.dstId == mostRepliedTweet}'})
        |CREATE UNIQUE (t1)-[r:REPLIED]->(t2)""".stripMargin

      Try(session.run(query))

      session.close()
    }
    driver.close()

    val mentions =
      popularTriplets
        // show mentions of the candidates for every tweet
        .map { triplet =>
          implicit def booleanToInt(b: Boolean): Int = if (b) 1 else 0

          (triplet.dstId, (isTrumpTweet(triplet.srcAttr): Int, isHillaryTweet(triplet.srcAttr): Int))
        }
        .reduceByKey { case ((l1, l2), (r1, r2)) => (l1 + r1, l2 + r2) } // sum up mentions
        .collect()

    val trumpMostPopular = mentions.maxBy { case (id, (trumpCount, clintonCount)) => trumpCount }._1
    val clintonMostPopular = mentions.maxBy { case (id, (trumpCount, clintonCount)) => clintonCount }._1

    val trumpTotalRepliesCount = popularTriplets
      .filter(triplet => triplet.dstId == trumpMostPopular)
      .count
    val clintonTotalRepliesCount = popularTriplets
      .filter(triplet => triplet.dstId == clintonMostPopular)
      .count

    val trumpOffensiveRepliesCount = popularTriplets
      .filter(triplet =>
        triplet.dstId == trumpMostPopular && isCurseTweet(triplet.srcAttr))
      .count
    val clintonOffensiveRepliesCount = popularTriplets
      .filter(triplet =>
        triplet.dstId == clintonMostPopular && isCurseTweet(triplet.srcAttr))
      .count

    println(
      s"Total replies to Trump's most popular tweet: $trumpTotalRepliesCount, number of tweets containing curses: $trumpOffensiveRepliesCount, ratio: ${trumpOffensiveRepliesCount.toFloat / trumpTotalRepliesCount}")
    println(
      s"Total replies to Clinton's most popular tweet: $clintonTotalRepliesCount, number of tweets containing curses: $clintonOffensiveRepliesCount, ratio: ${clintonOffensiveRepliesCount.toFloat / clintonTotalRepliesCount}")

    val popularPageRank = graph
      .staticPageRank(10)
      .vertices
      .sortBy(getRank, descending)
      .take(20)

    popularInDegrees
      .zip(popularPageRank) // zip the results that we have got from two approaches
      .foreach {
        case ((l, _), (r, _)) =>
          println(s"$l $r ${if (l == r) "" else "!"}") // print out ids of the tweets and append an ! if they are not equal
      }

    val replies = englishTweetsRDD
      // take tweets that are replies:
      .filter(!_._3.isEmpty)
      // get their ids:
      .map(_._1)
      .collect()
      .toSet

    val repliesWithRepliesIds = englishTweetsRDD
      // get tweets that reply to replies:
      .filter {
        case (_, _, inReplyToStatusId) => replies(inReplyToStatusId.toString)
      }
      // get ids of first level replies:
      .map(_._3.toLong)
      .collect()
      .toSet

    graph
      // get a collection of tuples (VertexId, NumberIncomingEdges):
      .inDegrees
      // get only replies with replies:
      .filter { case (id, _) => repliesWithRepliesIds(id) }
      // sort by number of incoming edges:
      .sortBy(getCount, descending)
      // take top 20 and print them out:
      .take(20)
      .foreach(println)
  }
}
