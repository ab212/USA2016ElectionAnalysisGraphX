package xyz.eugenzyx

import org.apache.spark.graphx.{Edge, VertexId}

import org.apache.spark.sql.Row

trait Transformations {
  type TweetSummary = (String, String, String)
  type VertexWithCount = (VertexId, Int)
  type VertexWithRank = (VertexId, Double)

  def tweetToIdTextPairRDD: TweetSummary => (Long, String) =
    tweetSummary =>
      tweetSummary match {
        case (id, text, _) => (id.toLong, text)
    }

  def responseToIdTextPairRDD: TweetSummary => (Long, String) =
    tweetSummary =>
      tweetSummary match {
        case (_, _, inReplyToStatusId) => (inReplyToStatusId.toLong, "")
    }

  def extractEdges: TweetSummary => Edge[String] =
    tweetSummary =>
      tweetSummary match {
        case (id, _, inReplyToStatusId) =>
          Edge(id.toLong, inReplyToStatusId.toLong, "Replies")
    }

  def toTweetSummary: Row => TweetSummary =
    row =>
      (row.getAs[String]("id"),
       row.getAs[String]("text"),
       row.getAs[String]("inReplyToStatusId"))

  def onlyValidRecords: TweetSummary => Boolean =
    tweetSummary =>
      tweetSummary match {
        case (id, _, inReplyToStatusId) =>
          List(id, inReplyToStatusId)
            .foldLeft(true)((start, value) => start && value.forall(_.isDigit)) // filter out corrupted tuples
    }

  def getCount: VertexWithCount => Int =
    tuple => tuple match { case (_, count) => count }

  def getRank: VertexWithRank => Double =
    tuple => tuple match { case (_, rank) => rank }

  def getIds: VertexWithCount => VertexId =
    tuple => tuple match { case (id, _) => id }

  val descending = false
}
