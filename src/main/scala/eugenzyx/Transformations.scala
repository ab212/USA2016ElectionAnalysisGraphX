package xyz.eugenzyx

import org.apache.spark.graphx.{Edge, VertexId}

import org.apache.spark.sql.Row

trait Transformations {
  type TweetSummary = (String, String, String)

  def tweetToIdTextPairRDD(tuple: TweetSummary): (Long, String) = tuple match {
    case (id, text, _) => (id.toLong, text)
  }

  def responseToIdTextPairRDD(tuple: TweetSummary): (Long, String) = tuple match {
    case (_, _, inReplyToStatusId) => (inReplyToStatusId.toLong, "")
  }

  def extractEdges(tuple: TweetSummary): Edge[String] = tuple match {
    case (id, _, inReplyToStatusId) => Edge(id.toLong, inReplyToStatusId.toLong, "Replies")
  }

  def toTweetSummary(row: Row): TweetSummary = (row.getAs[String]("id"), row.getAs[String]("text"), row.getAs[String]("inReplyToStatusId"))

  def onlyValidRecords(tweetSummary: TweetSummary): Boolean = tweetSummary match {
    case (id, _, inReplyToStatusId) =>
      List(id, inReplyToStatusId)
        .foldLeft(true)((start, value) =>
            start && value.forall(_.isDigit)) // filter out corrupted tuples
  }

  def getCount(tuple: (VertexId, Int)): Int = tuple match {
    case (_, count) => count
  }

  def getIds(tuple: (VertexId, Int)): Int = tuple match {
    case (id, _) => id
  }

  val descending = false
}
