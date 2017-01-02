package xyz.eugenzyx

trait TweetUtils {
  type Tweet = String

  def sanitizeTweet(t: Tweet): Tweet = t.replaceAll("'", "")

  def isTrumpTweet(implicit t: Tweet): Boolean = {
    val td = t.toLowerCase

    td.contains("donald") || td.contains("trump")
  }

  def isHillaryTweet(implicit t: Tweet): Boolean = {
    val td = t.toLowerCase

    td.contains("hillary") || td.contains("clinton")
  }

  def isCurseTweet(implicit t: Tweet): Boolean = {
    val td = t.toLowerCase

    curseWords.find(w => t contains w.toLowerCase) match {
      case Some(_) => true
      case None => false
    }
  }

  def getTweetType(implicit t: Tweet): String =
    if (isCurseTweet) "CurseTweet"
    else if (isTrumpTweet && isHillaryTweet) "ElectionTweet"
    else if (isTrumpTweet) "TrumpTweet"
    else if (isHillaryTweet) "HillaryTweet"
    else "Tweet"

  // special thanks to http://www.slate.com/blogs/lexicon_valley/2013/09/11/top_swear_words_most_popular_curse_words_on_facebook.html
  val curseWords = List("shit",
                        "fuck",
                        "damn",
                        "bitch",
                        "crap",
                        "piss",
                        "dick",
                        "darn",
                        "cock",
                        "pussy",
                        "asshole",
                        "fag",
                        "bastard",
                        "slut",
                        "douche")
}
