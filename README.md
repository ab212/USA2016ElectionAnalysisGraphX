Analysis of the USA election with Apache Spark GraphX
=====================================================

![Fig. 1 - The most popular tweets by Hillary Clinton and Donald Trump after the election has ended](_images/clinton-trump.png)

Fig. 1 - The most popular tweets by Hillary Clinton and Donald Trump after the election has ended

Almost right before the election started, I decided that it might have been interesting to analyse what people think and, more importantly, say on this topic. Because, as you know, this election had promised to be an extraordinary one.

This is when I came up with an idea to utilise Twitter's [streaming API](https://dev.twitter.com/streaming/overview) to continuously save vox populi onto disk for its further analysis. So I had been doing this during a period that spans four days - starting from 7th and up to 10th of November with occasional breaks. When I thought that I already have enough of data for some experiments, an ETL task had been performed and the tweets (which were stored as plain text files) had been turned into a DataFrame compressed with [Parquet](https://parquet.apache.org/). This DataFrame has around ten different fields. But for the purpose of this article only these three were used:

- `id` - id of the tweet
- `text` - contents of the tweet
- `inReplyToStatusId` - id of a tweet the original tweet has responded to

Now we have data to play around with. And as the title of the article suggests I will be using [Apache Spark GraphX](http://spark.apache.org/graphx/) functionality for this purpose. GraphX is one of four components built on top of Spark Core engine and it provides API for processing graphs.

The first step is to prepare data for our graph. So let's create an RDD from the raw DataFrame (file [TwitterGraph.scala](src/main/scala/eugenzyx/TwitterGraph.scala)):

```scala
val englishTweetsRDD =
  inputDataFrame
    .where("lang = \"en\"")    // here I took only tweets in English,
    .map(toTweetSummary)       // mapped them into triples of this format: (id, text, inReplyToStatusId),
    .filter(onlyValidRecords)  // and removed malformed tweets
```

The next reasonable step is to cache the `englishTweetsRDD` RDD in order to avoid repetitive processing of steps in its physical plan, as it is going to be used multiple times:

```scala
englishTweetsRDD.cache()
```

Afterward, we define vertices of the graph:

```scala
val tweetsRDD = englishTweetsRDD map tweetToIdTextPairRDD       // converting tweets to tuples of two elements:
val responsesRDD = englishTweetsRDD map responseToIdTextPairRDD // (id, text)

val vertices = tweetsRDD union responsesRDD                     // and performing a union of the two RDDs
```

And its edges:

```scala
val edges = englishTweetsRDD map extractEdges
```

where `extractEdges` is a function defined in [Transformations.scala](src/main/scala/eugenzyx/Transformations.scala):

```scala
def extractEdges: TweetSummary => Edge[String] = tweetSummary => tweetSummary match {
  case (id, _, inReplyToStatusId) => Edge(id.toLong, inReplyToStatusId.toLong, "Replies")
}
```

Having vertices, edges, and a default vertex we are finally able to define a graph:

```scala
val none = "none" // defining a defaul vertex
val graph = Graph(vertices, edges, none) // defining a graph of tweets
```

This is a directed acyclic graph. It is directed because an edge represents a response to a tweet. And it is acyclic because an earlier tweet can't reply to a tweet that isn't present by the time the first tweet has been created.

Now, what can we do with this graph? Well, let's first find the most popular tweets. In order to do this, I will be using `inDegrees` function, which returns a tuple containing a VertexId and the number of edges pointing to this vertex. Similarly to `inDegrees`, the Graph has also an `outDegrees` function, but in our context, it isn't very helpful since a tweet can only reply to one tweet at most.

```scala
val popularTweetsIds = graph
  .inDegrees
  .sortBy(getCount, descending)
  .take(20)
  .map(getIds)
```

This got us twenty ids of the tweets with the largest number of replies. Now it would be nice to visualise this data. In order to achieve this, there is a `triplets` method. What it does in detail you can read [here](http://spark.apache.org/docs/latest/graphx-programming-guide.html), but in short, it returns an RDD of tuples of three elements containing two vertices and the edge that connects them, like this:

![Fig. 2 - Triplet of two vertices and an edge](_images/triplet.png)

Fig. 2 - Triplet of two vertices and an edge

```scala
val popularTriplets = graph
  .triplets
  .filter(popularTweetsIds contains _.dstId)
```

Now, when we have this data it is finally possible to visualize it. I want this visualization to show relationship between tweets and quickly answer these questions:

- which tweets mention Donald Trump?
- which tweets mention Hillary Clinton?
- which tweets use offensive language?

I will be looking for answers using Neo4j graph database. And again I'm iterating over the triplets here:

```scala
popularTriplets.collect().foreach { triplet =>
  val session = driver.session()

  val query = s"""
    |MERGE (t1: ${ getTweetType(triplet.srcAttr) } {text:'${ sanitizeTweet(triplet.srcAttr) }', id:'${ triplet.srcId }'})
    |MERGE (t2: ${ getTweetType(triplet.dstAttr) } {text:'${ sanitizeTweet(triplet.dstAttr) }', id: '${ triplet.dstId }', isMostPopular: '${ triplet.dstId == mostRepliedTweet }'})
    |CREATE UNIQUE (t1)-[r:REPLIED]->(t2)""".stripMargin

  Try(session.run(query))

  session.close()
}
```

In this query, we create our graph in a local Neo4j database. Pay attention to the `getTweetType` function (its definition can be found in [TweetUtils.scala](src/main/scala/eugenzyx/TweetUtils.scala)). It parses a tweet and determines its category: TrumpTweet, HillaryTweet, ElectionTweet (for those mentioning both Clinton and Trump) or CurseTweet (containing offensive language).


Finally, after the program has been executed, we can observe the following picture ([high-resolution SVG](_images/graph.svg)):

![Fig. 3 - Visualization of the graph](_images/graph.png)

Fig. 3 - Visualization of the graph

![Fig. 4 - Legend for the graph on Fig. 3](_images/legend.png)

Fig. 4 - Legend for the graph on Fig. 3

So here you can see tweets that mention Donald Trump marked in red, Hillary Clinton - in Blue, both - in yellow, and tweets containing offensive language in green. Any other tweets are grey, as they interest us the least.

The other thing we can find on the graph is the most popular tweet over that period. And it is [this](https://twitter.com/realDonaldTrump/status/796315640307060738) tweet sent by Donald Trump as, by that time already, the president-elect of the USA. An interesting thing to note is the amount of obscene language used in response to this tweet - 13. In comparison, the most popular [tweet](https://twitter.com/HillaryClinton/status/796169187882369024) by Hillary Clinton has only 3 curses in response.

Let's calculate what fraction of all the amount of responses do obscene tweets make up and display this on the graph:

```scala
  val trumpMostPopular = 796315640307060738L // id of the most popular tweets were taken from the graph's visual representation
  val clintonMostPopular = 796169187882369024L

  val trumpTotalRepliesCount = popularTriplets.filter(triplet => triplet.dstId == trumpMostPopular).count
  val clintonTotalRepliesCount = popularTriplets.filter(triplet => triplet.dstId == clintonMostPopular).count

  val trumpOffensiveRepliesCount = popularTriplets.filter(triplet => triplet.dstId == trumpMostPopular && isCurseTweet(triplet.srcAttr)).count
  val clintonOffensiveRepliesCount = popularTriplets.filter(triplet => triplet.dstId == clintonMostPopular && isCurseTweet(triplet.srcAttr)).count

  println(s"Total replies to Trump's most popular tweet: $trumpTotalRepliesCount, number of tweets containing curses: $trumpOffensiveRepliesCount, ratio: ${ trumpOffensiveRepliesCount.toFloat / trumpTotalRepliesCount }")
  println(s"Total replies to Clinton's most popular tweet: $clintonTotalRepliesCount, number of tweets containing curses: $clintonOffensiveRepliesCount, ratio: ${ clintonOffensiveRepliesCount.toFloat / clintonTotalRepliesCount }")
```

And the output is:

```
Total replies to Trump's most popular tweet: 183, number of tweets containing curses: 13, ratio: 0.07103825
Total replies to Clinton's most popular tweet: 78, number of tweets containing curses: 3, ratio: 0.03846154
```

So what can we conclude from these results? I suppose, it is that there were more people who were mad about Trump's victory than Clinton's defeat. Keep in mind, however, that twitter's streaming API only outputs 5% of all the tweets that are posted. On top of that, I'll remind you that there were times when I stopped the streaming process for some time and I'm afraid I missed the most interesting part of that day.
