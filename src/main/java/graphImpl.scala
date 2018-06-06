import ml.sparkling.graph.operators.algorithms.community.pscan._
import org.apache.spark.graphx._
import com.mongodb.spark._
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.{LDA, OnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import com.mongodb.casbah.Imports._
import scala.util.control.Breaks._

import Utilities._


object graphImpl{

  //from the vertices Lists I make an edges List
  // of tuple (origin: Long(VertexId), destination: Long(VertexId))
  def makeFollowerEdges(userList: List[String]): List[Edge[Int]]  = {

    //create a mutable list to add data
    var edges = new ListBuffer[Edge[Int]]
    //first element of the list is the userid in mongo
    val destination = userList.head.toLong

    //println("Geting the followers of user:  " + destination)
    //remove the first user in userlist leaves only the followers
    val followers = userList.filter(_ != userList.head)
    //make tuples and add to mutable list
    //for each follower
    for (follower <- followers) {
      //println(follower.toLong+" ,"+destination.toLong)
      edges += Edge(follower.toLong, destination.toLong, 1)

    }
    //make edges to a List
    edges.toList
  }

  def mongoData(community: (Long, List[Long])): (Long, List[Long], List[String])={

    //initialize casbah
    val mongoClient = MongoClient()
    val db = mongoClient("twitter")
    val collection = db("userdata")

    val id = community._1
    var memberStatus = new ListBuffer[String]
    val members = community._2
    //println(members)
    for (member <- members) {
       breakable {
          val query = MongoDBObject("userid" -> member)
          val cursor = collection.find(query)
          if(cursor.isEmpty){break()}
          while (cursor.hasNext) {

            val dbObject = cursor.next()
            var status = " "
            if (dbObject.get("status") != null) {
              status = dbObject.get("status").toString
            }
            memberStatus += status

          }
     }
    }
    (id, members, memberStatus.toList)
  }

  def runLDA(community: (Long, List[Long], List[String]), spark: SparkSession): (Long, List[Long], List[String])={

    //now we will use the statuses as a corpus for LDA

    // For implicit conversions like converting RDDs to DataFrames
    //println("inside LDArun")
    import spark.implicits._
    val sc = spark.sparkContext


    val corpus = sc.parallelize(community._3)
    //corpus.take(2).foreach(println)

    val communityID = community._1
    val members = community._2
    val corpus_body = corpus.map(_.split("\\n\\n")).map(_.drop(1)).map(_.mkString(" "))
    val corpus_df = corpus_body.zipWithIndex.toDF("corpus", "id")

    // Set params for RegexTokenizer
    val tokenizer = new RegexTokenizer()
      .setPattern("[\\W_]+")
      .setMinTokenLength(4) // Filter away tokens with length < 4
      .setInputCol("corpus")
      .setOutputCol("tokens")

    // Tokenize document
    val tokenized_df = tokenizer.transform(corpus_df)

    // import file with stopwords
    val stopwords = spark.sparkContext.textFile("src/main/resources/stopwords").collect()

    val add_stopwords = Array("article", "writes", "entry", "soon", "wait", "open" , "meet" ,"link", "really" ,"look",  "need", "post", "click",  "check", "tweet", "date", "udel", "said", "tell", "want", "come", "think", "read", "watch", "share", "free", "twitter", "know", "just", "newsgroup", "line", "like", "does", "going", "make", "thanks", "didn", "http", "https", "follow", "retweet", "ctto")

    val new_stopwords = stopwords.union(add_stopwords)

    // Set params for StopWordsRemover
    val remover = new StopWordsRemover()
      .setStopWords(new_stopwords)
      .setInputCol("tokens")
      .setOutputCol("filtered")
    // Create new DF with Stopwords removed
    val filtered_df = remover.transform(tokenized_df)
    //filtered_df.show(false)
    // Set params for CountVectorizer
    val vectorizer = new CountVectorizer()
      .setInputCol("filtered")
      .setOutputCol("features")
      .setVocabSize(1000)
      .setMinDF(1)
      .fit(filtered_df)

    // Create vector of token counts
    val countVectors = vectorizer.transform(filtered_df).select("id", "features")
    // Convert DF to JavaRDD (lda needs javaRDD not the casual RDD)
    val lda_countVector = countVectors.map({ case Row(id: Long, countVector: MLVector) => (id, Vectors.fromML(countVector)) }).toJavaRDD

    val numTopics = 10

    // Set LDA params
    val lda = new LDA()
      .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
      .setK(numTopics)
      .setMaxIterations(20)
      .setDocConcentration(-1) // use default values
      .setTopicConcentration(-1) // use default values

    val ldaModel = lda.run(lda_countVector)

    // Review Results of LDA model with Online Variational Bayes
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 5)
    val vocabList = vectorizer.vocabulary
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.map(vocabList(_)).zip(termWeights).toList.map(x => x._1)
    }.toList.flatten

    (communityID, members, topics)
  }

  def main(args: Array[String]): Unit = {

    //setting log4j to show only errors
    setupLogging()

    //setting up spark Session
    val spark = SparkSession
      .builder()
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/twitter.tweets")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/twitter.communities")
      .master("local[*]")
      .appName("community_detection")
      .getOrCreate()

    val sc = spark.sparkContext

    //db ==> RDD
    val dbRDD = sc.loadFromMongoDB()

    //getting in a RDD only the IDs
    val allUsers = dbRDD.map(doc => (doc.get("userid").toString.split(" ").toList,
      doc.get("followers").toString.split(" ").toList))

    //maybe vertices RDD for graph
    val verticeList = allUsers.map(x => x._1 ++ x._2)

    //create edges from the verticesList RDD
    val edges = verticeList.flatMap(makeFollowerEdges)

    //creating graph from edges tuples
    val graph =  Graph.fromEdges(edges, 1)

    //detecting components from ml.sparkling's PSCAN implementation
    //PSCAN will detect the communities from the graph as components
    //of the graph and will associate every vertice with a component ID
    val components = PSCAN.detectCommunities(graph)

    //see the number of edges and vertices of the graph
    println("#edges: "+ graph.numEdges+" #vertices: "+ graph.numVertices)

    //mapping every component with all it's associated vertices
    val groups = components.vertices.map(x => (x._2, x._1.toString + " ")).reduceByKey(_+_)
    val compWithMembers = groups.map(x => (x._1.toLong, x._2.split(" ").toList.map(_.toLong)))

    //now  we filter out all the components with just one member
    //and we get the statuses from mongo
    val compFilt = compWithMembers.filter(_._2.length > 1)
    //compFilt.take(5).foreach(println)
    val communities = compFilt.map(mongoData).collect()
    //println(communities.count())
    val result = communities.map(runLDA(_,spark)).toList

    result.map(x => (x._1, x._3)).foreach(println)

  }
}
