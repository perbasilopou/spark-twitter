import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.{LDA, OnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import com.mongodb.spark._
import org.apache.spark.rdd.RDD


import Utilities._

object runLDA {


  def main(args: Array[String]): Unit = {


    setupLogging()

    val spark = SparkSession
      .builder()
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/twitter.userdata")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/twitter.dataOutput")
      .master("local[*]")
      .appName("ldatrain")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val sc = spark.sparkContext
    val corpus = sc.loadFromMongoDB().map(doc => doc.get("status").toString.toLowerCase())
    val corpus_body = corpus.map(_.split("\\n\\n")).map(_.drop(1)).map(_.mkString(" "))
    //val corpus = sc.wholeTextFiles("src/main/resources/mini_newsgroups/*").map(_._2).map(_.toLowerCase())
    //val corpus_body = corpus.map(_.split("\\n\\n")).map(_.drop(1)).map(_.mkString(" "))
    val corpus_df = corpus_body.zipWithIndex.toDF("corpus", "id")
    //corpus_df.show(false)
    //corpus_body.take(5).foreach(println)
    // Set params for RegexTokenizer
    val tokenizer = new RegexTokenizer()
      .setPattern("[\\W_]+")
      .setMinTokenLength(4) // Filter away tokens with length < 4
      .setInputCol("corpus")
      .setOutputCol("tokens")

    // Tokenize document
    val tokenized_df = tokenizer.transform(corpus_df)
    // import file with stopwords
    val stopwords = sc.textFile("src/main/resources/stopwords").collect()
    val add_stopwords = Array("article","today", "turn", "writes", "entry", "soon", "wait", "open" , "meet" ,"link", "really" ,"look",  "need", "post", "click",  "check", "tweet", "date", "udel", "said", "tell", "want", "come", "think", "read", "watch", "share", "free", "twitter", "know", "just", "newsgroup", "line", "like", "does", "going", "make", "thanks", "didn", "http", "https", "follow", "retweet")
    val new_stopwords = stopwords.union(add_stopwords)
    //stopwords.take(20).foreach(println)
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
      .setVocabSize(10000)
      .setMinDF(5)
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
      terms.map(vocabList(_)).zip(termWeights)
    }
    println(s"$numTopics topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) => println(s"$term\t$weight") }
      println(s"==========")
    }

  }
}