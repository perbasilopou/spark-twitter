import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import com.mongodb.spark._
import org.bson.Document
import Utilities._

object stream {

  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    //setting up spark configuration and connection with mongodb
    val sparkConf = new SparkConf()
      .set("spark.mongodb.input.uri","mongodb://127.0.0.1/twitter.tweets")
      .set("spark.mongodb.output.uri","mongodb://127.0.0.1/twitter.tweets")
      .setMaster("local[*]")
      .setAppName("spark-twitter")

    //setting up spark streaming context
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    //stream RDD with tweets written at english
    //and have followers
    //and are not protected, so I can request their followers
    val tweet = TwitterUtils.createStream(ssc, None)
      .filter(_.getLang == "en").filter(_.getUser.getFollowersCount > 3).filter(_.getUser.isProtected == false).filter(_.getText.contains("#"))
    //extract from the stream the properties we need
    val tweetData = tweet.map(tweet => {
      val text = tweet.getText
      val userid = tweet.getUser.getId
      val screenName = tweet.getUser.getScreenName
      val followersCount = tweet.getUser.getFollowersCount
      val friendsCount = tweet.getUser.getFriendsCount
      val location = tweet.getUser.getLocation
      val hashtags = tweet.getHashtagEntities.map(_.getText.toLowerCase).mkString(" ")
      val mentionids = tweet.getUserMentionEntities.map(_.getId).mkString(" ")


      //create a document to save the properties to mongodb
      val doc = new Document()

      doc.put("text", text)
      doc.put("userid", userid)
      doc.put("screenName", screenName)
      doc.put("followersCount", followersCount)
      doc.put("friendsCount", friendsCount)
      doc.put("hashtags", hashtags)
      doc.put("location", location)
      doc.put("mentionids", mentionids)
      doc.put("followers", null)
      doc
    })

    //save the data to mongodb
    var collected = 0L

    tweetData.foreachRDD ( rdd => {

      rdd.cache()

      val count = rdd.count()

      rdd.saveToMongoDB()

      collected += count

      println("Collected Tweets: " + collected)

      //I close the stream because of rate limitation
      //from the REST API
      if (collected >= 120){
          ssc.stop()
          println("Application stopped")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

