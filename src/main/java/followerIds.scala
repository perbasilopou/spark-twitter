import com.mongodb.casbah.Imports._
import twitter4j.{IDs, Twitter, TwitterFactory, TwitterResponse}
import twitter4j.conf.ConfigurationBuilder
import com.mongodb.casbah.Imports._
import com.mongodb.spark._
import Utilities._

object followerIds{


  def main(args: Array[String]): Unit = {

    //setting Logger to ERROR
    setupLogging()
    var count = 0

    //connect to mongodb
    val mongoClient = MongoClient()
    val db = mongoClient("twitter")
    val collection = db("tweets")

    //clean db
    println("cleaning db...")
    collection.remove(MongoDBObject("followersCount" -> 0))
    collection.remove(MongoDBObject("hashtags" -> ""))


    //setting Twitter4j OAuth credentials
    setupTwitter()

    //setting Twitter4J
    val twitter = new TwitterFactory().getInstance()
    val cursor = collection.find()

    while(cursor.hasNext){

      val user  = cursor.next()

      //if we don't have already data for the cursor user
      if(user.get("followers") == null) {

        //checking Rate limiting
        rateLimitHandler("/followers/ids", twitter)

        val id = user.get("userid").toString.toLong

        val screenName = user.get("screenName")

        println("Requesting data for user: " + screenName + " with id: " + id)

        val followers = twitter.getFollowersIDs(id, -1).getIDs.mkString(" ")


        println("saving followers list...")
        collection.update(MongoDBObject("userid" -> id), $set ("followers" -> followers))

        count+=1
        println("user "+count+"/"+collection.count())

      } else {count+=1
        println(count+"/"+collection.count())

      }
    }

    println("db.tweets is complete!!")
    println("# documents: "+ collection.count())


  }
}
