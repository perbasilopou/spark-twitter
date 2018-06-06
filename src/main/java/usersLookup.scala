import com.mongodb.casbah.Imports._
import twitter4j._
import scala.collection.mutable.ListBuffer
import com.mongodb.util.JSON
import org.bson.Document
import scala.util.control.Breaks._
import Utilities._

object usersLookup{

  def mongoIdList(twitter :Twitter): List[Long] ={


    //connect to mongodb
    val mongoClient = MongoClient()
    val db = mongoClient("twitter")
    val collection = db("tweets")
    val cursor = collection.find()
    //listbuffer to fill with IDs
    val idList = new ListBuffer[Long]
    //get all ids of users and followers in mongo<
    while(cursor.hasNext){

      val user  = cursor.next()
     // if(user.get("READ") != true) {
        idList += user.get("userid").toString.toLong

        val foll = user.get("followers").toString

        val follwrs = foll.split(" ")

        follwrs.foreach(idList += _.toLong)
      //}
    }
    idList.distinct
    var count = 0L
    breakable{
      println("cleaning list...")

      for (id <- idList) {
        if (db("userdata").find(DBObject("userid" -> id)).limit(1).size != 0) {
          idList -= id
          count += 1
          println("progress . . . " + count + "/" + db("userdata").count())
        }
        if (count == db("userdata").count()) {
        break()
        }
      }
    }
    idList.toList
  }


  def main(args: Array[String]): Unit = {

    //setting Logger to ERROR
    setupLogging()

    //initializing casbah
    val mongoClient = MongoClient()
    val db = mongoClient("twitter")
    val collection = db("userdata")

    //setting Twitter4j OAuth credentials
    setupTwitter()


    //setting Twitter4J
    val twitter = new TwitterFactory().getInstance()
    //getting List of all ids in mongo
    val userIDs =  mongoIdList(twitter)

    var count = 0L

    for ( id <- userIDs) {


     // if (collection.find(DBObject("userid" -> id)).limit(1).size == 0) {
        breakable {
          try {
            val data = twitter.lookupUsers(id)
            val limit = data.getRateLimitStatus
            println("remaining requests: " + limit.getRemaining)

            if (limit.getRemaining <= 1) {

              println("rate limit Reached")

              println("Thread will sleep for " + (limit.getSecondsUntilReset + 10) / 60 + " minutes")

              Thread.sleep((limit.getSecondsUntilReset + 10) * 1000)
            }

          count += 1

            //users with protected account are ignored
            if (!data.get(0).isProtected) {

              val doc = new Document()
              val status = data.get(0).getStatus.getText.toLowerCase
              val hashtags = data.get(0).getStatus.getHashtagEntities.map(_.getText).mkString(" ")

              doc.put("userid", id)
              doc.put("status", status)
              doc.put("hashtags", hashtags)
              collection.insert(JSON.parse(doc.toJson).asInstanceOf[DBObject])

            }
          } catch {

            //Some user requests throw npe
            //no idea why...
            //they are ignored

            case e: NullPointerException =>

              println("Null pointer exception from id: " + id)

            //some user requests return weird exception
            //they are ignored
            case e: TwitterException =>


              println("weird")
              break()
          }
        }
      //}
      /*  else
        {
          println("Data for user already exists")

          count += 1

        }*/
        println("users: " + count + "/" + userIDs.length)


      }
    println("DB complete!!!")
  }
}