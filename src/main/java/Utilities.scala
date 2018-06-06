import org.apache.log4j.{Level, Logger}
import twitter4j.Twitter

object Utilities {
  // Makes sure only ERROR messages get logged to avoid log spam.
  def setupLogging(): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    Logger.getLogger("akka").setLevel(Level.ERROR)
  }

  //Configures Twitter service credentials using twitter.txt in the resources directory
  def setupTwitter(): Unit = {

    import scala.io.Source

    for (line <- Source.fromFile("src/main//resources/twitter").getLines) {

      val fields = line.split(" ")

      if (fields.length == 2) {

        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  //Manage Rate Limitation
  //Different requests get different rate limits
  //so we use the Paths we receive from .getRateLimitStatus()
  def rateLimitHandler(apiPath :String, twitter :Twitter): Unit={

    val response = twitter.getRateLimitStatus()
    //println(response)
    val limit = response.get(apiPath)

    println(apiPath+" remaining requests: " + limit.getRemaining)

    if(limit == null){

      println("No such path in twitter response")
      return

    }
    if (limit.getRemaining == 0) {

      println(apiPath+" rate limit Reached")

      println("Thread will sleep for "+(limit.getSecondsUntilReset+10)/60+" minutes")

      Thread.sleep((limit.getSecondsUntilReset+10)*1000)
    }
  }

}
