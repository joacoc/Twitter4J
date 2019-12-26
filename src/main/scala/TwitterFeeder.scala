import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.logging.log4j.{Level, LogManager}
import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase, Observer}
import org.mongodb.scala.bson.Document
import twitter4j.{FilterQuery, StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterStream, TwitterStreamFactory}
import twitter4j.conf.ConfigurationBuilder

import scala.collection.mutable

object TwitterFeeder {

  /* Twitter Config Variables */
  val CONSUMER_KEY: String = sys.env("TWITTER_CONSUER_KEY")
  val CONSUMER_SECRET: String = sys.env("TWITTER_CONSUMER_SECRET")
  val ACCESS_TOKEN: String = sys.env("TWITTER_ACCESS_TOKEN")
  val ACCESS_TOKEN_SECRET: String = sys.env("TWITTER_ACESS_TOKEN_SECRET")


  /* Mongo Config Variables */
  val MONGO_USERNAME: String = sys.env("MONGO_TWEEPY_USER")
  val MONGO_PASSWORD: String = sys.env("MONGO_TWEEPY_PSW")
  val MONGO_TWEETS_DB: String = sys.env("MONGO_TWEETS_DB")
  val MONGO_IP: String = sys.env("MONGO_IP")
  val MONGO_DB: String = sys.env("MONGO_TWEETS_DB")
  val MONGO_AUTH_DB: String = sys.env("MONGO_AUTH_DB")

  private val mongoClient: MongoClient = MongoClient(s"mongodb://${MONGO_USERNAME}:${MONGO_PASSWORD}@${MONGO_IP}/${MONGO_TWEETS_DB}?authSource=${MONGO_AUTH_DB}")
  private val database: MongoDatabase = mongoClient.getDatabase(MONGO_TWEETS_DB)
  private val tagsCollection: MongoCollection[Document] = database.getCollection("tags")
  private val tagsValues: mutable.HashMap[Int,String] = new mutable.HashMap[Int,String]

  /* Kafka Config Variables */
  val KAFKA_BROKER_IP: String = sys.env("KAFKA_BROKER_IP")
  val topic = "tweets"

  /* Logger */
  private val logger = LogManager.getLogger(TwitterFeeder.getClass)

  def main(args: Array[String]): Unit = {

    val cb: ConfigurationBuilder = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(CONSUMER_KEY)
      .setOAuthConsumerSecret(CONSUMER_SECRET)
      .setOAuthAccessToken(ACCESS_TOKEN)
      .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
    val tf: TwitterStreamFactory = new TwitterStreamFactory(cb.build())
    val twitterStream: TwitterStream = tf.getInstance()

    val props: Properties = new Properties()
    props.put("bootstrap.servers",s"${KAFKA_BROKER_IP}:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    val producer = new KafkaProducer[String, String](props)

    val listener = new StatusListener {
      override def onStatus(status: Status): Unit = {
        logger.log(Level.DEBUG, status.getText)
        val stringJSON = s"""{"text": ${status.getText}, "timestamp": ${status.getCreatedAt.getTime}}"""
        val record = new ProducerRecord[String, String](topic,  stringJSON)
        producer.send(record)
      }

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {
        logger.log(Level.WARN, s"Deletion Notice ${statusDeletionNotice.toString}")
      }

      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {
        logger.log(Level.WARN, s"# Of Twitter Limitation Notice: ${numberOfLimitedStatuses}")
      }

      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {
        logger.log(Level.WARN, s"Scrub Geo: ${userId} ${upToStatusId}")
      }

      override def onStallWarning(warning: StallWarning): Unit = {
        logger.log(Level.WARN, s"Stall Warning: ${warning.toString}")
      }

      override def onException(ex: Exception): Unit = {
        logger.log(Level.WARN, s"Exception ${ex.toString}")
      }
    }

    twitterStream.addListener(listener)

    new Thread {
      override def run: Unit = {
        tagsCollection.find().subscribe(createTagsObserver(twitterStream))
        Thread.sleep(60000)
      }
    }.start()
  }


  private def createTagsObserver(twitterStream: TwitterStream): Observer[Document] = {
    new Observer[Document] {
      val newValues: mutable.HashMap[Int,String] = new mutable.HashMap[Int,String]
      var newValuesDetected = false

      override def onNext(result: Document): Unit = {
        val newTag = result.getString("tag")

        newTag match {
          case tag: String =>
            val hashCode = tag.toLowerCase().hashCode()
            newValues.put(hashCode, tag.toLowerCase())
            if (!tagsValues.contains(hashCode)) {
              newValuesDetected = true
            }

          case _ =>
        }
      }

      override def onError(e: Throwable): Unit = {}

      override def onComplete(): Unit = {
        if (newValuesDetected) {
          tagsValues.clear()
          newValues.valuesIterator.foreach(tag => {
            val hashCode = tag.toLowerCase().hashCode()
            tagsValues.put(hashCode, tag.toLowerCase())
          })

          logger.info("Changing twitter track")
          logger.info(s"New track: ${newValues.values.toString()}")
          val newTweetFilterQuery: FilterQuery = new FilterQuery()
          newTweetFilterQuery.track(newValues.values.toVector:_*)
          newTweetFilterQuery.language("es")
          twitterStream.filter(newTweetFilterQuery)
        }
      }
    }
  }
}
