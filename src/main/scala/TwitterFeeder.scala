import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.logging.log4j.{Level, LogManager}
import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase, Observer}
import org.mongodb.scala.bson.Document
import org.mongodb.scala.model.Projections.{include, exclude}
import twitter4j.{FilterQuery, StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterStream, TwitterStreamFactory}
import twitter4j.conf.ConfigurationBuilder
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry

import scala.collection.mutable

object TwitterFeeder {

  /* Twitter Config Variables */
  val CONSUMER_KEY: String = sys.env("TWITTER_CONSUMER_KEY")
  val CONSUMER_SECRET: String = sys.env("TWITTER_CONSUMER_SECRET")
  val ACCESS_TOKEN: String = sys.env("TWITTER_ACCESS_TOKEN")
  val ACCESS_TOKEN_SECRET: String = sys.env("TWITTER_ACCESS_TOKEN_SECRET")

  /* Mongo Config Variables */
  val MONGO_USER: String = sys.env("MONGO_USER")
  val MONGO_PSW: String = sys.env("MONGO_PSW")
  val MONGO_DB: String = sys.env("MONGO_DB")
  val MONGO_IP: String = sys.env("MONGO_IP")
  val MONGO_CONFIG_COLLECTION: String = sys.env("MONGO_CONFIG_COLLECTION")

  /* Mongo Client Init */
  val mongoUri: String = s"mongodb+srv://${MONGO_USER}:${MONGO_PSW}@${MONGO_IP}/${MONGO_DB}?retryWrites=true&w=majority"
  private val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[ConfigDocument], classOf[Options]), DEFAULT_CODEC_REGISTRY )
  private val mongoClient: MongoClient = MongoClient(mongoUri)
  private val database: MongoDatabase = mongoClient.getDatabase(MONGO_DB).withCodecRegistry(codecRegistry)
  private val configCollection: MongoCollection[ConfigDocument] = database.getCollection(MONGO_CONFIG_COLLECTION)
  private val tags: mutable.HashMap[Int,String] = new mutable.HashMap[Int,String]

  /* Kafka Config Variables */
  val KAFKA_BROKER_IP: String = sys.env("KAFKA_BROKER_IP")
  val topic = sys.env("KAFKA_TWEETS_TOPIC")

  /* Logger */
  private val logger = LogManager.getLogger(TwitterFeeder.getClass)

  def main(args: Array[String]): Unit = {

    /* Kafka Properties & Set Up */
    val props: Properties = new Properties()
    props.put("bootstrap.servers",s"${KAFKA_BROKER_IP}:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    val producer = new KafkaProducer[String, String](props)

    /* Twitter Streaming configuration build */
    val cb: ConfigurationBuilder = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(CONSUMER_KEY)
      .setOAuthConsumerSecret(CONSUMER_SECRET)
      .setOAuthAccessToken(ACCESS_TOKEN)
      .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
      .setTweetModeExtended(true)
    val tf: TwitterStreamFactory = new TwitterStreamFactory(cb.build())
    val twitterStream: TwitterStream = tf.getInstance()

    /* Twitter Status listener */
    val mapper: ObjectMapper = new ObjectMapper()

    val listener = new StatusListener {
      override def onStatus(status: Status): Unit = {
        val objectNode: ObjectNode = mapper.createObjectNode()
        objectNode.put("timestamp", status.getCreatedAt.getTime)

        if (status.isRetweet) {
          objectNode.put("text", status.getRetweetedStatus.getText)
        } else {
          objectNode.put("text", status.getText)
        }

        val stringJSON = objectNode.toPrettyString
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

    /* Tags & Config retrieving */
    new Thread {
      override def run: Unit = {
        while (true) {
          configCollection.find().subscribe(createTagsObserver(twitterStream))
          Thread.sleep(60000)
        }
      }
    }.start()
  }


  private def createTagsObserver(twitterStream: TwitterStream): Observer[ConfigDocument] = {
    new Observer[ConfigDocument] {
      val newValues: mutable.HashMap[Int,String] = new mutable.HashMap[Int,String]
      var newValuesDetected = false

      override def onNext(configDocument: ConfigDocument): Unit = {
        configDocument.options.tags.foreach(tag => {
          val hashCode = tag.toLowerCase().hashCode()
          newValues.put(hashCode, tag.toLowerCase())
          if (!tags.contains(hashCode)) {
            newValuesDetected = true
          }
        })
      }

      override def onError(e: Throwable): Unit = {
        logger.error(e)
      }

      override def onComplete(): Unit = {
        if (newValuesDetected) {
          tags.clear()
          newValues.valuesIterator.foreach(tag => {
            val hashCode = tag.toLowerCase().hashCode()
            tags.put(hashCode, tag.toLowerCase())
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

case class Options(tags: Vector[String], windowsTimes: Vector[Int], stopwords: Vector[String])
case class ConfigDocument(options: Options)