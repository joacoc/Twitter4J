import java.util.Properties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.logging.log4j.{Level, LogManager}
import twitter4j.{FilterQuery, StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterStream, TwitterStreamFactory}
import twitter4j.conf.ConfigurationBuilder
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}

import scala.io.Source

object TwitterFeeder {

  /* Twitter Config Variables */
  val CONSUMER_KEY: String = sys.env("TWITTER_CONSUMER_KEY")
  val CONSUMER_SECRET: String = sys.env("TWITTER_CONSUMER_SECRET")
  val ACCESS_TOKEN: String = sys.env("TWITTER_ACCESS_TOKEN")
  val ACCESS_TOKEN_SECRET: String = sys.env("TWITTER_ACCESS_TOKEN_SECRET")

  /* Kafka Config Variables */
  val KAFKA_BROKER_IP: String = sys.env("KAFKA_BROKER_IP")
  val KAFKA_TWEETS_TOPIC = sys.env("KAFKA_TWEETS_TOPIC")

  /* Logger */
  private val logger = LogManager.getLogger(TwitterFeeder.getClass)

  def main(args: Array[String]): Unit = {

    /* Kafka Properties & Set Up */
    val props: Properties = new Properties()
    props.put("bootstrap.servers", s"${KAFKA_BROKER_IP}:29092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    val producer = new KafkaProducer[String, String](props)

    /* Twitter Streaming configuration build */
    val cb: ConfigurationBuilder = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(CONSUMER_KEY)
      .setOAuthConsumerSecret(CONSUMER_SECRET)
      .setOAuthAccessToken(ACCESS_TOKEN)
      .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
      .setTweetModeExtended(true)

    /* Build twitter stream */
    val tf: TwitterStreamFactory = new TwitterStreamFactory(cb.build())
    val twitterStream: TwitterStream = tf.getInstance()

    /* Object mapper used to create string JSON */
    val mapper: ObjectMapper = new ObjectMapper()

    /* Stanford tokenizer */
    val tokenizerProperties: Properties = new Properties()
    tokenizerProperties.setProperty("annotators", "tokenize")
    val pipeline = new StanfordCoreNLP(tokenizerProperties)

    /* Stopwords detector */
    val filename = "data/stopwords"
    val stopwords = Source.fromFile(filename).getLines.toVector.map(x => x.hashCode).toSet

    /* Twitter Status listener */
    val listener = new StatusListener {
      override def onStatus(status: Status): Unit = {
        /* Get tweet information */
        val tweetText = if (status.isRetweet) status.getRetweetedStatus.getText else status.getText
        val timestamp = status.getCreatedAt.getTime

        /* Tokenize tweet text */
        val doc: CoreDocument = new CoreDocument(tweetText)
        pipeline.annotate(doc)

        /* Get tokens */
        val tokens = doc.tokens()

        /* Send each token to kafka */
        tokens.forEach(token => {
          val word = token.word()
          val wordHashCode = word.toLowerCase.hashCode

          if (!stopwords.contains(wordHashCode)) {
            /* Turn values into an object node */
            val objectNode: ObjectNode = mapper.createObjectNode()
            objectNode.put("timestamp", timestamp)
            objectNode.put("text", word)

            /* Send record data as JSON */
            val stringJSON = objectNode.toPrettyString
            val record = new ProducerRecord[String, String](KAFKA_TWEETS_TOPIC, stringJSON)
            producer.send(record)
          }
        })
      }

      /* Warnings */
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

    /* Add listener */
    twitterStream.addListener(listener)

    /* Set up filter */
    val newTweetFilterQuery: FilterQuery = new FilterQuery()
    newTweetFilterQuery.track("eth", "ethereum", "$eth", "ether")
    newTweetFilterQuery.language("en")

    /* Filter tweets */
    twitterStream.filter(newTweetFilterQuery)
  }
}
