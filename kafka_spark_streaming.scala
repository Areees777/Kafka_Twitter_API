import org.apache.spark.storage.StorageLevel
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j.Level
import org.apache.spark.streaming.twitter._
import twitter4j.TwitterFactory

import java.util.Properties

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object kafka_spark_streaming {

  def main(args: Array[String]): Unit = {

    //Claves para la conexion con la API de Twitter
    val consumerKey = "XXXX"
    val consumerSecret = "XXXX"
    val accessToken = "XXXX"
    val accessTokenSecret = "XXXX"

    setupLogging()

    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Streaming - Kafka")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    //Conexion a la API de Twitter
    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)


    val auth = new OAuthAuthorization(cb.build)
    val stream = TwitterUtils.createStream(ssc, Some(auth))
    val spanishTweets = stream.filter(_.getLang() == "es")

    //Por cada tweet cojo el texto y el nombre de usuario
    val statuses = spanishTweets.map(status => (status.getText(),status.getUser.getName()))

    statuses.foreachRDD{ rdd =>

      rdd.foreachPartition { partitionIter =>

        val props = new Properties()
        val bootstrap = "127.0.0.1:9092"
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("bootstrap.servers", bootstrap)
        val producer = new KafkaProducer[String, String](props)
        partitionIter.foreach { elem =>
          val dat = elem.toString()
          val data = new ProducerRecord[String, String]("twitter", null, dat)
          producer.send(data)

        }

        producer.flush()
        producer.close()

      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

}

