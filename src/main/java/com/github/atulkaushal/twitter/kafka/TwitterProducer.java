package com.github.atulkaushal.twitter.kafka;

import java.util.Properties;
import java.util.function.Consumer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.redouane59.twitter.TwitterClient;
import com.github.redouane59.twitter.dto.tweet.TweetV2;

/**
 * The Class TwitterProducer.
 *
 * @author Atul
 */
public class TwitterProducer {

  /** The logger. */
  static Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

  /** The Constant BOOTSTRAP_SERVERS. */
  private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

  /** Instantiates a new twitter producer. */
  TwitterProducer() {}

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  /** Run. */
  public void run() {

    // create twitter client
    TwitterClient twitterClient = createTwitterClient();

    // create kafka producer
    KafkaProducer<String, String> producer = createKafkaProducer();

    // push data into kafka
    pushDataToKafka(twitterClient, producer);
  }

  /**
   * Push data to kafka.
   *
   * @param twitterClient the twitter client
   * @param producer the producer
   */
  @SuppressWarnings("unchecked")
  private static void pushDataToKafka(
      TwitterClient twitterClient, KafkaProducer<String, String> producer) {
    System.out.println("Reading Stream:");
    twitterClient.startFilteredStream(
        new Consumer() {
          @Override
          public void accept(Object t) {
            TweetV2 tweet = (TweetV2) t;
            String jsonTweet = new com.google.gson.Gson().toJson(tweet);
            logger.info(jsonTweet);
            producer.send(
                new ProducerRecord<String, String>("twitter_tweets", jsonTweet),
                new Callback() {

                  @Override
                  public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                      logger.error("Something went wrong!", exception);
                    }
                  }
                });
          }
        });
  }

  /**
   * Creates the kafka producer.
   *
   * @return the kafka producer
   */
  private KafkaProducer<String, String> createKafkaProducer() { // TODO Auto-generated method stub
    // Create Producer Properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create the Producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    return producer;
  }

  /**
   * Creates the twitter client.
   *
   * @return the twitter client
   */
  private TwitterClient createTwitterClient() {
    TwitterClient twitterClient = new TwitterClient();
    twitterClient.addFilteredStreamRule("Kafka", "Rule for Kafka");
    return twitterClient;
  }
}
