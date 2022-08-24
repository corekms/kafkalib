package wavebridge.kafkaproducer;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import wavebridge.kafkaproducer.util.ProducerConfig;

@Slf4j
public class UserDataProducer {

  private static String topicName;
  private static Producer<String, Object> producer;
  private static class InstanceHolder {
    public static UserDataProducer producerInstance = new UserDataProducer();
  }

  public static UserDataProducer getInstance() {
      log.info("===========================================================");
      log.info("Initializing UserDataProducer ...");
      log.info("===========================================================");
      final Properties PROPS = ProducerConfig.getProperties();
      topicName = PROPS.getProperty("default.topic");
      producer = new KafkaProducer<>(PROPS);
      return InstanceHolder.producerInstance;
  }
  
  // To send message Synchronously which means more latency.
  // To make it able to change topic name on-the-fly
  public void sendUserDataSync(String key, Object messageToSend, String topicName) {
    try {
      ProducerRecord<String, Object> record = new ProducerRecord<>(topicName, key, messageToSend);
      RecordMetadata metadata = producer.send(record).get();
      log.debug("Topic: {}, Partition: {}, Offset: {}, Key: {}, Received Message: {}\n", 
        metadata.topic(), metadata.partition(), metadata.offset(), record.key(), record.value());
    } catch (Exception e) {
      log.error("Exception occured while sending message : %s", e);
    }
  }

  // To send message Asynchronously with callback.
  public void sendUserDataAsync(String key, Object messageToSend, String topicName) {
    try {
      ProducerRecord<String, Object> record = new ProducerRecord<>(topicName, key, messageToSend);
      producer.send(record, new ProducerCallback(record));
    } catch (Exception e) {
      log.error("Exception occured while sending message : %s", e);
    }
  }

  public void sendUserDataSync(String key, Object messageToSend) {
    sendUserDataSync(key, messageToSend, topicName) ;
  }

  public void sendUserDataAsync(String key, Object messageToSend) {
    sendUserDataAsync(key, messageToSend, topicName);
  }

  public void close() throws Exception {
    UserDataProducer.producer.close();
  }
}