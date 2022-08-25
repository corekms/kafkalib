package wavebridge.kafkalib.producer;

import lombok.extern.slf4j.Slf4j;
import wavebridge.kafkalib.util.ProducerProperties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class AsyncProducer {
  private static final Producer<String, Object> producer = new KafkaProducer<>(ProducerProperties.getProducerProperties());
  private final String topicName = ProducerProperties.getTopicName();
  private static class InstanceHolder {
    public static AsyncProducer producerInstance = new AsyncProducer();
  }

  public static AsyncProducer getInstance() {
    // producer = new KafkaProducer<>(ProducerProperties.getProducerProperties());
    log.debug("===========================================================");
    log.debug("Initializing AsyncProducer : producer : {}", producer.hashCode());
    log.debug("===========================================================");
    return InstanceHolder.producerInstance;
  }

  // 비동기 전송/콜백(예외처리필요)
  public void sendUserDataAsync(String key, Object messageToSend, String topicName) throws Exception {
    try {
      ProducerRecord<String, Object> record = new ProducerRecord<>(topicName, key, messageToSend);
      producer.send(record, new ProducerCallback(record));
    } catch (Exception e) {
      log.error("Exception occured while sending message : %s", e);
    }
  }

  public void sendUserDataAsync(String key, Object messageToSend) throws Exception {
    sendUserDataAsync(key, messageToSend, topicName);
  }

  public void close() {
    AsyncProducer.producer.close();
  }
}
