package wavebridge.kafkalib.producer;
import lombok.extern.slf4j.Slf4j;
import wavebridge.kafkalib.util.ProducerProperties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class TransactionalProducer {
  private static final Producer<String, Object> producer = new KafkaProducer<>(ProducerProperties.getTransactionalProperties());
  private final String topicName = ProducerProperties.getTopicName();
  private static class InstanceHolder {
    public static TransactionalProducer producerInstance = new TransactionalProducer();
  }

  public static TransactionalProducer getInstance() {
    // producer = new KafkaProducer<>(ProducerProperties.getProducerProperties());
    log.debug("===========================================================");
    log.debug("Initializing UserDataProducer : producer : {}", producer.hashCode());
    log.debug("===========================================================");
    producer.initTransactions();
    return InstanceHolder.producerInstance;
  }

  public void sendUserDataCommit(String key, Object messageToSend, String topicName) throws Exception{
    // Producer<String, Object> transactionalProducer = new KafkaProducer<>(ProducerProperties.getTransactionalProperties()); 
    producer.beginTransaction();
    try {
      ProducerRecord<String, Object> record = new ProducerRecord<>(topicName, key, messageToSend);
      producer.send(record);
      producer.flush();
    } catch (Exception e) {
      producer.abortTransaction();
      log.error("Exception occured while sending message with transaction... transaction aborted: {}", e);
    } finally {
      producer.commitTransaction();      
      // producer.close();
    }
  }

  public void sendUserDataCommit(String key, Object messageToSend) throws Exception{
    sendUserDataCommit(key, messageToSend, topicName);
  }

  public void close() throws Exception {
    TransactionalProducer.producer.close();
  }
}
