package wavebridge.kafkalib;

import lombok.extern.slf4j.Slf4j;
import wavebridge.kafkalib.util.ProducerProperties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/*
 * 주의 : 
 */
@Slf4j
public class messageProducer {

  private static final Producer<String, Object> producer = new KafkaProducer<>(ProducerProperties.getProducerProperties());
  private final String topicName = ProducerProperties.getTopicName();
  private static class InstanceHolder {
    public static messageProducer producerInstance = new messageProducer();
  }

  public static messageProducer getInstance() {
    // producer = new KafkaProducer<>(ProducerProperties.getProducerProperties());
    log.debug("===========================================================");
    log.debug("Initializing UserDataProducer : producer : {}", producer.hashCode());
    log.debug("===========================================================");
    return InstanceHolder.producerInstance;
  }
  
  /* 
   * 동기전송
   * ACK 설정에 따라 브로커 응답을 대기함
   * [ACK : 1]  리더 파티션 수신 후 응답(브로커 -> 프로듀서)
   * [ACK : 2]  리더 파티션 + 하나의 팔로워 파티션 수신 후 응답(브로커 -> 프로듀서)
   * [ACK : -1 또는 all] -> 리더 파티션과 모든 팔로워 파티션 수신 후 응답(브로커 -> 프로듀서)
   */
  public void sendUserDataSync(String key, Object messageToSend, String topicName) throws Exception{
    try {
      ProducerRecord<String, Object> record = new ProducerRecord<>(topicName, key, messageToSend);
      RecordMetadata metadata = producer.send(record).get();
      log.debug("Topic: {}, Partition: {}, Offset: {}, Key: {}, Received Message: {}\n", 
        metadata.topic(), metadata.partition(), metadata.offset(), record.key(), record.value());
    } catch (Exception e) {
      log.error("Exception occured while sending message : %s", e);
    }
  }

  // 비동기 전송/콜백(예외처리필요)
  public void sendUserDataAsync(String key, Object messageToSend, String topicName) throws Exception{
    try {
      ProducerRecord<String, Object> record = new ProducerRecord<>(topicName, key, messageToSend);
      producer.send(record, new ProducerCallback(record));
    } catch (Exception e) {
      log.error("Exception occured while sending message : %s", e);
    }
  }

  // 트랜잭션모드 전송
  // 라이브러리로는 적합하지 않지만 템플릿 차원에서 작성함.
  public void sendUserDataCommit(String key, Object messageToSend, String topicName) throws Exception{
    Producer<String, Object> transactionalProducer = new KafkaProducer<>(ProducerProperties.getTransactionalProperties());
    transactionalProducer.initTransactions(); 
    transactionalProducer.beginTransaction();
    try {
      ProducerRecord<String, Object> record = new ProducerRecord<>(topicName, key, messageToSend);
      transactionalProducer.send(record);
      transactionalProducer.flush();
    } catch (Exception e) {
      transactionalProducer.abortTransaction();
      log.error("Exception occured while sending message with transaction... transaction aborted: {}", e);
    } finally {
      transactionalProducer.commitTransaction();      
      transactionalProducer.close();
    }
  }

  public void sendUserDataSync(String key, Object messageToSend) throws Exception{
    sendUserDataSync(key, messageToSend, topicName);
  }

  public void sendUserDataAsync(String key, Object messageToSend) throws Exception{
    sendUserDataAsync(key, messageToSend, topicName);
  }

  public void sendUserDataCommit(String key, Object messageToSend) throws Exception{
    sendUserDataCommit(key, messageToSend, topicName);
  }
  public void close() {
    messageProducer.producer.close();
  }

  public void close(Producer<String, Object> producer) {
    producer.close();
  }
}