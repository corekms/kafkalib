package wavebridge.kafkalib.producer;
import lombok.extern.slf4j.Slf4j;
import wavebridge.kafkalib.util.ProducerProperties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
public class SyncProducer {
  private static final Producer<String, Object> producer = new KafkaProducer<>(ProducerProperties.getProducerProperties());
  private final String topicName = ProducerProperties.getTopicName();
  private static class InstanceHolder {
    public static SyncProducer producerInstance = new SyncProducer();
  }

  public static SyncProducer getInstance() {
    // producer = new KafkaProducer<>(ProducerProperties.getProducerProperties());
    log.debug("===========================================================");
    log.debug("Initializing AsyncProducer : producer : {}", producer.hashCode());
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

  public void sendUserDataSync(String key, Object messageToSend) throws Exception{
    sendUserDataSync(key, messageToSend, topicName);
  }

  public void close() {
    SyncProducer.producer.close();
  }
}
