package wavebridge.kafkalib.consumer;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import wavebridge.kafkalib.util.ConsumerProperties;

public class ManualCommitConsumer {
  private static final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ConsumerProperties.getManualCommitConsumerProperties());
  private static class InstanceHoler {
    public static ManualCommitConsumer consmuerInstance = new ManualCommitConsumer();
  }
  
  public static ManualCommitConsumer getInstance() {
    consumer.subscribe(Arrays.asList(ConsumerProperties.getTopicName()));
    return InstanceHoler.consmuerInstance;
  }

  public void pollAndCommit(Boolean isSynchronous) throws Exception {
    ConsumerRecords<String, String> records = consumer.poll(ConsumerProperties.getPollingIntervalMs());
    for(ConsumerRecord<String, String> record : records) { //poll()은 레코드 전체를 리턴하고, 하나의 메시지만 가져오는 것이 아니므로, 반복문 처리
      System.out.printf("Topic: %s, Partition: %s, Offset: %d, Key: %s, Value: %s\n",
              record.topic(), record.partition(), record.offset(), record.key(), record.value());
    }
  }

  public void close() throws Exception {
    consumer.close();
  }
}