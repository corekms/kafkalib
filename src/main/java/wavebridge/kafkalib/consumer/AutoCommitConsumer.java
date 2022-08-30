package wavebridge.kafkalib.consumer;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import wavebridge.kafkalib.util.ConsumerProperties;

public class AutoCommitConsumer {
  private static final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ConsumerProperties.getAutoCommitConsumerProperties());
  private static class InstanceHolder {
    public static AutoCommitConsumer consmuerInstance = new AutoCommitConsumer();
  }
  
  public static AutoCommitConsumer getInstance() {
    consumer.subscribe(Arrays.asList(ConsumerProperties.getTopicName()));
    return InstanceHolder.consmuerInstance;
  }

  // Auto-commit consumer : 백그라운드에서 주기적으로 자동커밋
  public void pollAutoCommit() throws Exception {
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
