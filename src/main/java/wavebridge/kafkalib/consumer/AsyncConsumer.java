package wavebridge.kafkalib.consumer;

import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import wavebridge.kafkalib.util.ConsumerProperties;

public class AsyncConsumer {
  private static final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ConsumerProperties.getConsumerProperties());
  private static class InstanceHoler {
    public static AutoCommitConsumer consmuerInstance = new AutoCommitConsumer();
  }

  public static AutoCommitConsumer getInstance() {
    consumer.subscribe(Arrays.asList(ConsumerProperties.getTopicName()));
    return InstanceHoler.consmuerInstance;
  }

  // async-commit consumer : 브로커에게 커밋요청 후 응답을 기다리지 않음.
  public void pollAutoCommit() throws Exception {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
    for (ConsumerRecord<String, String> record : records) { //poll()은 레코드 전체를 리턴하고, 하나의 메시지만 가져오는 것이 아니므로, 반복문 처리합니다.
      System.out.printf("Topic: %s, Partition: %s, Offset: %d, Key: %s, Value: %s\n",
              record.topic(), record.partition(), record.offset(), record.key(), record.value());
    }
  }

  public void close() {
    consumer.close();
  }
}
