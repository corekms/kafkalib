package wavebridge.kafkalib.consumer;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import lombok.extern.slf4j.Slf4j;
import wavebridge.kafkalib.util.ConsumerProperties;

@Slf4j
public class AutoCommitConsumer {
  static class HookThread extends Thread {
		@Override
		public void run() {
			log.info("Shutting down...");
      try {
        AutoCommitConsumer.consumer.wakeup();
      } catch (Exception e) {
        e.printStackTrace();
      }
		}
  }

  // public static final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ConsumerProperties.getAutoCommitConsumerProperties());
  private static final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ConsumerProperties.getAutoCommitConsumerProperties());
  private static class InstanceHolder {
    public static AutoCommitConsumer consmuerInstance = new AutoCommitConsumer();
  }
  
  public static AutoCommitConsumer getInstance() {
    Runtime.getRuntime().addShutdownHook(new HookThread());
    consumer.subscribe(Arrays.asList(ConsumerProperties.getTopicName()));
    return InstanceHolder.consmuerInstance;
  }

  // Auto-commit consumer : 백그라운드에서 주기적으로 자동커밋
  public void fetchMessage() {
    try {
      ConsumerRecords<String, String> records = consumer.poll(ConsumerProperties.getPollingIntervalMs());
      for(ConsumerRecord<String, String> record : records) { //poll()은 레코드 전체를 리턴하고, 하나의 메시지만 가져오는 것이 아니므로, 반복문 처리
        System.out.printf("Topic: %s, Partition: %s, Offset: %d, Key: %s, Value: %s\n",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
      }
    } catch(WakeupException e) {
      close();
    }
  }

  public void close() {
    log.info("Closing Auto-commit consumer...");
    consumer.close();
  }
}
