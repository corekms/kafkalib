package wavebridge.kafkalib.consumer;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import lombok.extern.slf4j.Slf4j;
import wavebridge.kafkalib.util.ConsumerProperties;

@Slf4j
public class ManualCommitConsumer {
  static class HookThread extends Thread {
		@Override
		public void run() {
			log.info("Shutting down...");
      try {
        ManualCommitConsumer.consumer.wakeup();
      } catch (Exception e) {
        e.printStackTrace();
      }
		}
  }

  private static final KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(ConsumerProperties.getManualCommitConsumerProperties());
  private static class InstanceHoler {
    public static ManualCommitConsumer consmuerInstance = new ManualCommitConsumer();
  }
  
  public static ManualCommitConsumer getInstance() {
    Runtime.getRuntime().addShutdownHook(new HookThread());
    consumer.subscribe(Arrays.asList(ConsumerProperties.getTopicName()));
    return InstanceHoler.consmuerInstance;
  }

  public void fetchMessage() throws Exception {
    try {
      ConsumerRecords<String, Object> records = consumer.poll(ConsumerProperties.getPollingIntervalMs());
      for(ConsumerRecord<String, Object> record : records) { //poll()은 레코드 전체를 리턴하고, 하나의 메시지만 가져오는 것이 아니므로, 반복문 처리
        log.info("Topic: {}, Partition: {}, Offset: {}, Key: {}, Value: {}\n",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
      }
    } catch(WakeupException e) {
      close();
    }
  }

  public void commitConsumer(Boolean isSynchronous) {
    if(isSynchronous) consumer.commitSync(); consumer.commitAsync();
  }

  public void close() throws Exception {
    System.out.println("Closing Manual-commit consumer...");
    consumer.close();
  }
}