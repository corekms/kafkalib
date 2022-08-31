package wavebridge.kafkalib.consumer;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import lombok.extern.slf4j.Slf4j;
import wavebridge.kafkalib.util.ConsumerProperties;

/*
 * 컨슈머 사이드에서 isolation.level을 READ_COMMIT 으로 운영하고 할때 사용한다.
 * READ_COMMIT : non-transactional message 와 더불어 transactional message 중 프로듀서 사이드에서 send() 이후 commit()이 완료된 메세지만 읽을 수 있다.
 */
@Slf4j
public class TransactionalConsumer {
  static class HookThread extends Thread {
		@Override
		public void run() {
			log.info("Shutting down...");
      try {
        TransactionalConsumer.consumer.wakeup();
      } catch (Exception e) {
        e.printStackTrace();
      }
		}
  }

  private static final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ConsumerProperties.getTransactionalConsumerProperties());
  private static class InstanceHoler {
    public static TransactionalConsumer consmuerInstance = new TransactionalConsumer();
  }

  public static TransactionalConsumer getInstance() {
    Runtime.getRuntime().addShutdownHook(new HookThread());
    consumer.subscribe(Arrays.asList(ConsumerProperties.getTopicName()));
    return InstanceHoler.consmuerInstance;
  }

  public void fetchMessage(Boolean isSynchronous) throws Exception {
    try {
      ConsumerRecords<String, String> records = consumer.poll(ConsumerProperties.getPollingIntervalMs());
      for(ConsumerRecord<String, String> record : records) { //poll()은 레코드 전체를 리턴하고, 하나의 메시지만 가져오는 것이 아니므로, 반복문 처리
        System.out.printf("Topic: %s, Partition: %s, Offset: %d, Key: %s, Value: %s\n",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
      }
      if(isSynchronous) consumer.commitSync(); else consumer.commitAsync();
    } catch(WakeupException e) {
      close();
    }
  }

  public void fetchMessage() throws Exception {
    fetchMessage(true);
  }

  public void close() throws Exception {
    System.out.println("Closing Transactional consumer...");
    consumer.close();
  }
}
