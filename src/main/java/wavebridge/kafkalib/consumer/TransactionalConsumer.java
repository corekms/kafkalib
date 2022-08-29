package wavebridge.kafkalib.consumer;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import wavebridge.kafkalib.util.ConsumerProperties;

/*
 * 컨슈머 사이드에서 isolation.level을 READ_COMMIT 으로 운영하고 할때 사용한다.
 * READ_COMMIT : non-transactional message 와 더불어 transactional message 중 프로듀서 사이드에서 send() 이후 commit()이 완료된 메세지만 읽을 수 있다.
 */
public class TransactionalConsumer {
  private static final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ConsumerProperties.getTransactionalConsumerProperties());
  private static class InstanceHoler {
    public static TransactionalConsumer consmuerInstance = new TransactionalConsumer();
  }

  public static TransactionalConsumer getInstance() {
    consumer.subscribe(Arrays.asList(ConsumerProperties.getTopicName()));
    return InstanceHoler.consmuerInstance;
  }

  public void pollCommittedAndCommit(Boolean isSynchronous) throws Exception {
    ConsumerRecords<String, String> records = consumer.poll(ConsumerProperties.getPollingIntervalMs());
    for (ConsumerRecord<String, String> record : records) { //poll()은 레코드 전체를 리턴하고, 하나의 메시지만 가져오는 것이 아니므로, 반복문 처리
      System.out.printf("Topic: %s, Partition: %s, Offset: %d, Key: %s, Value: %s\n",
              record.topic(), record.partition(), record.offset(), record.key(), record.value());
    }
    if(isSynchronous) consumer.commitAsync(); else consumer.commitAsync();
  }

  public void close() throws Exception {
    consumer.close();
  }
}
