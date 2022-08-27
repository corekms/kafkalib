package wavebridge.kafkalib;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.junit.jupiter.api.Test;

import wavebridge.kafkalib.consumer.AutoCommitConsumer;
import wavebridge.kafkalib.producer.AsyncProducer;
import wavebridge.kafkalib.producer.SyncProducer;
import wavebridge.kafkalib.producer.TransactionalProducer;
import wavebridge.kafkalib.util.ProducerProperties;
import wavebridge.kafkalib.util.ConsumerProperties;

@SpringBootTest
@ContextConfiguration(classes = KafkaproducerApplication.class)
class KafkaproducerApplicationTests {

  @Test
  public void testConsumerProperties() throws Exception {
    Properties consumerProperties = ConsumerProperties.getConsumerProperties();

    assertEquals("172.16.60.188:9092,172.16.60.225:9092,172.16.60.187:9092", consumerProperties.getProperty("bootstrap.servers"));
    assertEquals("org.apache.kafka.common.serialization.StringSerializer", consumerProperties.getProperty("key.serializer"));
    assertEquals("org.apache.kafka.common.serialization.StringSerializer", consumerProperties.getProperty("value.serializer"));
    assertEquals("data-loader", consumerProperties.getProperty("group.id"));
    assertEquals("40000", consumerProperties.getProperty("heartbeat.interval.ms"));
    assertEquals("120000", consumerProperties.getProperty("session.timeout.ms"));
    assertEquals("none", consumerProperties.getProperty("auto.offset.reset"));
    assertEquals("range", consumerProperties.getProperty("partition.assignment.strategy"));
    assertEquals("topic-udp-ccxt-balance", ConsumerProperties.getTopicName());
  }
 
  public class prodceToTestConsumer implements Runnable {
    @Override
    public void run() {
      AsyncProducer producer = AsyncProducer.getInstance();
      // SyncProducer producer = SyncProducer.getInstance();
      // TransactionalProducer transactionalProducer = TransactionalProducer.getInstance();
      int cnt = 0;
      try {
        while(cnt < 1000) {
          producer.sendUserDataAsync(String.valueOf(++cnt), "message : " + cnt + " / Mesage can be objects.");
          Thread.sleep(100);
        } 
      }
      catch(Exception e) {}
      finally {producer.close();}
    }
  }

  @Test
  // Auto-commit 모드 : 백그라운드로 일정 메세지 간격으로 자동 커밋
  void testConsumer() throws Exception {
    Thread t1 = new Thread(new prodceToTestConsumer());
    t1.start();
    AutoCommitConsumer autoCommitConsumer = AutoCommitConsumer.getInstance();
    while(true) {
      autoCommitConsumer.pollWithoutCommit();
    }
  }

  @Test
  // Async Commit 모드 : 브로커로 commit 요청 후 응답을 기다리지 않음
  void testAsyncCommitConsumer() throws Exception {

  }

  @Test
  // Sync commit 모드 : 브로커로 commit 요청 후 응답을 기다림.
  void testSyncCommitConsumer() throws Exception {

  }
}