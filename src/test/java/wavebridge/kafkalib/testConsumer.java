package wavebridge.kafkalib;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.junit.jupiter.api.Test;

import wavebridge.kafkalib.consumer.AutoCommitConsumer;
import wavebridge.kafkalib.consumer.ManualCommitConsumer;
import wavebridge.kafkalib.producer.SyncProducer;
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
 
  public class produceToTestConsumer implements Runnable {
    @Override
    public void run() {
      SyncProducer producer = SyncProducer.getInstance();
      // SyncProducer producer = SyncProducer.getInstance();
      // TransactionalProducer transactionalProducer = TransactionalProducer.getInstance();
      int cnt = 0;
      try {
        while(cnt < 1000) {
          producer.sendUserDataSync(String.valueOf(++cnt), "message : " + cnt + " / Mesage can be objects.");
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
    int i=0;
    Thread t1 = new Thread(new produceToTestConsumer());
    t1.start();
    AutoCommitConsumer autoCommitConsumer = AutoCommitConsumer.getInstance();
    do {
      autoCommitConsumer.pollAutoCommit();
      i++;
    } while(i<1000);
    autoCommitConsumer.close();
  }

  @Test
  // Async Commit 모드 : 브로커로 commit 요청 후 응답을 기다리지 않음.(Non-blocking Method)
  // ACK 수신하지 못해도 pass / 예외처리는 별도 callback으로도 가능.
  // pass 일경우 다음 commit 요청에 대한 응답으로 갈음.
  void testManualCommitConsumer() throws Exception {
    Thread t1 = new Thread(new produceToTestConsumer());
    t1.start();
    ManualCommitConsumer manualCommitConsumer = ManualCommitConsumer.getInstance();
    while(true) {
      manualCommitConsumer.pollAndCommit(false);
    }
  }

  @Test
  // Sync commit 모드 : 브로커로 commit 요청 후 응답을 기다림.(Blocking-Method)
  // ACK 신호를 수신하지 못하면 Exception 발생
  void testTransactionalConsumer() throws Exception {

  }
}