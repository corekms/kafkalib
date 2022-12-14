package wavebridge.kafkalib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import java.util.Properties;

import wavebridge.kafkalib.consumer.AutoCommitConsumer;
import wavebridge.kafkalib.consumer.ManualCommitConsumer;
import wavebridge.kafkalib.consumer.TransactionalConsumer;
import wavebridge.kafkalib.producer.SyncProducer;
import wavebridge.kafkalib.util.ConsumerProperties;

@SpringBootTest
@ContextConfiguration(classes = KafkaproducerApplication.class)
class testConsumer {

  @Test
  public void testConsumerProperties() throws Exception {
    Properties consumerProperties = ConsumerProperties.getAutoCommitConsumerProperties();
    // Properties consumerProperties = ConsumerProperties.getManualCommitConsumerProperties();
    // Properties consumerProperties = ConsumerProperties.getTransactionalConsumerProperties();

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
 
  public class nonTranProducer implements Runnable {
    @Override
    public void run() {
      SyncProducer producer = SyncProducer.getInstance();
      int cnt = 0;
      try {
        while(true) {
          producer.sendUserDataSync(String.valueOf(++cnt % 2), "message : " + cnt + " / Mesage can be objects.");
          Thread.sleep(50);
        }
      }
      catch(Exception e) {}
      finally {
        producer.close();
      }
    }
  }

  // Auto-commit ?????? : ?????????????????? ?????? ????????? ???????????? ?????? ??????
  @Test
  void testAutoCommitConsumer() throws Exception {
    // Thread t1 = new Thread(new nonTranProducer());
    // t1.start();
    AutoCommitConsumer autoCommitConsumer = AutoCommitConsumer.getInstance();
    try {
      while(true) {
        autoCommitConsumer.fetchMessage();
      }
    } catch(Exception e) {}
    finally {
      autoCommitConsumer.close();
    }
  }

  /*
   * Async Commit ?????? : ???????????? commit ?????? ??? ????????? ???????????? ??????.(Non-blocking Method)
   * ACK ???????????? ????????? pass / ??????????????? ?????? callback????????? ??????.
   * pass ????????? ?????? commit ????????? ?????? ???????????? ??????.
   * Sync commit ?????? : ???????????? commit ?????? ??? ????????? ?????????.(Blocking-Method)
   */
  @Test
  void testManualCommitConsumer() throws Exception {
    // Thread t1 = new Thread(new tranProducer());
    // t1.start();
    ManualCommitConsumer manualCommitConsumer = ManualCommitConsumer.getInstance();
    try {
      while(true) {
        manualCommitConsumer.fetchMessage();
        manualCommitConsumer.commitConsumer(true);
      }
    } catch(Exception e) {}
    finally {
      manualCommitConsumer.close();
    }
  }

  // ACK ????????? ???????????? ????????? Exception ??????
  @Test
  void testTransactionalConsumer() throws Exception {
    // Thread t1 = new Thread(new tranProducer());
    // t1.start();
    TransactionalConsumer transactionalConsumer = TransactionalConsumer.getInstance();
    try {
      while(true) {
        transactionalConsumer.fetchMessage(true);
      }
    } catch(Exception e) {}
    finally {
      transactionalConsumer.close();
    }
  }
}