package wavebridge.kafkalib;

import java.util.Properties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.junit.jupiter.api.Test;

import wavebridge.kafkalib.producer.AsyncProducer;
import wavebridge.kafkalib.producer.SyncProducer;
import wavebridge.kafkalib.producer.TransactionalProducer;
import wavebridge.kafkalib.util.ProducerProperties;

@SpringBootTest
@ContextConfiguration(classes = KafkaproducerApplication.class)
class KafkaproducerApplicationTests {

	@Test
  public void testPropertiesValues() throws Exception {
    Properties props1 = ProducerProperties.getProducerProperties();

    System.out.println(props1.getProperty("bootstrap.servers"));
    System.out.println(props1.getProperty("default.topic"));
    System.out.println(props1.getProperty("key.serializer"));
    System.out.println(props1.getProperty("value.serializer"));
    System.out.println(props1.getProperty("acks"));
    System.out.println(props1.getProperty("linger.ms"));
  }

  @Test
  public void testProducer() throws Exception {
    AsyncProducer asyncProducer = AsyncProducer.getInstance();

    int cnt = 0;
    while(cnt < 5) {
      asyncProducer.sendUserDataAsync(String.valueOf(++cnt), "message : " + cnt + " / Mesage can be objects.");
      Thread.sleep(1000);
    }
    asyncProducer.close();

    SyncProducer syncProducer = SyncProducer.getInstance();

    cnt = 0;
    while(cnt < 5) {
      syncProducer.sendUserDataSync(String.valueOf(++cnt), "message : " + cnt + " / Mesage can be objects.");
      // Thread.sleep(100);
    }
    syncProducer.close();

    TransactionalProducer transactionalProducer = TransactionalProducer.getInstance();

    cnt = 0;
    while(cnt < 5) {
      transactionalProducer.sendUserDataCommit(String.valueOf(++cnt), "message : " + cnt + " / Mesage can be objects.");
      // Thread.sleep(100);
    }

  }
}