package wavebridge.kafkalib;

import java.util.Properties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.junit.jupiter.api.Test;

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
  public void testSyncProducer() throws Exception {
    messageProducer userDataProducer = messageProducer.getInstance();

    int cnt = 0;
    while(cnt < 5) {
      userDataProducer.sendUserDataSync(String.valueOf(++cnt), "message : " + cnt + " / Mesage can be objects.");
      userDataProducer.sendUserDataAsync(String.valueOf(cnt), "message : " + cnt + " / Mesage can be objects.");
      userDataProducer.sendUserDataCommit(String.valueOf(cnt), "message : " + cnt + " / Mesage can be objects.");
      Thread.sleep(500);
    }
    userDataProducer.close();

  }
}