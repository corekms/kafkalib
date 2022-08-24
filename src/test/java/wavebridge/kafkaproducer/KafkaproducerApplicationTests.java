package wavebridge.kafkaproducer;

import java.util.Properties;
// import org.springframework.boot.test.context.SpringBootTest;
import org.junit.jupiter.api.Test;

import wavebridge.kafkaproducer.util.ProducerConfig;

// @SpringBootTest
class KafkaproducerApplicationTests {

	@Test
  public void testPropertiesValues() throws Exception {
    Properties props1 = ProducerConfig.getProperties();

    System.out.println(props1.getProperty("bootstrap.servers"));
    System.out.println(props1.getProperty("default.topic"));
    System.out.println(props1.getProperty("key.serializer"));
    System.out.println(props1.getProperty("value.serializer"));
    System.out.println(props1.getProperty("acks"));
    System.out.println(props1.getProperty("linger.ms"));
  }

  @Test
  public void testSyncProducer() throws Exception {
    UserDataProducer userDataProducer1 = UserDataProducer.getInstance();
    
    int cnt = 0;
    while(cnt < 2) {
      userDataProducer1.sendUserDataAsync(String.valueOf(++cnt), "message : " + cnt + " / Mesage can be objects.");
      Thread.sleep(1000);
    }
    userDataProducer1.close();

  }
}