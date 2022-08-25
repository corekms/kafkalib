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

  /*
   * 용법
   * 
   */
  @Test
  public void testProducer() throws Exception {
    // At-Least Once : 매세지 중복 X, 메세지 유실 가능
    AsyncProducer asyncProducer = AsyncProducer.getInstance();
    int cnt = 0;
    while(cnt < 5) {
      asyncProducer.sendUserDataAsync(String.valueOf(++cnt), "message : " + cnt + " / Mesage can be objects.");
      Thread.sleep(100);
    }
    asyncProducer.close(); // 자원회수 필수

    // At-most Once : 메세지 중복 가능, 메세지 유실은 없음
    SyncProducer syncProducer = SyncProducer.getInstance();
    cnt = 0;
    while(cnt < 5) {
      syncProducer.sendUserDataSync(String.valueOf(++cnt), "message : " + cnt + " / Mesage can be objects.");
      // Thread.sleep(100);
    }
    syncProducer.close();// 자원회수 필수

    // Exactly Once : 메세지 중복 없음, 메세지 유실 없음.(재시도)
    // 브로커의 트랜잭션 코디네이터와 프로듀서 간 트랙잭션 정보를 교환.(느림)
    // 메세지 전송 요청 시 마다 producer 를 생성 후 close 함.
    TransactionalProducer transactionalProducer = TransactionalProducer.getInstance();
    cnt = 0;
    while(cnt < 5) {
      transactionalProducer.sendUserDataCommit(String.valueOf(++cnt), "message : " + cnt + " / Mesage can be objects.");
      // Thread.sleep(100);
    }

  }
}