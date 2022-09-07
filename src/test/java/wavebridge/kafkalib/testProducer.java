package wavebridge.kafkalib;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
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
class testProducer {

	@Test
  public void testProducerProperties() throws Exception {
    Properties props1 = ProducerProperties.getProducerProperties();

    System.out.println(props1.getProperty("bootstrap.servers"));
    System.out.println(props1.getProperty("default.topic"));
    System.out.println(props1.getProperty("key.serializer"));
    System.out.println(props1.getProperty("value.serializer"));
    System.out.println(props1.getProperty("acks"));
    System.out.println(props1.getProperty("linger.ms"));
  }

  public class NonTranProducer implements Runnable {
    @Override
    public void run() {
      SyncProducer producer = SyncProducer.getInstance();
      int cnt = 0;
      try {
        while(cnt<500) {
          producer.sendUserDataSync(String.valueOf(++cnt % 2), "message : " + cnt + " / Mesage can be objects.");
          Thread.sleep(10);
        }
      }
      catch(Exception e) {}
      finally {
        producer.close();
      }
    }
  }

  public class TranProducer implements Runnable {
    @Override
    public void run() {
      TransactionalProducer producer = TransactionalProducer.getInstance();
      int cnt = 0;
      try {
        while(cnt<1000) {
          producer.sendUserDataCommit(String.valueOf(++cnt % 2), "message : " + cnt + " / Mesage can be objects.");
          Thread.sleep(50);
        }
      }
      catch(Exception e) {}
      finally {
        producer.close();
      }
    }
  }

  @Test
  // non-tran 프로듀서 쓰레드 테스트
  public void testNonTranProducerThread() throws Exception {
    Thread t1 = new Thread(new NonTranProducer());
    t1.start();
    Thread t2 = new Thread(new NonTranProducer());
    t2.start();
    Thread t3 = new Thread(new NonTranProducer());
    t3.start();
    Thread t4 = new Thread(new NonTranProducer());  
    t4.start();
    Thread t5 = new Thread(new NonTranProducer());
    t5.start();
    Thread.sleep(100000000);
  }

  @Test
  // At-Least Once : 매세지 중복 X, 메세지 유실 가능
  public void testAsyncProducer() throws Exception {
    AsyncProducer asyncProducer = AsyncProducer.getInstance();
    int cnt = 0;
    while(cnt < 1000) {
      asyncProducer.sendUserDataAsync(String.valueOf(++cnt % 2), "message : " + cnt + " / Mesage can be objects.");
      // Thread.sleep(5);
    }
    asyncProducer.close();
  }

  @Test
  // At-most Once : 메세지 중복 가능, 메세지 유실은 없음
  public void testSyncProducer() throws Exception {
    SyncProducer syncProducer = SyncProducer.getInstance();
    int cnt = 0;
    while(cnt < 1000) {
      syncProducer.sendUserDataSync(String.valueOf(++cnt), "message : " + cnt + " / Mesage can be objects.");
      // Thread.sleep(100);
    }
    syncProducer.close();
  }

  @Test
  // Exactly Once : 메세지 중복 없음, 메세지 유실 없음.(재시도)
  // 브로커의 트랜잭션 코디네이터와 프로듀서 간 트랙잭션 정보를 교환.(느림)
  // 메세지 전송 요청 시 마다 producer 를 생성 후 close 함.
  public void testTransactionalProducer() throws Exception {
    TransactionalProducer transactionalProducer = TransactionalProducer.getInstance();
    int cnt = 0;
    while(cnt < 100) {
      transactionalProducer.sendUserDataCommit(String.valueOf(++cnt), "message : " + cnt + " / Mesage can be objects.");
      // Thread.sleep(100);
    }
  }
}