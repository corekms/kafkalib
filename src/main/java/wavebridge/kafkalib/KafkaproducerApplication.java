package wavebridge.kafkalib;

import org.apache.kafka.common.errors.WakeupException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import wavebridge.kafkalib.consumer.AutoCommitConsumer;
import wavebridge.kafkalib.consumer.ManualCommitConsumer;
import wavebridge.kafkalib.consumer.TransactionalConsumer;

@SpringBootApplication
class KafkaproducerApplication {
  public static void main(String[] args) throws Exception {
    
    SpringApplication.run(KafkaproducerApplication.class, args);
    // AutoCommitConsumer Consumer = AutoCommitConsumer.getInstance();
    // ManualCommitConsumer Consumer = ManualCommitConsumer.getInstance();
    TransactionalConsumer Consumer = TransactionalConsumer.getInstance();
    while(true) {
      Consumer.fetchMessage();
    }
  }
}