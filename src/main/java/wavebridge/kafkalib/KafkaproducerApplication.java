package wavebridge.kafkalib;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
class KafkaproducerApplication {
  public static void main(String[] args) throws Exception {
    SpringApplication.run(KafkaproducerApplication.class, args);
  }
}