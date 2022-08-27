package wavebridge.kafkalib.consumer;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import wavebridge.kafkalib.util.ConsumerProperties;

public class AsyncConsumer {
  private static final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ConsumerProperties.getConsumerProperties());
  private static class InstanceHoler {
    public static AutoCommitConsumer consmuerInstance = new AutoCommitConsumer();
  }

  public static AutoCommitConsumer getInstance() {
    consumer.subscribe(Arrays.asList(ConsumerProperties.getTopicName()));
    return InstanceHoler.consmuerInstance;
  }
  
}
