package wavebridge.kafkalib.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProducerCallback implements Callback {
private ProducerRecord<String, Object> record;

  public ProducerCallback(ProducerRecord<String, Object> record) {
    this.record = record;
  }

  @Override
  public void onCompletion(RecordMetadata metadata, Exception e) {
    if (e != null) {
      log.error("[Callback error]Topic: {}, Partition: {}, Offset: {}, Key: {}, Received Message: {}\n", 
        metadata.topic(), metadata.partition(), metadata.offset(), record.key(), record.value());
    }
  }
}