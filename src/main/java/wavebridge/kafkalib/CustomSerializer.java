package wavebridge.kafkalib;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomSerializer implements Serializer<Object> {

  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, Object data) {
    try {
      return objectMapper.writeValueAsBytes(data);
    } catch (Exception e) {
      log.error("Unable to serialize object {}", data, e);
      return null;
    }
  }

  @Override
  public void close() {
  }
}
