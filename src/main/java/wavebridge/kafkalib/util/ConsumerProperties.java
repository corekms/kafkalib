package wavebridge.kafkalib.util;

import java.util.Properties;
import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix="consumer")
public class ConsumerProperties {
  private String bootstrapServers;
	private String keySerializer;
	private String valueSerializer;
  private String groupId;
  private String heartbeatIntervalms;
  private String sessionTimeoutMs;
  private String autoOffsetReset;
  private String partitionAssignmentStrategy; 

  static private String TOPIC_NAME;
  static private Properties PROP = new Properties();

  @PostConstruct
  void initProperties() {
    PROP.setProperty("bootstrap.servers", this.bootstrapServers);
    PROP.setProperty("key.serializer", this.keySerializer);
    PROP.setProperty("value.serializer",this.valueSerializer);
    PROP.setProperty("group.id",this.groupId);
    PROP.setProperty("heartbeat.interval.ms",this.heartbeatIntervalms);
    PROP.setProperty("session.timeout.ms",this.sessionTimeoutMs);
    PROP.setProperty("auto.offset.reset",this.autoOffsetReset);
    PROP.setProperty("partition.assignment.strategy",this.partitionAssignmentStrategy);
  }

  public String getBootstrapServers() {
    return this.bootstrapServers;
  }

  @Value("${consumer.bootstrap-servers}")
  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }
}
