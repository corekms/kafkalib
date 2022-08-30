package wavebridge.kafkalib.util;

import java.time.Duration;
import java.util.Properties;
import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix="consumer")
public class ConsumerProperties {
  private String bootstrapServers;
	private String keyDeserializer;
	private String valueDeserializer;
  private String groupId;
  private String heartbeatIntervalms;
  private String sessionTimeoutMs;
  private String autoOffsetReset;
  
  static private String TOPIC_NAME;
  static private Duration POLLING_DURATION_MS;
  static private final Properties PROP = new Properties();

  @PostConstruct
  void initProperties() {
    PROP.setProperty("bootstrap.servers", this.bootstrapServers);
    PROP.setProperty("key.deserializer", this.keyDeserializer);
    PROP.setProperty("value.deserializer",this.valueDeserializer);
    PROP.setProperty("group.id",this.groupId);
    PROP.setProperty("heartbeat.interval.ms",this.heartbeatIntervalms);
    PROP.setProperty("session.timeout.ms",this.sessionTimeoutMs);
    PROP.setProperty("auto.offset.reset",this.autoOffsetReset);
  }

  public static Properties getAutoCommitConsumerProperties() {
    PROP.setProperty("enable.auto.commit", "true");
    return PROP;
  }

  public static Properties getManualCommitConsumerProperties() {
    PROP.setProperty("enable.auto.commit", "false");
    return PROP;
  }

  public static Properties getTransactionalConsumerProperties() {
    PROP.setProperty("isolation.level", "read_committed");
    return PROP;
  }

  public static String getTopicName() {
    return TOPIC_NAME;
  }

  @Value("${consumer.topic-name}")
  public void setTopicName(String topicName) {
    ConsumerProperties.TOPIC_NAME = topicName;
  }

  public static Duration getPollingIntervalMs() {
    return POLLING_DURATION_MS;
  }

  @Value("${consumer.polling-duration-ms}") 
  public void setPollingIntervalMs(Duration pollingIntervalMs){
    ConsumerProperties.POLLING_DURATION_MS = pollingIntervalMs;
  }

  public String getBootstrapServers() {
    return this.bootstrapServers;
  }

  @Value("${consumer.bootstrap-servers}")
  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public String getKeyDeserializer() {
    return this.keyDeserializer;
  }

  @Value("${consumer.key-deserializer}")
  public void setKeyDeserializer(String keyDeserializer) {
    this.keyDeserializer = keyDeserializer;
  }

  public String getValueDeserializer() {
    return this.valueDeserializer;
  }

  @Value("${consumer.value-deserializer}")
  public void setValueDeserializer(String valueDeserializer) {
    this.valueDeserializer = valueDeserializer;
  }

  public String getGroupId() {
    return this.groupId;
  }

  @Value("${consumer.group-id}")
  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public String getHeartbeatIntervalms() {
    return this.heartbeatIntervalms;
  }

  @Value("${consumer.heartbeat-interval-ms}")
  public void setHeartbeatIntervalms(String heartbeatIntervalms) {
    this.heartbeatIntervalms = heartbeatIntervalms;
  }

  public String getSessionTimeoutMs() {
    return this.sessionTimeoutMs;
  }

  @Value("${consumer.session-timeout-ms}")
  public void setSessionTimeoutMs(String sessionTimeoutMs) {
    this.sessionTimeoutMs = sessionTimeoutMs;
  }

  public String getAutoOffsetReset() {
    return this.autoOffsetReset;
  }

  @Value("${consumer.auto-offset-reset}")
  public void setAutoOffsetReset(String autoOffsetReset) {
    this.autoOffsetReset = autoOffsetReset;
  }
}
