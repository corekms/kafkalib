package wavebridge.kafkalib.util;

import java.util.Properties;
import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@ConfigurationProperties(prefix="producer")
public class ProducerProperties {
	// non-transactional producer 설정
	private String bootstrapServers;
	private String acks;
	private String keySerializer;
	private String valueSerializer;
	// transactional producer 설정
	private String enableIdempotence;
	private String maxInFlightRequestsPerConnection;
	private String retries;
	private String transactionalId;

	static private String TOPIC_NAME;
	static private Properties PROP = new Properties();
	static private Properties TRANSACTIONAL_PROP = new Properties();

	// BEAN 생성 및 의존성 주입 완료 후 딱 한번 실행되는 메소드
	@PostConstruct
	void initProperties() {
    PROP.setProperty("bootstrap.servers", this.bootstrapServers);
    PROP.setProperty("acks", this.acks);
    PROP.setProperty("key.serializer", this.keySerializer);
    PROP.setProperty("value.serializer",this.valueSerializer);
		/*
		 * transactional producer 생성을 위한 필수파라메터
		 * acks : all
		 * enable.idempotence : true
		 * transaction.id : 트랜잭션 식별자가 중복되지 않도록 kafka API 에서 자동으로 생성되는 UUID 앞에 붙는 식별자. 
		 */
		TRANSACTIONAL_PROP = (Properties)PROP.clone();
    TRANSACTIONAL_PROP.setProperty("enable.idempotence", this.enableIdempotence);
    TRANSACTIONAL_PROP.setProperty("max.in.flight.requests.per.connection", maxInFlightRequestsPerConnection);
    TRANSACTIONAL_PROP.setProperty("retries", retries);
    TRANSACTIONAL_PROP.setProperty("transactional.id", this.getTransactionIdPrefix());
	}

	public static Properties getProducerProperties() {
		return PROP;
	}

	public static Properties getTransactionalProperties() {
		return TRANSACTIONAL_PROP;
	}

	public String getBootStrapServers() {
		return bootstrapServers;
	}

	@Value("${producer.bootstrap-servers}")
	public void setBootStrapServers(String bootStrapServer) {
		this.bootstrapServers = bootStrapServer;
	}

	public String getAcks() {
		return acks;
	}

	@Value("${producer.acks}")
	public void setAcks(String acks) {
		this.acks = acks;
	}

	public String getKeySerializer() {
		return keySerializer;
	}

	@Value("${producer.key-serializer}")
	public void setKeySerializer(String keySerializer) {
		this.keySerializer = keySerializer;
	}

	public String getValueSerializer() {
		return valueSerializer;
	}

	@Value("${producer.value-serializer}")
	public void setValueSerializer(String valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	/*
	 * transactional-producer
	 */
	public String getenableIdempotence() {
		return enableIdempotence;
	}

	@Value("${producer.enable-idempotence}")
	public void setenableIdempotence(String enableIdempotence) {
		this.enableIdempotence = enableIdempotence;
	}

	public String getMaxInFlightRequestsPerConnection() {
		return maxInFlightRequestsPerConnection;
	}

	@Value("${producer.max-in-flight-requests-per-connection}")
	public void setMaxInFlightRequestsPerConnection(String maxInFlightRequestsPerConnection) {
		this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
	}

	public String getretries() {
		return retries;
	}

	@Value("${producer.retries}")
	public void setretries(String retries) {
		this.retries = retries;
	}

	public String getTransactionIdPrefix() {
		return transactionalId;
	}

	@Value("${producer.transaction-id-prefix}")
	public void setTransactionIdPrefix(String transactionalIdPrefix) {
		this.transactionalId = transactionalIdPrefix;
	}

	public static String getTopicName() {
		return TOPIC_NAME;
	}

	@Value("${producer.topic-name}")
	public void setTopicName(String topicName) {
		ProducerProperties.TOPIC_NAME = topicName;
	}
}