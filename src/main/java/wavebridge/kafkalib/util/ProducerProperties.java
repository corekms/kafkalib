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
	static private String BOOTSTRAP_SERVERS;
	static private String ACKS;
	static private String KEY_SERIALIZER;
	static private String VALUE_SERIALIZER;

	// transactional producer 설정
	static private String ENABLEIDEMPOTENCE;
	static private String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
	static private String RETRIES;
	static private String TRANSACTIONAL_ID;
	static private String TOPIC_NAME;

	static private Properties PROP = new Properties();
	static private Properties TRANSACTIONAL_PROP = new Properties();

	public ProducerProperties() {
		log.info("===========================================================");
    log.info("ProducerProperties called ...");
    log.info("===========================================================");
	}

	// BEAN 생성 및 의존성 주입 완료 후 딱 한번 실행되는 메소드
	@PostConstruct
	void initProperties() {
		log.info("===========================================================");
    log.info("PostConstruct called ...");
    log.info("===========================================================");
    PROP.setProperty("bootstrap.servers", ProducerProperties.getBootStrapServers());
    PROP.setProperty("acks", ProducerProperties.getAcks());
    PROP.setProperty("key.serializer", ProducerProperties.getKeySerializer());
    PROP.setProperty("value.serializer", ProducerProperties.getValueSerializer());

		/*
		 * transactional producer 생성을 위한 필수파라메터
		 * acks : all
		 * enable.idempotence : true
		 * transaction.id : 트랜잭션 식별자가 중복되지 않도록 kafka API 에서 자동으로 생성되는 UUID 앞에 붙는 식별자. 
		 */
		TRANSACTIONAL_PROP = (Properties)PROP.clone();
    TRANSACTIONAL_PROP.setProperty("enable.idempotence", ProducerProperties.getEnableIdempotence());
    TRANSACTIONAL_PROP.setProperty("max.in.flight.requests.per.connection", ProducerProperties.getMaxInFlightRequestsPerConnection());
    TRANSACTIONAL_PROP.setProperty("retries", ProducerProperties.getRetries());
    TRANSACTIONAL_PROP.setProperty("transactional.id", ProducerProperties.getTransactionIdPrefix());
	}

	public static Properties getProducerProperties() {
		return PROP;
	}

	public static Properties getTransactionalProperties() {
		return TRANSACTIONAL_PROP;
	}
	/*
	 * getter & setter
	 * setter : applcation.yml 설정값을 @Value로 읽어온다.
	 * non-transactional producer
	 */
	public static String getBootStrapServers() {
		return BOOTSTRAP_SERVERS;
	}

	@Value("${producer.bootstrap-servers}")
	public void setBootStrapServers(String bootStrapServer) {
		ProducerProperties.BOOTSTRAP_SERVERS = bootStrapServer;
	}

	public static String getAcks() {
		return ACKS;
	}

	@Value("${producer.acks}")
	public void setAcks(String acks) {
		ProducerProperties.ACKS = acks;
	}

	public static String getKeySerializer() {
		return KEY_SERIALIZER;
	}

	@Value("${producer.key-serializer}")
	public void setKeySerializer(String keySerializer) {
		ProducerProperties.KEY_SERIALIZER = keySerializer;
	}

	public static String getValueSerializer() {
		return VALUE_SERIALIZER;
	}

	@Value("${producer.value-serializer}")
	public void setValueSerializer(String valueSerializer) {
		ProducerProperties.VALUE_SERIALIZER = valueSerializer;
	}

	public static String getEnableIdempotence() {
		return ENABLEIDEMPOTENCE;
	}

	/*
	 * transactional-producer
	 */
	@Value("${producer.enable-idempotence}")
	public void setEnableIdempotence(String enableIdempotence) {
		ProducerProperties.ENABLEIDEMPOTENCE = enableIdempotence;
	}

	public static String getMaxInFlightRequestsPerConnection() {
		return MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
	}

	@Value("${producer.max-in-flight-requests-per-connection}")
	public void setMaxInFlightRequestsPerConnection(String maxInFlightRequestsPerConnection) {
		ProducerProperties.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = maxInFlightRequestsPerConnection;
	}

	public static String getRetries() {
		return RETRIES;
	}

	@Value("${producer.retries}")
	public void setRetries(String retries) {
		ProducerProperties.RETRIES = retries;
	}

	public static String getTransactionIdPrefix() {
		return TRANSACTIONAL_ID;
	}

	@Value("${producer.transaction-id-prefix}")
	public void setTransactionIdPrefix(String transactionalIdPrefix) {
		ProducerProperties.TRANSACTIONAL_ID = transactionalIdPrefix;
	}

	public static String getTopicName() {
		return TOPIC_NAME;
	}

	@Value("${producer.topic-name}")
	public void setTopicName(String topicName) {
		ProducerProperties.TOPIC_NAME = topicName;
	}
}