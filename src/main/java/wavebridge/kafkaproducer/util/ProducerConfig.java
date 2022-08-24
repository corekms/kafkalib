package wavebridge.kafkaproducer.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

/**
 * The type ProducerConfig is a class that represents the value stored
 * in the properties file named config.properties.
 */
@Slf4j
public class ProducerConfig {
  public static Properties getProperties()  {
    Properties props = new Properties();
    //try to load the file config.properties
    try (InputStream input = ProducerConfig.class.getClassLoader().getResourceAsStream("producer.properties")) {
      if (input == null) {
        // throw new Exception("Sorry, unable to find config.properties");
        log.error("Failed to find config.properties. Aborting startup!!!");
        System.exit(1);
      }

      //load a properties file from class path, inside static method
      props.load(input);
    } catch (IOException ex) {
      log.error("Failed to create properties due to IOException. Aborging startup!!!");
      ex.printStackTrace();
      System.exit(1);
      
    }
    
    return props;
  }
}