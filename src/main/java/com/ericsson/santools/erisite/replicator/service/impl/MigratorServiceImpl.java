package com.ericsson.santools.erisite.replicator.service.impl;

import static java.text.MessageFormat.format;

import com.ericsson.santools.erisite.replicator.service.MigratorService;
import java.text.MessageFormat;
import java.util.Properties;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class MigratorServiceImpl implements MigratorService {

  public static final Logger LOG = LoggerFactory.getLogger(MigratorServiceImpl.class);

  private final String bootstrapServer;
  private final String applicationGroupId;
  private final String currentTopic;
  private final String newTopic;

  public MigratorServiceImpl(@Value("${kafka.bootstrap.servers}") String bootstrapServer,
      @Value("${kafka.application.group.id}") String applicationGroupId,
      @Value("${kafka.current.topic}") String currentTopic,
      @Value("${kafka.new.topic}") String newTopic) {
    this.bootstrapServer = bootstrapServer;
    this.applicationGroupId = applicationGroupId;
    this.currentTopic = currentTopic;
    this.newTopic = newTopic;
  }

  @PostConstruct
  @Override
  public void migrate() {
    if (LOG.isInfoEnabled()) {
      LOG.info(format("BootstrapServers: {0}", bootstrapServer));
      LOG.info(format("ApplicationGroupId: {0}", applicationGroupId));
      LOG.info(format("Read From Topic: {0}", currentTopic));
      LOG.info(format("Write to Topic: {0}", newTopic));
    }
    runKafkaStream();
  }

  private Properties getKafkaConfiguration(){
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationGroupId);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    return config;
  }

  private void runKafkaStream(){
    try {
      StreamsBuilder builder = new StreamsBuilder();
      KStream<String, String> stream = builder.stream(currentTopic);
      stream
          .peek(this::showMessage)
          .to(newTopic);
      KafkaStreams streams = new KafkaStreams(builder.build(), getKafkaConfiguration());
      streams.start();
      Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    } catch (Exception ex) {
      LOG.error("It was impossible to migrate the topic due to: "+ex.getLocalizedMessage(), ex);
    }
  }

  private void showMessage(String key, String value){
    if (LOG.isInfoEnabled()) {
      LOG.info(format("Adding message ==> key: {0}, value: {1}", key, value));
    }
  }

}
