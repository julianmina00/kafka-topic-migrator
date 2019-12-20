package com.ericsson.santools.erisite.replicator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaTopicReplicatorApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaTopicReplicatorApplication.class, args);
	}

}
