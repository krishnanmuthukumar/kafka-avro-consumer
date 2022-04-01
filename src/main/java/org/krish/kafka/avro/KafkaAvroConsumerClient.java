package org.krish.kafka.avro;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class KafkaAvroConsumerClient {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("Please provide command line arguments: configPath topic");
			System.exit(1);
		}

		final String topic = args[1];
		final Properties props = loadConfig(args[0]);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		final KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));

		try {
			while (true) {
				final ConsumerRecords<String, Customer> records = consumer.poll(100);
				for (final ConsumerRecord<String, Customer> record : records) {
					final String key = record.key();
					final Customer value = record.value();
					System.out.printf("Consumed record with key %s and value %s", key, value);
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			consumer.close();
		}

	}

	public static Properties loadConfig(final String configFile) throws IOException {
		if (!Files.exists(Paths.get(configFile))) {
			throw new IOException(configFile + " not found.");
		}
		final Properties cfg = new Properties();
		try (InputStream inputStream = new FileInputStream(configFile)) {
			cfg.load(inputStream);
		}
		return cfg;
	}
}
