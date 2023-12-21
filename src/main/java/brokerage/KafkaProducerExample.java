package brokerage;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerExample {

	private static final String BOOTSTRAP_SERVERS = "localhost:29092";
	private static final String TOPIC_NAME = "bar";

	public static void main(String[] args) throws InterruptedException {
		InetAddress localHost = null;

		try {
			localHost = InetAddress.getLocalHost();
			String hostname = localHost.getHostName();
			System.out.println("Hostname: " + hostname);
		} catch (UnknownHostException e) {
			System.out.println("Could not find hostname: " + e.getMessage());
		}

		createTopicIfNotExists();

		// Kafka producer properties
		Properties props = new Properties();
		props.put(ProducerConfig.CLIENT_ID_CONFIG, localHost.toString());
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

		// Create Kafka producer
		try (Producer<String, String> producer = new KafkaProducer<>(props)) {
			// Produce a sample message to a topic
			String message = "Hello, Kafka!";

			// Null key, using default partition record sent to random partition
			ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, message);

			while (true) {

				// Asynchronous example
				System.out.println(Thread.currentThread().getName() + " Asynchronously sending message");
				producer.send(record, (metadata, exception) -> {
					if (exception == null) {
						System.out.println("[" + Thread.currentThread().getName()
								+ "] Message sent successfully! Topic: " + metadata.topic() + ", Partition: "
								+ metadata.partition() + ", Offset: " + metadata.offset());
					} else {
						System.err.println("Error sending message: " + exception.getMessage());
					}
				});
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void createTopicIfNotExists() throws InterruptedException {
		try (AdminClient adminClient = createAdminClient()) {
			NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
			adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
		} catch (ExecutionException e) {
		}
	}

	private static AdminClient createAdminClient() {
		Properties adminProps = new Properties();
		adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		return AdminClient.create(adminProps);
	}
}
