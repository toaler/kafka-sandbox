package brokerage;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.util.Collections;
import java.util.Properties;

public class KafkaCommandPatternExample {

    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String COMMANDS_TOPIC = "commands";

    public static void main(String[] args) {
        // Kafka producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
            // Send commands
            sendCommand(producer, "create_order", "{\"order_id\": \"123\", \"items\": [\"item1\", \"item2\"], \"customer_id\": \"customer_456\"}");
            sendCommand(producer, "update_order", "{\"order_id\": \"123\", \"status\": \"shipped\"}");
            sendCommand(producer, "cancel_order", "{\"order_id\": \"123\"}");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Kafka consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "command_consumer_group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(COMMANDS_TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    // Process the command based on its type
                    String commandType = record.key();
                    String commandData = record.value();

                    switch (commandType) {
                        case "create_order":
                            System.out.println("Creating order: " + commandData);
                            // Execute logic for creating an order
                            break;
                        case "update_order":
                            System.out.println("Updating order: " + commandData);
                            // Execute logic for updating an order
                            break;
                        case "cancel_order":
                            System.out.println("Canceling order: " + commandData);
                            // Execute logic for canceling an order
                            break;
                        default:
                            System.out.println("Unknown command: " + commandData);
                            break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void sendCommand(Producer<String, String> producer, String type, String data) {
        producer.send(new ProducerRecord<>(COMMANDS_TOPIC, type, data));
    }
}
