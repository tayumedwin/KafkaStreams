package kafka.reset;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import kafka.constants.Constants;

public class ResetLocal {

    public static void main(final String[] args) throws Exception {
        // Kafka Streams configuration
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, Constants.CONFIG_ID_INPUT_STREAM);
        // make sure to consume the complete topic via "auto.offset.reset = earliest"
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.LOCAL_HOST_PORT);
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // define the processing topology
        final KStreamBuilder builder = new KStreamBuilder();
        builder.stream(Constants.INPUT_TOPIC)
            .selectKey((ignoredKey, word) -> word)
            .through(Constants.THROUGH_TOPIC)
            //.countByKey("global-count")
            .to(Constants.OUTPUT_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        
        // Delete the application's local state.
        // Note: In real application you'd call `cleanUp()` only under certain conditions.
        // See Confluent Docs for more details:
        // https://docs.confluent.io/current/streams/developer-guide/app-reset-tool.html#step-2-reset-the-local-environments-of-your-application-instances
        streams.cleanUp();

        streams.start();
        
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
        }));

    }

}