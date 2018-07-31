package kafka.demo;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import kafka.constants.Constants;


public class KStreamJoin {
	
	final Long JOIN_WINDOW = TimeUnit.SECONDS.toMillis(60);
	
	public KStreamJoin() throws InterruptedException, ExecutionException {
		// TODO Auto-generated constructor stub
		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, Constants.CONFIG_ID_INPUT_STREAM);
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.LOCAL_HOST_PORT);
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		// The commit interval for flushing records to state stores and downstream must
		// be lower than
		// this test's timeout (30 secs) to ensure we observe the expected
		// processing results.
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, TimeUnit.SECONDS.toMillis(10));
		streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		streamsConfiguration.put("enable.auto.commit", "false");
		
		KStreamBuilder builder = new KStreamBuilder();
		
		//KStream<String, String> left =  builder.stream("left");
		KStream<String, String> left =  builder.stream(Serdes.String(), Serdes.String(), "left");
		KStream<String, String> right = builder.stream(Serdes.String(), Serdes.String(), "right");
		
		KStream<String, String> joined = left.join(right,
			    (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
			    JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
			    Joined.with(
			      Serdes.String(), /* key */
			      Serdes.String(),   /* left value */
			      Serdes.String())  /* right value */
			  );
		
		
		joined.to(Serdes.String(), Serdes.String(), "joined");

		KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
		streams.start();
		produce();
		streams.close();

	}
	
	public void produce() throws InterruptedException, ExecutionException {
		//
		// Step 2: Produce some input data to the input topic.
		//
		Properties producerConfig = new Properties();
		/*
		 * producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
		 * Constants.LOCAL_HOST_PORT); producerConfig.put(ProducerConfig.ACKS_CONFIG,
		 * "all"); // producerConfig.put("enable.auto.commit", "false");
		 * producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
		 * producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
		 * StringSerializer.class);
		 * producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
		 * StringSerializer.class);
		 */
		// Transactional Config
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.LOCAL_HOST_PORT);
		producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "transactional-producer");
		producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // enable idempotence
		//producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-transactional-id"); // set transaction id
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerConfig.put("enable.auto.commit", "false");
		Producer<String, String> producer = new KafkaProducer<String, String>(producerConfig);

		long time = System.currentTimeMillis();


		Long elapsedTime = System.currentTimeMillis() - time;
		
		Long delay = TimeUnit.SECONDS.toMillis(10);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>("left", "4", "test9");
		RecordMetadata metadata = producer.send(record).get();
		System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key(),
				record.value(), metadata.partition(), metadata.offset(), elapsedTime);
		
		try {
			Thread.sleep(delay);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		ProducerRecord<String, String> record2 = new ProducerRecord<String, String>("right", "5", "test10");
		RecordMetadata metadata2 = producer.send(record2).get();
		System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record2.key(),
				record2.value(), metadata2.partition(), metadata2.offset(), elapsedTime);

			
		producer.close();

		
	}
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		new KStreamJoin();
	}

}
