package kafka.dedupe;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import kafka.constants.Constants;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * This demonstrates how to remove duplicate records from an input stream.
 *
 * Here, a stateful {@link org.apache.kafka.streams.kstream.Transformer} (from
 * the Processor API) detects and discards duplicate input records based on an
 * "event id" that is embedded in each input record. This transformer is then
 * included in a topology defined via the DSL.
 *
 * In this simplified example, the values of input records represent the event
 * ID by which duplicates will be detected. In practice, record values would
 * typically be a more complex data structure, with perhaps one of the fields
 * being such an event ID. De-duplication by an event ID is but one example of
 * how to perform de-duplication in general. The code example below can be
 * adapted to other de-duplication approaches.
 *
 * IMPORTANT: The Apache Kafka project is currently working on supporting
 * exactly-once semantics. Once available, most use cases will no longer need to
 * worry about duplicates or duplicate processing because, typically, such
 * duplicates only happen in the face of failures. That said, there will still
 * be some scenarios where the upstream data producers may generate duplicate
 * events under normal, non-failure conditions; in such cases, an event
 * de-duplication approach as shown in this example is helpful.
 *
 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-129%3A+Streams+Exactly-Once+Semantics
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class Dedupe {

	private static String inputTopic = Constants.INPUT_TOPIC;
	private static String outputTopic = Constants.OUTPUT_TOPIC;

	public Dedupe() throws Exception {
		// TODO Auto-generated constructor stub
		shouldRemoveDuplicatesFromTheInput();
		//produceAndConsume();
	}

	public void shouldRemoveDuplicatesFromTheInput() throws Exception {
		String firstId = UUID.randomUUID().toString(); // e.g. "4ff3cb44-abcb-46e3-8f9a-afb7cc74fbb8"
		String secondId = UUID.randomUUID().toString();
		String thirdId = UUID.randomUUID().toString();
		List<String> inputValues = Arrays.asList(firstId, secondId, firstId, firstId, secondId, thirdId, thirdId,
				firstId, secondId);
		List<String> expectedValues = Arrays.asList(firstId, secondId, thirdId);

		//
		// Step 1: Configure and start the processor topology.
		//
		

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
		// Use a temporary directory for storing state, which will be automatically
		// removed after the test.

		// How long we "remember" an event. During this time, any incoming duplicates of
		// the event
		// will be, well, dropped, thereby de-duplicating the input data.
		//
		// The actual value depends on your use case. To reduce memory and disk usage,
		// you could
		// decrease the size to purge old windows more frequently at the cost of
		// potentially missing out
		// on de-duplicating late-arriving records.
		// long maintainDurationPerEventInMs = TimeUnit.SECONDS.toMillis(10);//converts
		// N seconds into milliseconds
		long maintainDurationPerEventInMs = TimeUnit.MINUTES.toMillis(5);// converts N minutes into milliseconds

		StateStoreSupplier deduplicationStoreSupplier = Stores.create(Constants.EVENT_ID_STORE)
				.withKeys(Serdes.String()) // must
				// match
				// the
				// return
				// type
				// of
				// the
				// Transformer's
				// id
				// extractor
				.withValues(Serdes.Long()).persistent()
				.windowed(maintainDurationPerEventInMs, TimeUnit.MINUTES.toMillis(60), 3, false).build();
		//param1 windowSize - size of the windows
		//param2 retentionPeriod - the maximum period of time in milli-second to keep each window in this store
		//param3 numSegments - the maximum number of segments for rolling the windowed store
		//param4 retainDuplicates - whether or not to retain duplicate data within the window
	
		KStreamBuilder builder = new KStreamBuilder();
		
		builder.addStateStore(deduplicationStoreSupplier);

		KStream<String, String> input = builder.stream(inputTopic);
		KStream<String, String> deduplicated = input.transform(
				// In this example, we assume that the record value as-is represents a unique
				// event ID by
				// which we can perform de-duplication. If your records are different, adapt the
				// extractor
				// function as needed.
				() -> new DeduplicationTransformer<>(maintainDurationPerEventInMs, (key, value) -> value),
				Constants.EVENT_ID_STORE);
		deduplicated.through(Constants.THROUGH_TOPIC);
		deduplicated.to(outputTopic);

		KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
		streams.start();
		//produce();
		consume();
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
		producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-transactional-id"); // set transaction id
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerConfig.put("enable.auto.commit", "false");
		Producer<String, String> producer = new KafkaProducer<String, String>(producerConfig);

		long time = System.currentTimeMillis();

		producer.initTransactions(); // initiate transaction

		try {
			producer.beginTransaction(); // begin transaction

			Long elapsedTime = System.currentTimeMillis() - time;
			ProducerRecord<String, String> record = null;
			RecordMetadata metadata = null;
			Long delay = TimeUnit.SECONDS.toMillis(10);

			for(int txnid=0; txnid <= Constants.MESSAGE_COUNT; txnid++) {
				record = new ProducerRecord<String, String>(inputTopic, new Integer(txnid).toString(), "Monday Kafka");
				metadata = producer.send(record).get();
				
			}

			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			if(Constants.FOR_TXN_COMMIT) {
				System.out.println("Commit Transaction");
				producer.commitTransaction(); //commit
			}else {
				System.out.println("Abort Transaction");
				producer.abortTransaction(); //abort
			}
			
			producer.flush();
			System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key(),
					record.value(), metadata.partition(), metadata.offset(), elapsedTime);
			

		} catch (KafkaException e) {
			System.out.println("Abort Transaction, due to Exception");
			producer.abortTransaction(); //abort
		}

		producer.close();

		
	}
	
	public void consume() {
		//
		// Step 3: Verify the application's output data.
		//
		Properties consumerConfig = new Properties();
		consumerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.LOCAL_HOST_PORT);
		consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.CONFIG_ID_OUTPUT);
		consumerConfig.put("enable.auto.commit", "false");
		consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// props.put("auto.commit.interval.ms", "1000");
		// props.put("session.timeout.ms", "30000");
		consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerConfig);

		consumer.subscribe(Arrays.asList(outputTopic));

		int i = 0;

		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> r : records) {
					// print the offset,key and value for the consumer records.
					/*
					 * System.out.printf("offset = %d, key = %s, value = %s\n", r.offset(), r.key(),
					 * r.value());
					 */

					Map<String, Object> data = new HashMap<>();
					data.put("partition", r.partition());
					data.put("offset", r.offset());
					data.put("value", r.value());
					data.put("key", r.key());
					System.out.println(r.offset() + ": " + data);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// The easiest way to handle commits manually is with the synchronous commit
				// API. when enable.auto.commit is false
				/*
				 * Obviously committing after every message is probably not a great idea for
				 * most use cases since the processing thread has to block for each commit
				 * request to be returned from the server. This would kill throughput. A more
				 * reasonable approach might be to commit after every N messages where N can be
				 * tuned for better performance.
				 */
				consumer.commitSync();
			} catch (CommitFailedException e) {
				e.printStackTrace();
			}
			consumer.close();
		}
	}

}