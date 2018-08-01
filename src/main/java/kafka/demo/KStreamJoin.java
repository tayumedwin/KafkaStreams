package kafka.demo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class KStreamJoin {
	
	final Long JOIN_WINDOW = TimeUnit.SECONDS.toMillis(60);
	
	public KStreamJoin() throws InterruptedException, ExecutionException, IOException {
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
		consume();
		streams.close();

	}
	
	public void produce() throws InterruptedException, ExecutionException, IOException {
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
//		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		producerConfig.put("enable.auto.commit", "false");
		Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(producerConfig);

		long time = System.currentTimeMillis();


		Long elapsedTime = System.currentTimeMillis() - time;
		
		Long delay = TimeUnit.SECONDS.toMillis(10);
		
		Accounts accounts = new Accounts();
		accounts.setId(123);
		accounts.setName("Edwin");
		accounts.setAddress("Philippines");
		accounts.setAge(27);
		
		AccountsBalance accountsbalance = new AccountsBalance();
		accountsbalance.setId(123);
		accountsbalance.setAvailBalance("500.00");
		accountsbalance.setCurrentBalance("500.00");
		
		byte[] input1 = serialize(accounts);
		byte[] input2 = serialize(accountsbalance);
		
		ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>("left", "1a", input1);
		RecordMetadata metadata = producer.send(record).get();
		System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key(),
				record.value(), metadata.partition(), metadata.offset(), elapsedTime);
		
		try {
			Thread.sleep(delay);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		ProducerRecord<String, byte[]> record2 = new ProducerRecord<String, byte[]>("right", "1a", input2);
		RecordMetadata metadata2 = producer.send(record2).get();
		System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record2.key(),
				record2.value(), metadata2.partition(), metadata2.offset(), elapsedTime);

			
		producer.close();

		
	}
	
	public byte[] serialize(Object obj) throws IOException {
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    ObjectOutputStream os = new ObjectOutputStream(out);
	    os.writeObject(obj);
	    return out.toByteArray();
	}
	public Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
	    ByteArrayInputStream in = new ByteArrayInputStream(data);
	    ObjectInputStream is = new ObjectInputStream(in);
	    return is.readObject();
	}
	
	public void consume() {
		//
		// Step 3: Verify the application's output data.
		//
		Properties consumerConfig = new Properties();
		consumerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.LOCAL_HOST_PORT);
		consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "deserialization");
		consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		//consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		//consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		//consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		//consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(consumerConfig);

		consumer.subscribe(Arrays.asList("joined"));

		int i = 0;

		try {
			while (true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(100);
				for (ConsumerRecord<String, byte[]> r : records) {

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
	
	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
		new KStreamJoin();
	}

}
