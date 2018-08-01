package kafka.demo;

public class Constants {
	public static final String INPUT_TOPIC = "topic-input";
	public static final String OUTPUT_TOPIC = "topic-output";
	public static final String THROUGH_TOPIC = "rekeyed-topic";
	public static final String CONFIG_ID_INPUT_STREAM = "deduplication-lambda";
	public static final String LOCAL_HOST_PORT = "localhost:9092";
	public static final String EVENT_ID_STORE = "eventId-store";
	public static final String CONFIG_ID_OUTPUT = "deduplication-consumer";
	public static final int MESSAGE_COUNT = 2;
	public static final boolean FOR_TXN_COMMIT = true;
}
