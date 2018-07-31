package kafka.dedupe;

public class RunnerDedupe {
	
	/** This sample project aims to eliminate completely the duplication of input from producer
	 *  
	 *  Requirements:
	 *  1. Three topics: topic-input, topic-output, rekeyed-topic as through topic
	 *  2. This automatically creates change log: deduplication-lambda-eventId-store-changelog
	 *  
	 *  Contents:
	 *  1. Stream
	 *  2. Deduplication topology using Windowed
	 *  3. Producer (currently disabled)
	 *  4. Consumer (enabled for logging purposes)
	 *  
	 *  How to Test
	 *  1. Run this runner (RunnerDedupe.java) using IDE (eclipse)
	 *  2. Produce key and value message in cli using this command: 
	 *  	kafka-console-producer --broker-list localhost:9092 --topic  topic-input --property "parse.key=true" --property "key.separator=:”
	 *  3. Check the output from the IDE
	 *  4. Expected Output:
	 *  	1.A Positive test 
	 *         1.A.a When you input "0:test message" (remove the double quotes) on the cli, 
	 *         1.A.b the ide prints out a not duplicate result
	 *         1.A.c the Stream successfully passed message to output topic
	 *         1.A.d verify using this command:
	 *          Median Topic: kafka-console-consumer --topic rekeyed-topic --from-beginning --bootstrap-server localhost:9092 --property print.key=true
	 *         	Output Topic: kafka-console-consumer --topic topic-output --from-beginning --bootstrap-server localhost:9092 --property print.key=true
	 *		1.A Negative test
	 *         1.A.a When you input "0:test message" (remove the double quotes) on the cli for the second time 
	 *         1.A.b the ide prints out a duplicate result
	 *         1.A.c the Stream should not passed message to output topic.
	 *         1.A.d verify using this command:
	 *          Median Topic: kafka-console-consumer --topic rekeyed-topic --from-beginning --bootstrap-server localhost:9092 --property print.key=true
	 *         	Output Topic: kafka-console-consumer --topic topic-output --from-beginning --bootstrap-server localhost:9092 --property print.key=true  	
	 *  
	 *  Note: Regardless, if message is duplicate or not input topic sends message to the changelog. 
	 *        Reset Local.java has no use for now. It will be useful for restarting topics.
	 */
	public static void main(String[] args) throws Exception {
		new Dedupe();
	}
}
