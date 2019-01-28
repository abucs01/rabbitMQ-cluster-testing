package rabbitMQ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class ConsumerTwo {

	private final static String QUEUE_NAME = "test_cluster";
	public static List<String> messageIds = new ArrayList<String>();

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("devk.almtar.local");
		factory.setPort(30672);
		factory.setUsername("thematra");
		factory.setPassword("P@ssw0rd");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		// channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
		});
		System.out.println(" RECEIVER 2 JOB  [x] Done ");
		Thread.sleep(10000);
		whenWriteToTmpFile_thenCorrect(messageIds);

	}

	static DeliverCallback deliverCallback = (consumerTag, delivery) -> {
		String message = new String(delivery.getBody(), "UTF-8");
		System.out.println(" RECEIVER 1  [x] Received '" + message + "'");
		messageIds.add(message);
	};

	public static void whenWriteToTmpFile_thenCorrect(List<String> messageIds) throws IOException {
		String concatString = String.join(",", messageIds);
		String toWrite = concatString;
		File tmpFile = File.createTempFile("receive-messages-2", ".tmp");
		FileWriter writer = new FileWriter(tmpFile);
		writer.write(toWrite);
		writer.close();

		BufferedReader reader = new BufferedReader(new FileReader(tmpFile));
		reader.close();
	}

}
