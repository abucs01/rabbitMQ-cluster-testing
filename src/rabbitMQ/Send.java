package rabbitMQ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Send {

	private final static String QUEUE_NAME = "test_cluster";
	final static String FILE_NAME = "C:\\Temp\\input.txt";
	final static String OUTPUT_FILE_NAME = "C:\\Temp\\output.txt";
	final static Charset ENCODING = StandardCharsets.UTF_8;
	public static List<String> messageIds = new ArrayList<String>();

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("devk.almtar.local");
		factory.setPort(30672);
		factory.setUsername("thematra");
		factory.setPassword("P@ssw0rd");
		// factory.setVirtualHost("/");
		// ConnectionFactory factory = new ConnectionFactory();
		// factory.setUri("amqp://thematra:P@ssw0rd@localhost:5672");

		try (Connection connection = factory.newConnection();

				Channel channel = connection.createChannel()) {
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			for (int i = 0; i < 100000; i++) {
				String message = "AB100" + i;
				messageIds.add(message);
				channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
				System.out.println(" [x] Sent '" + message + "'");
			}

			whenWriteToTmpFile(messageIds);

		}
	}

	public static void whenWriteToTmpFile(List<String> messageIds) throws IOException {
		String concatString = String.join(",", messageIds);
		String toWrite = concatString;
		File tmpFile = File.createTempFile("send-messages", ".tmp");
		FileWriter writer = new FileWriter(tmpFile);
		writer.write(toWrite);
		writer.close();

		BufferedReader reader = new BufferedReader(new FileReader(tmpFile));
		reader.close();
	}

}
