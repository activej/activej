import io.activej.common.ApplicationSettings;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promises;
import io.activej.redis.RedisClient;
import io.activej.redis.RedisRequest;
import io.activej.redis.RedisResponse;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;

public final class RedisTransactionExample {
	public static final InetSocketAddress DEFAULT_ADDRESS = new InetSocketAddress("localhost", 6379);
	public static final InetSocketAddress ADDRESS = ApplicationSettings.getInetSocketAddress(RedisSimpleExample.class, "address", DEFAULT_ADDRESS);

	private static final String LIST_KEY = "ActiveJ List";
	private static final int N = 20;

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.builder()
			.withCurrentThread()
			.build();

		RedisClient client = RedisClient.create(eventloop, ADDRESS);

		client.connect()
			.then(connection -> {
				System.out.println("Transaction begins");
				return connection.multi()
					.then(() -> {
						System.out.println("Adding elements to list inside transaction");
						for (int i = 0; i < N; i++) {
							String element = String.valueOf(i);
							connection.cmd(RedisRequest.of("RPUSH", LIST_KEY, element), RedisResponse.LONG)
								.whenResult(size -> System.out.println(
									"Added element '" +
									element + "' to the list, total size of list is " + size));
						}

						System.out.println("Wait some time before committing transaction...");
						return Promises.delay(Duration.ofSeconds(3));
					})
					.then(() -> {
						System.out.println("Committing transaction");
						return connection.exec();
					})
					.then(transactionResult -> {
						System.out.println("Transaction complete, result: " + Arrays.toString(transactionResult));
						System.out.println("Checking list");
						return connection.cmd(RedisRequest.of("LRANGE", LIST_KEY, "0", "-1"), RedisResponse.ARRAY)
							.map(array -> Arrays.stream(array)
								.map(element -> new String((byte[]) element))
								.collect(Collectors.toList()))
							.whenResult(list -> System.out.println("List: " + list));
					})
					.then(() -> connection.cmd(RedisRequest.of("DEL", LIST_KEY), RedisResponse.SKIP))
					.then(connection::quit);
			})
			.whenException(Exception::printStackTrace);

		eventloop.run();
	}
}
