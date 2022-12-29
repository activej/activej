import io.activej.common.ApplicationSettings;
import io.activej.eventloop.Eventloop;
import io.activej.redis.RedisClient;
import io.activej.redis.RedisRequest;
import io.activej.redis.RedisResponse;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.stream.Collectors;

public final class RedisPipelineExample {
	public static final InetSocketAddress DEFAULT_ADDRESS = new InetSocketAddress("localhost", 6379);
	public static final InetSocketAddress ADDRESS = ApplicationSettings.getInetSocketAddress(RedisSimpleExample.class, "address", DEFAULT_ADDRESS);

	private static final String LIST_KEY = "ActiveJ List";
	private static final int N = 20;

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		RedisClient client = RedisClient.create(eventloop, ADDRESS);

		client.connect()
				.then(connection -> {
					for (int i = 0; i < N; i++) {
						String element = String.valueOf(i);
						connection.cmd(RedisRequest.of("RPUSH", LIST_KEY, element), RedisResponse.LONG)
								.whenResult(size -> System.out.println("Added element '" + element + "' to the list, list size:" + size));
					}

					connection.cmd(RedisRequest.of("LRANGE", LIST_KEY, "0", "-1"), RedisResponse.ARRAY)
							.map(array -> Arrays.stream(array)
									.map(element -> new String((byte[]) element))
									.collect(Collectors.toList()))
							.whenResult(list -> System.out.println("List: " + list));

					for (int i = 0; i < N; i++) {
						connection.cmd(RedisRequest.of("RPOP", LIST_KEY), RedisResponse.BYTES_UTF8)
								.whenResult(popped -> System.out.println("Popped element '" + popped + "' from list"));
					}
					return connection.cmd(RedisRequest.of("DEL", LIST_KEY), RedisResponse.SKIP)
							.then(connection::quit);
				})
				.whenException(Exception::printStackTrace);


		eventloop.run();
	}
}
