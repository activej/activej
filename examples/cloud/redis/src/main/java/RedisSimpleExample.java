import io.activej.common.ApplicationSettings;
import io.activej.eventloop.Eventloop;
import io.activej.redis.RedisClient;
import io.activej.redis.RedisRequest;
import io.activej.redis.RedisResponse;

import java.net.InetSocketAddress;

public final class RedisSimpleExample {
	public static final InetSocketAddress DEFAULT_ADDRESS = new InetSocketAddress("localhost", 6379);
	public static final InetSocketAddress ADDRESS = ApplicationSettings.getInetSocketAddress(RedisSimpleExample.class, "address", DEFAULT_ADDRESS);

	private static final String KEY = "ActiveJ";
	private static final String VALUE = "This is redis client example";

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		RedisClient client = RedisClient.create(eventloop, ADDRESS);

		client.connect()
				.then(connection -> connection.cmd(RedisRequest.of("SET", KEY, VALUE), RedisResponse.OK)
						.then(() -> connection.cmd(RedisRequest.of("GET", KEY), RedisResponse.BYTES_UTF8))
						.whenResult(result -> System.out.println("Key: '" + KEY + "', value: '" + result + '\''))
						.then(result -> connection.cmd(RedisRequest.of("DEL", KEY), RedisResponse.SKIP))
						.then(connection::quit)
				)
				.whenException(Exception::printStackTrace);

		eventloop.run();
	}
}
