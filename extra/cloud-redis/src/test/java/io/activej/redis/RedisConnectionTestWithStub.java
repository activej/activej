package io.activej.redis;

import ai.grakn.redismock.RedisServer;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public class RedisConnectionTestWithStub {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static final String EXPIRATION_KEY = "expireKey";
	private static final String EXPIRATION_VALUE = "expireValue";
	private static final Duration TTL_MILLIS = Duration.ofMillis(100);
	private static final Duration TTL_SECONDS = Duration.ofSeconds(1);
	private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

	private RedisServer server;
	protected RedisClient client;

	@Before
	public void setUp() throws IOException {
		server = RedisServer.newRedisServer();
		server.start();
		client = RedisClient.create(Eventloop.getCurrentEventloop(), new InetSocketAddress(server.getHost(), server.getBindPort()));
	}

	@After
	public void tearDown() {
		if (server != null) server.stop();
	}


	@Test
	public void getNil() {
		String dummy = await(redis -> redis.cmd(RedisRequest.of("GET", "dummy"), RedisResponse.BYTES_UTF8));
		assertNull(dummy);
	}

	@Test
	public void getNilBinary() {
		byte[] dummy = await(redis -> redis.cmd(RedisRequest.of("GET", "dummy"), RedisResponse.BYTES));
		assertNull(dummy);
	}

	@Test
	public void setValue() {
		String key = "key";
		String value = "value";
		await(redis -> redis.cmd(RedisRequest.of("SET", key, value), RedisResponse.OK));

		assertEquals(value, await(redis -> redis.cmd(RedisRequest.of("GET", key), RedisResponse.BYTES_UTF8)));
	}

	@Test
	public void setBinaryValue() {
		String key = "key";
		byte[] value = new byte[1024];
		RANDOM.nextBytes(value);
		await(redis -> redis.cmd(RedisRequest.of("SET", key, value), RedisResponse.OK));

		assertArrayEquals(value, await(redis -> redis.cmd(RedisRequest.of("GET", key), RedisResponse.BYTES)));
	}

	protected <T> T await(Function<RedisConnection, Promise<T>> clientCommand) {
		return io.activej.redis.TestUtils.await(client, clientCommand);
	}
}
