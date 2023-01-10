package io.activej.redis;

import ai.grakn.redismock.RedisServer;
import io.activej.async.function.AsyncFunction;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.reactor.Reactor.getCurrentReactor;
import static org.junit.Assert.*;

public class RedisConnectionTestWithStub {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

	private RedisServer server;
	protected RedisClient client;

	@Before
	public void setUp() throws IOException {
		server = RedisServer.newRedisServer();
		server.start();
		client = RedisClient.create(getCurrentReactor(), new InetSocketAddress(server.getHost(), server.getBindPort()));
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
		Assume.assumeTrue(canBeRun());

		String key = "key";
		String value = "value";
		await(redis -> redis.cmd(RedisRequest.of("SET", key, value), RedisResponse.OK));

		assertEquals(value, await(redis -> redis.cmd(RedisRequest.of("GET", key), RedisResponse.BYTES_UTF8)));
	}

	@Test
	public void setBinaryValue() {
		Assume.assumeTrue(canBeRun());

		String key = "key";
		byte[] value = new byte[1024];
		RANDOM.nextBytes(value);
		await(redis -> redis.cmd(RedisRequest.of("SET", key, value), RedisResponse.OK));

		assertArrayEquals(value, await(redis -> redis.cmd(RedisRequest.of("GET", key), RedisResponse.BYTES)));
	}

	protected <T> T await(AsyncFunction<RedisConnection, T> clientCommand) {
		return io.activej.redis.TestUtils.await(client, clientCommand);
	}

	/*
	 Redis-mock uses OS-specific line separator for response encoding
	 */
	private boolean canBeRun() {
		return "\n".equals(System.lineSeparator())
				||
				getClass() != RedisConnectionTestWithStub.class;
	}
}
