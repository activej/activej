package io.activej.redis;

import ai.grakn.redismock.RedisServer;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

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

	protected <T> T await(Function<RedisConnection, Promise<T>> clientCommand) {
		return io.activej.redis.TestUtils.await(client, clientCommand);
	}
}
