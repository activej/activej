package io.activej.redis;

import ai.grakn.redismock.RedisServer;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promises;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.stream.Stream;

import static io.activej.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertComplete;
import static org.junit.Assert.*;

public final class RedisClientTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private RedisServer server;
	private RedisClient client;

	@Before
	public void setUp() throws IOException {
		server = RedisServer.newRedisServer();
		server.start();
		client = createClient();
	}

	@After
	public void tearDown() {
		server.stop();
	}

	@Test
	public void noPool() {
		await(client.getConnection()
				.then(connection1 -> connection1.returnToPool()
						.then(() -> client.getConnection())
						.whenResult(connection2 -> assertNotSame(connection1, connection2))
				)
				.whenComplete(assertComplete($ -> client.stop())));
	}

	@Test
	public void pool() {
		// enabling pool
		client.withPoolTTL(Duration.ofSeconds(10));

		await(client.getConnection()
				.then(connection1 -> connection1.returnToPool()
						.then(() -> client.getConnection())
						.whenResult(connection2 -> assertSame(connection1, connection2))
				)
				.whenComplete(assertComplete($ -> client.stop())));
	}

	@Test
	public void poolEviction() {
		Eventloop originalEventloop = Eventloop.getCurrentEventloop();
		// use custom time provider
		Eventloop.create(CurrentTimeProvider.ofTimeSequence(0, 1))
				.withCurrentThread()
				.withFatalErrorHandler(rethrowOnAnyError());
		client = createClient();

		// enabling pool
		int ttlMillis = 1;
		client.withPoolTTL(Duration.ofMillis(ttlMillis));

		await(client.getConnection()
				.then(connection1 -> connection1.returnToPool()
						.then(() -> client.getConnection())
						.then(connection2 -> {
							assertSame(connection1, connection2);
							return connection2.returnToPool();
						})
						.then(() -> Promises.delay(Duration.ofMillis(ttlMillis + 1)))
						.then(() -> client.getConnection())
						.whenResult(connection3 -> assertNotSame(connection1, connection3))
				)
				.whenComplete(assertComplete($ -> client.stop())));

		// resetting original eventloop
		originalEventloop.withCurrentThread();
	}

	@Test
	public void start() {
		await(client.start().whenComplete(assertComplete($ -> client.stop())));
	}

	@Test
	public void stop() {
		await(client.stop());
	}

	@Test
	public void stopWithActiveConnections() {
		int connectionCount = 10;

		await(Promises.toList(Stream.generate(() -> client.getConnection()).limit(connectionCount))
				.then(connections -> {
					connections.forEach(connection -> assertFalse(connection.isClosed()));
					return client.stop()
							.whenResult(() -> connections.forEach(connection -> assertTrue(connection.isClosed())));
				})
				.whenComplete(assertComplete()));
	}

	private RedisClient createClient() {
		return RedisClient.create(Eventloop.getCurrentEventloop(), new InetSocketAddress(server.getHost(), server.getBindPort()));
	}
}
