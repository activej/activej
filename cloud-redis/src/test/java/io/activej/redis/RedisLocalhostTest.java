package io.activej.redis;

import io.activej.common.ApplicationSettings;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.net.InetSocketAddress;
import java.util.function.Function;

import static io.activej.redis.TestUtils.assertOk;

public abstract class RedisLocalhostTest {
	private static final InetSocketAddress ADDRESS = ApplicationSettings.getInetSocketAddress(RedisLocalhostTest.class, "address", null);

	protected RedisClient client;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@BeforeClass
	public static void beforeClass() {
		Assume.assumeNotNull(ADDRESS);
	}

	@Before
	public void setUp() {
		client = RedisClient.create(Eventloop.getCurrentEventloop(), ADDRESS);
		assertOk(await(connection -> connection.flushAll(false)));
	}

	protected  <T> T await(Function<RedisConnection, Promise<T>> clientCommand) {
		return TestUtils.await(client, clientCommand);
	}

}
