package io.activej.redis;

import io.activej.promise.Promise;

import java.util.function.Function;

import static java.util.Objects.deepEquals;
import static org.junit.Assert.assertTrue;

public final class TestUtils {

	public static <T> T await(RedisClient client, Function<RedisConnection, Promise<T>> clientCommand) {
		return io.activej.promise.TestUtils.await(client.connect()
				.then(connection -> clientCommand.apply(connection)
						.thenEx((result, e) -> connection.quit()
								.then(() -> e == null ?
										Promise.of(result) :
										Promise.ofException(e)))));
	}

	public static void assertDeepEquals(Object[] expected, Object[] actual) {
		assertTrue(deepEquals(expected, actual));
	}
}
