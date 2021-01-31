package io.activej.redis;

import io.activej.promise.Promise;

import java.util.List;
import java.util.function.Function;

import static java.util.Objects.deepEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class TestUtils {
	public static void assertOk(String result) {
		assertEquals("OK", result);
	}

	public static <T> T await(RedisClient client, Function<RedisConnection, Promise<T>> clientCommand) {
		return io.activej.promise.TestUtils.await(client.connect()
				.then(connection -> clientCommand.apply(connection)
						.whenComplete(connection::quit)));
	}

	public static void assertDeepEquals(List<Object> expected, List<Object> actual) {
		assertTrue(deepEquals(expected, actual));
	}
}
