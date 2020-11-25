package io.activej.redis;

import ai.grakn.redismock.RedisServer;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.TestUtils;
import io.activej.redis.api.RedisAPI;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static io.activej.common.collection.CollectionUtils.map;
import static io.activej.common.collection.CollectionUtils.set;
import static io.activej.redis.TestUtils.assertOk;
import static io.activej.test.TestUtils.assertComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public final class RedisConnectionTest {
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
	private RedisClient client;

	@Before
	public void setUp() throws IOException {
		server = RedisServer.newRedisServer();
		server.start();
		client = RedisClient.create(Eventloop.getCurrentEventloop(), new InetSocketAddress(server.getHost(), server.getBindPort()));
	}

	@After
	public void tearDown() {
		server.stop();
	}

	@Test
	public void getNil() {
		String dummy = await(redis -> redis.get("dummy"));
		assertNull(dummy);
	}

	@Test
	public void getNilBinary() {
		byte[] dummy = await(redis -> redis.getAsBinary("dummy"));
		assertNull(dummy);
	}

	@Test
	public void setValue() {
		String key = "key";
		String value = "value";
		assertOk(await(redis -> redis.set(key, value)));

		assertEquals(value, await(redis -> redis.get(key)));
	}

	@Test
	public void setBinaryValue() {
		String key = "key";
		byte[] value = new byte[1024];
		RANDOM.nextBytes(value);
		assertOk(await(redis -> redis.set(key, value)));

		assertArrayEquals(value, await(redis -> redis.getAsBinary(key)));
	}

	@Test
	public void setMultipleValues() {
		String key = "key";
		String value = "value";

		await(redis -> {
			Promise<String> set1 = redis.set(key + 1, value + 1);
			Promise<String> set2 = redis.set(key + 2, value + 2);
			Promise<String> set3 = redis.set(key + 3, value + 3);

			return Promises.toList(set1, set2, set3)
					.whenComplete(assertComplete(list -> list.forEach(io.activej.redis.TestUtils::assertOk)))
					.then(() -> Promises.toList(redis.get(key + 1), redis.get(key + 2), redis.get(key + 3)))
					.whenComplete(assertComplete(list -> {
						assertEquals(value + 1, list.get(0));
						assertEquals(value + 2, list.get(1));
						assertEquals(value + 3, list.get(2));
					}));
		});
	}

	@Test
	public void setMultipleBinaryValues() {
		String key = "key";
		byte[] value1 = "value1".getBytes(UTF_8);
		byte[] value2 = "value2".getBytes(UTF_8);
		byte[] value3 = "value3".getBytes(UTF_8);

		await(redis -> {
			Promise<String> set1 = redis.set(key + 1, value1);
			Promise<String> set2 = redis.set(key + 2, value2);
			Promise<String> set3 = redis.set(key + 3, value3);

			return Promises.toList(set1, set2, set3)
					.whenComplete(assertComplete(list -> list.forEach(io.activej.redis.TestUtils::assertOk)))
					.then(() -> Promises.toList(redis.getAsBinary(key + 1), redis.getAsBinary(key + 2), redis.getAsBinary(key + 3)))
					.whenComplete(assertComplete(list -> {
						assertArrayEquals(value1, list.get(0));
						assertArrayEquals(value2, list.get(1));
						assertArrayEquals(value3, list.get(2));
					}));
		});
	}

	@Test
	public void setEmptyKey() {
		String key = "";
		String value = "value";
		assertOk(await(redis -> redis.set(key, value)));

		assertEquals(value, await(redis -> redis.get(key)));
	}

	@Test
	public void setEmptyValue() {
		String key = "key";
		String value = "";
		assertOk(await(redis -> redis.set(key, value)));

		assertEquals(value, await(redis -> redis.get(key)));
	}

	@Test
	public void setEmptyBinaryValue() {
		String key = "key";
		byte[] value = {};
		assertOk(await(redis -> redis.set(key, value)));

		assertArrayEquals(value, await(redis -> redis.getAsBinary(key)));
	}

	@Test
	public void ping() {
		assertEquals("PONG", await(RedisConnection::ping));
	}

	@Test
	public void keys() {
		assertTrue(await(RedisAPI::keys).isEmpty());

		assertOk(await(redis -> redis.set("aaa", "one")));
		assertOk(await(redis -> redis.set("aba", "two")));
		assertOk(await(redis -> redis.set("abb", "three")));

		assertEquals(set("aaa", "aba", "abb"), new HashSet<>(await(RedisAPI::keys)));
		assertEquals(singletonList("aaa"), await(redis -> redis.keys("aaa")));
		assertEquals(set("aba", "abb"), new HashSet<>(await(redis -> redis.keys("ab?"))));
	}

	@Test
	public void deleteKeys() {
		assertOk(await(redis -> redis.set("a", "valueA")));
		assertOk(await(redis -> redis.set("b", "valueB")));
		assertOk(await(redis -> redis.set("c", "valueC")));
		assertOk(await(redis -> redis.set("d", "valueD")));

		assertEquals(set("a", "b", "c", "d"), new HashSet<>(await(RedisAPI::keys)));
		await(redis -> redis.del("a", "c"));
		assertEquals(set("b", "d"), new HashSet<>(await(RedisAPI::keys)));
	}

	@Test
	public void exists() {
		assertOk(await(redis -> redis.set("a", "valueA")));
		assertOk(await(redis -> redis.set("b", "valueB")));
		assertOk(await(redis -> redis.set("c", "valueC")));

		assertEquals(1, (long) await(redis -> redis.exists("a")));
		assertEquals(0, (long) await(redis -> redis.exists("nonexistent")));
	}

	@Test
	public void expire() {
		testExpiration(TTL_SECONDS, redis -> redis.expire(EXPIRATION_KEY, TTL_SECONDS.getSeconds()));
	}

	@Test
	public void expireat() {
		testExpiration(TTL_SECONDS, redis -> redis.expireat(EXPIRATION_KEY, (System.currentTimeMillis() / 1000) + TTL_SECONDS.getSeconds()));
	}

	@Test
	public void pexpire() {
		testExpiration(TTL_MILLIS, redis -> redis.pexpire(EXPIRATION_KEY, TTL_MILLIS.toMillis()));
	}

	@Test
	public void pexpireat() {
		testExpiration(TTL_MILLIS, redis -> redis.pexpireat(EXPIRATION_KEY, System.currentTimeMillis() + TTL_MILLIS.toMillis()));
	}

	@Test
	public void expirationDefaults() {
		testExpiration(TTL_MILLIS, redis -> redis.expire(EXPIRATION_KEY, TTL_MILLIS));
		testExpiration(TTL_MILLIS, redis -> redis.expireat(EXPIRATION_KEY, Instant.now().plus(TTL_MILLIS)));
	}

	@Test
	public void ttl() {
		String key = "a";
		assertOk(await(redis -> redis.set(key, "value")));

		assertEquals(1L, (long) await(redis -> redis.expire(key, TTL_SECONDS)));
		assertEquals(TTL_SECONDS.getSeconds(), (long) await(redis -> redis.ttl(key)));
	}

	@Test
	public void pttl() {
		String key = "a";
		assertOk(await(redis -> redis.set(key, "value")));

		assertEquals(1L, (long) await(redis -> redis.expire(key, TTL_MILLIS)));
		Long ttlMillis = await(redis -> redis.pttl(key));

		assertTrue(0 < ttlMillis && ttlMillis <= TTL_MILLIS.toMillis());
	}

	@Test
	public void append() {
		String key = "a";
		String value = "value A";
		String appended = "appended";
		String nonexistent = "nonexistent";

		assertOk(await(redis -> redis.set(key, value)));

		assertEquals(singletonList(key), await(RedisAPI::keys));
		assertEquals(value.length() + appended.length(), (long) await(redis -> redis.append(key, appended)));
		assertEquals(appended.length(), (long) await(redis -> redis.append(nonexistent, appended)));

		assertEquals(appended, await(redis -> redis.get(nonexistent)));
		assertEquals(value + appended, await(redis -> redis.get(key)));
	}

	@Test
	public void decr() {
		String key = "key";
		long initialValue = 100;
		int decrBy = 10;

		assertOk(await(redis -> redis.set(key, String.valueOf(initialValue))));

		for (int i = 0; i < decrBy; i++) {
			await(redis -> redis.decr(key));
		}

		assertEquals(initialValue - decrBy, Long.parseLong(await(redis -> redis.get(key))));
	}

	@Test
	public void decrby() {
		String key = "key";
		long initialValue = 100;
		long decrBy = 100_000;

		assertOk(await(redis -> redis.set(key, String.valueOf(initialValue))));

		assertEquals(initialValue - decrBy, (long) await(redis -> redis.decrby(key, decrBy)));
		assertEquals(initialValue - decrBy, Long.parseLong(await(redis -> redis.get(key))));

		assertEquals(initialValue, (long) await(redis -> redis.decrby(key, -decrBy)));
		assertEquals(initialValue, Long.parseLong(await(redis -> redis.get(key))));

		assertEquals(initialValue, (long) await(redis -> redis.decrby(key, 0)));
		assertEquals(initialValue, Long.parseLong(await(redis -> redis.get(key))));
	}

	@Test
	public void getbit() {
		String key = "key";
		byte[] bytes = new byte[1024];
		RANDOM.nextBytes(bytes);

		assertOk(await(redis -> redis.set(key, bytes)));

		int offset = RANDOM.nextInt(bytes.length);

		long expectedBit = BitSet.valueOf(bytes).get(offset) ? 1 : 0;
		long actualBit = await(redis -> redis.getbit(key, offset));
		assertEquals(expectedBit, actualBit);
	}

	@Test
	public void getset() {
		String key = "key";
		String value = "value 0";
		String newValue = "value 1";
		byte[] newBinaryValue = "value 2".getBytes(UTF_8);

		assertNull(await(redis -> redis.getset(key, value)));
		assertEquals(value, await(redis -> redis.getset(key, newValue)));
		assertArrayEquals(newValue.getBytes(UTF_8), await(redis -> redis.getset(key, newBinaryValue)));
	}

	@Test
	public void incr() {
		String key = "key";
		long initialValue = 100;
		int incrBy = 10;

		assertOk(await(redis -> redis.set(key, String.valueOf(initialValue))));

		assertEquals(initialValue, Long.parseLong(await(redis -> redis.get(key))));

		for (int i = 0; i < incrBy; i++) {
			await(redis -> redis.incr(key));
		}

		assertEquals(initialValue + incrBy, Long.parseLong(await(redis -> redis.get(key))));
	}

	@Test
	public void incrby() {
		String key = "key";
		long initialValue = -500_100;
		long incrBy = 100_000;

		assertOk(await(redis -> redis.set(key, String.valueOf(initialValue))));

		assertEquals(initialValue + incrBy, (long) await(redis -> redis.incrby(key, incrBy)));
		assertEquals(initialValue + incrBy, Long.parseLong(await(redis -> redis.get(key))));

		assertEquals(initialValue, (long) await(redis -> redis.incrby(key, -incrBy)));
		assertEquals(initialValue, Long.parseLong(await(redis -> redis.get(key))));

		assertEquals(initialValue, (long) await(redis -> redis.incrby(key, 0)));
		assertEquals(initialValue, Long.parseLong(await(redis -> redis.get(key))));
	}

	@Test
	public void mget() {
		String key1 = "key1";
		String key2 = "key2";
		String key3 = "key3";
		String value1 = "value1";
		String value2 = "value2";
		String value3 = "value3";

		assertOk(await(redis -> redis.set(key1, value1)));
		assertOk(await(redis -> redis.set(key2, value2)));
		assertOk(await(redis -> redis.set(key3, value3)));

		List<@Nullable String> expected = asList(null, value2, value3, null, value1, null);
		List<@Nullable String> actual = await(redis -> redis.mget("nonexistent", key2, key3, "nonexistent", key1, "test"));
		assertEquals(expected, actual);
	}

	@Test
	public void mgetAsBinary() {
		String key1 = "key1";
		String key2 = "key2";
		String key3 = "key3";
		byte[] value1 = "value1".getBytes(UTF_8);
		byte[] value2 = "value2".getBytes(UTF_8);
		byte[] value3 = "value3".getBytes(UTF_8);

		assertOk(await(redis -> redis.set(key1, value1)));
		assertOk(await(redis -> redis.set(key2, value2)));
		assertOk(await(redis -> redis.set(key3, value3)));

		List<@Nullable byte[]> expected = asList(null, value2, value3, null, value1, null);
		List<@Nullable byte[]> actual = await(redis -> redis.mgetAsBinary("nonexistent", key2, key3, "nonexistent", key1, "test"));
		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			assertArrayEquals(expected.get(i), actual.get(i));
		}
	}

	@Test
	public void mset() {
		String key1 = "key1";
		String key2 = "key2";
		String key3 = "key3";
		String value1 = "value1";
		String value2 = "value2";
		String value3 = "value3";

		await(redis -> redis.mset(
				key1, value1,
				key2, value2,
				key3, value3
		));

		assertEquals(value1, await(redis -> redis.get(key1)));
		assertEquals(value2, await(redis -> redis.get(key2)));
		assertEquals(value3, await(redis -> redis.get(key3)));
	}

	@Test
	public void msetBinary() {
		String key1 = "key1";
		String key2 = "key2";
		String key3 = "key3";
		byte[] value1 = "value1".getBytes(UTF_8);
		byte[] value2 = "value2".getBytes(UTF_8);
		byte[] value3 = "value3".getBytes(UTF_8);

		await(redis -> redis.mset(map(
				key1, value1,
				key2, value2,
				key3, value3
		)));

		assertArrayEquals(value1, await(redis -> redis.getAsBinary(key1)));
		assertArrayEquals(value2, await(redis -> redis.getAsBinary(key2)));
		assertArrayEquals(value3, await(redis -> redis.getAsBinary(key3)));
	}

	@Test
	public void psetex() {
		testExpiration(TTL_MILLIS, redis -> redis.psetex(EXPIRATION_KEY, TTL_MILLIS.toMillis(), EXPIRATION_VALUE));
	}

	@Test
	public void setex() {
		testExpiration(TTL_SECONDS, redis -> redis.setex(EXPIRATION_KEY, TTL_SECONDS.getSeconds(), EXPIRATION_VALUE));
	}

	@Test
	public void setexDefaults() {
		testExpiration(TTL_MILLIS, redis -> redis.setex(EXPIRATION_KEY, TTL_MILLIS, EXPIRATION_VALUE));
	}

	@Test
	public void setnx() {
		assertEquals(1, (long) await(redis -> redis.setnx("key", "first")));
		assertEquals(0, (long) await(redis -> redis.setnx("key", "second")));

		assertEquals("first", await(redis -> redis.get("key")));
	}

	@Test
	public void strlen() {
		String key = "key";
		String value = "value";

		assertEquals(0, (long) await(redis -> redis.strlen(key)));
		assertOk(await(redis -> redis.set(key, value)));
		assertEquals(value.length(), (long) await(redis -> redis.strlen(key)));
	}

	@Test
	public void lindex() {
		String key = "list";

		assertEquals(3, (long) await(redis -> redis.rpush(key, "a", "b", "c")));

		assertEquals("a", await(redis -> redis.lindex(key, 0)));
		assertArrayEquals(new byte[]{'c'}, await(redis -> redis.lindexAsBinary(key, -1)));
		assertNull(await(redis -> redis.lindex(key, Integer.MAX_VALUE)));
	}

	@Test
	public void llen() {
		String key = "list";

		assertEquals(3, (long) await(redis -> redis.lpush(key, "a", "b", "c")));
		assertEquals(3, (long) await(redis -> redis.llen(key)));
	}

	@Test
	public void lpop() {
		String key = "list";

		assertEquals(2, (long) await(redis -> redis.rpush(key, "a", "b")));
		assertEquals("a", await(redis -> redis.lpop(key)));
		assertArrayEquals(new byte[]{'b'}, await(redis -> redis.lpopAsBinary(key)));
		assertNull(await(redis -> redis.lpop(key)));
	}

	@Test
	public void lpush() {
		String key = "list";

		assertEquals(3, (long) await(redis -> redis.lpush(key, "a", "b", "c")));
		assertEquals(6, (long) await(redis -> redis.lpush(key, "d", "e", "f")));

		assertEquals(asList("f", "e", "d", "c", "b", "a"), await(redis -> redis.lrange(key)));

		assertEquals(1, (long) await(redis -> redis.del(key)));

		assertEquals(2, (long) await(redis -> redis.lpush(key, new byte[]{'a', 'b', 'c'}, new byte[]{'d', 'e', 'f'})));

		List<byte[]> binaryList = await(redis -> redis.lrangeAsBinary(key));
		assertEquals(2, binaryList.size());
		assertArrayEquals(new byte[]{'d', 'e', 'f'}, binaryList.get(0));
		assertArrayEquals(new byte[]{'a', 'b', 'c'}, binaryList.get(1));
	}

	@Test
	public void lpushx() {
		String key = "list";

		assertEquals(3, (long) await(redis -> redis.lpush(key, "a", "b", "c")));
		assertEquals(6, (long) await(redis -> redis.lpushx(key, "d", "e", "f")));
		assertEquals(8, (long) await(redis -> redis.lpushx(key, new byte[]{'1', '2', '3'}, new byte[]{'4', '5', '6'})));

		assertEquals(asList("456", "123", "f", "e", "d", "c", "b", "a"), await(redis -> redis.lrange(key)));

		assertEquals(1, (long) await(redis -> redis.del(key)));

		assertEquals(0, (long) await(redis -> redis.lpushx(key, "a")));
	}

	@Test
	public void lrange() {
		String key = "list";

		assertTrue(await(redis -> redis.lrange(key)).isEmpty());

		assertEquals(4, (long) await(redis -> redis.rpush(key, "a", "b", "c", "d")));
		assertEquals(asList("a", "b", "c", "d"), await(redis -> redis.lrange(key)));
		assertEquals(asList("b", "c", "d"), await(redis -> redis.lrange(key, 1)));
		assertEquals(asList("b", "c"), await(redis -> redis.lrange(key, 1, 2)));

		List<byte[]> binaryList = await(redis -> redis.lrangeAsBinary(key));
		assertEquals(4, binaryList.size());
		for (int i = 0; i < binaryList.size(); i++) {
			assertArrayEquals(new byte[]{(byte) ('a' + i)}, binaryList.get(i));
		}
	}

	@Test
	public void lrem() {
		String key = "list";

		assertEquals(8, (long) await(redis -> redis.rpush(key, "a", "b", "c", "a", "c", "g", "c", "b")));
		assertEquals(asList("a", "b", "c", "a", "c", "g", "c", "b"), await(redis -> redis.lrange(key)));

		assertEquals(1, (long) await(redis -> redis.lrem(key, 1, "a")));
		assertEquals(asList("b", "c", "a", "c", "g", "c", "b"), await(redis -> redis.lrange(key)));

		assertEquals(1, (long) await(redis -> redis.lrem(key, -1, "c")));
		assertEquals(asList("b", "c", "a", "c", "g", "b"), await(redis -> redis.lrange(key)));

		assertEquals(2, (long) await(redis -> redis.lrem(key, 0, "c")));
		assertEquals(asList("b", "a", "g", "b"), await(redis -> redis.lrange(key)));

		assertEquals(2, (long) await(redis -> redis.lrem(key, 10, "b")));
		assertEquals(asList("a", "g"), await(redis -> redis.lrange(key)));
	}

	@Test
	public void rpop() {
		String key = "list";

		assertEquals(2, (long) await(redis -> redis.rpush(key, "a", "b")));
		assertEquals("b", await(redis -> redis.rpop(key)));
		assertArrayEquals(new byte[]{'a'}, await(redis -> redis.rpopAsBinary(key)));
		assertNull(await(redis -> redis.rpop(key)));
	}

	@Test
	public void rpush() {
		String key = "list";

		assertEquals(3, (long) await(redis -> redis.rpush(key, "a", "b", "c")));
		assertEquals(6, (long) await(redis -> redis.rpush(key, "d", "e", "f")));

		assertEquals(asList("a", "b", "c", "d", "e", "f"), await(redis -> redis.lrange(key)));

		assertEquals(1, (long) await(redis -> redis.del(key)));

		assertEquals(2, (long) await(redis -> redis.rpush(key, new byte[]{'a', 'b', 'c'}, new byte[]{'d', 'e', 'f'})));

		List<byte[]> binaryList = await(redis -> redis.lrangeAsBinary(key));
		assertEquals(2, binaryList.size());
		assertArrayEquals(new byte[]{'a', 'b', 'c'}, binaryList.get(0));
		assertArrayEquals(new byte[]{'d', 'e', 'f'}, binaryList.get(1));
	}

	@Test
	public void pfaddAndPfcount() {
		String key1 = "hll1";
		String key2 = "hll2";

		assertEquals(1, (long) await(redis -> redis.pfadd(key1, "a", "b", "c", "d", "e", "f", "g")));
		assertEquals(0, (long) await(redis -> redis.pfadd(key1, "a", "a", "b")));

		assertEquals(1, (long) await(redis -> redis.pfadd(key2, "1", "2", "3")));

		assertEquals(10, (long) await(redis -> redis.pfcount(key1, key2)));
		assertEquals(0, (long) await(redis -> redis.pfcount("nonexistent")));
	}

	@Test
	public void pfmerge() {
		String key1 = "hll1";
		String key2 = "hll2";
		String destKey = "result";

		assertEquals(1, (long) await(redis -> redis.pfadd(key1, "a", "b", "c", "d", "e", "f", "g")));
		assertEquals(1, (long) await(redis -> redis.pfadd(key2, "1", "2", "3", "4", "a", "b", "c", "d")));

		assertEquals(7, (long) await(redis -> redis.pfcount(key1)));
		assertEquals(8, (long) await(redis -> redis.pfcount(key2)));

		await(redis -> redis.pfmerge(destKey, key1, key2));
		assertEquals(11, (long) await(redis -> redis.pfcount(destKey)));
	}

	private void testExpiration(Duration ttl, Function<RedisAPI, Promise<?>> expireCommand) {
		assertEquals(-2L, (long) await(redis -> redis.ttl(EXPIRATION_KEY)));

		assertOk(await(redis -> redis.set(EXPIRATION_KEY, EXPIRATION_VALUE)));
		assertEquals(-1L, (long) await(redis -> redis.ttl(EXPIRATION_KEY)));


		await(expireCommand::apply);
		long keyTtl = await(redis -> redis.pttl(EXPIRATION_KEY));
		assertTrue(keyTtl > 0 && keyTtl <= ttl.toMillis());

		TestUtils.await(Promises.delay(ttl));
		assertEquals(0L, (long) await(redis -> redis.exists(EXPIRATION_KEY)));
		assertEquals(-2L, (long) await(redis -> redis.ttl(EXPIRATION_KEY)));

		assertEquals(0L, (long) await(redis -> redis.expire("nonexistent", Duration.ofSeconds(1))));
	}

	private <T> T await(Function<RedisConnection, Promise<T>> clientCommand) {
		return io.activej.redis.TestUtils.await(client, clientCommand);
	}
}
