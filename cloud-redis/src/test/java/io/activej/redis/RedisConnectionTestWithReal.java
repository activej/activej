package io.activej.redis;

import io.activej.common.ApplicationSettings;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.Nullable;
import org.junit.*;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.IntStream;

import static io.activej.common.CollectorsEx.toMap;
import static io.activej.common.collection.CollectionUtils.first;
import static io.activej.common.collection.CollectionUtils.keysToMap;
import static io.activej.redis.RedisConnection.TRANSACTION_DISCARDED;
import static io.activej.redis.RedisConnection.TRANSACTION_FAILED;
import static io.activej.redis.TestUtils.assertDeepEquals;
import static io.activej.redis.TestUtils.assertOk;
import static io.activej.redis.Utils.ZERO_CURSOR;
import static io.activej.redis.api.ScanModifier.match;
import static io.activej.redis.api.SortModifier.alpha;
import static io.activej.redis.api.SortModifier.desc;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public final class RedisConnectionTestWithReal extends RedisConnectionTestWithStub {
	private static final InetSocketAddress ADDRESS = ApplicationSettings.getInetSocketAddress(RedisConnectionTestWithReal.class, "address", null);

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
		await(connection -> connection.flushAll(false));
	}

	@Test
	public void simpleTransaction() {
		await(redis -> redis.multi()
				.then(() -> {
					Promise<List<Object>> listPromise = Promises.toList(
							redis.get("x"),
							redis.set("x", "value"),
							redis.get("x"),
							redis.set("x", "new value"),
							redis.get("x"),
							redis.del("x", "y")
					);
					return redis.exec()
							.whenResult(transactionResult -> {
								assertDeepEquals(listPromise.getResult(), transactionResult);
								assertDeepEquals(asList(null, "OK", "value", "OK", "new value", 1L), transactionResult);
							});
				}));
	}

	@Test
	public void pipelinedTransaction() {
		await(redis -> {
			Promise<Void> multiPromise = redis.multi();
			Promise<List<Object>> listPromise = Promises.toList(
					redis.get("x"),
					redis.set("x", "value"),
					redis.get("x"),
					redis.set("x", "new value"),
					redis.get("x"),
					redis.del("x", "y")
			);
			return redis.exec()
					.whenResult(transactionResult -> {
						assertTrue(multiPromise.isResult());
						assertDeepEquals(listPromise.getResult(), transactionResult);
						assertDeepEquals(asList(null, "OK", "value", "OK", "new value", 1L), transactionResult);
					});
		});
	}

	@Test
	public void pipelinedMultipleTransactions() {
		await(redis -> {
			Promise<Void> multi1Promise = redis.multi();
			Promise<List<Object>> list1Promise = Promises.toList(
					redis.get("x"),
					redis.set("x", "value x"),
					redis.get("x"),
					redis.set("x", "new value x"),
					redis.get("x")
			);
			Promise<@Nullable List<Object>> exec1Promise = redis.exec();

			Promise<Void> multi2Promise = redis.multi();
			Promise<List<Object>> list2Promise = Promises.toList(
					redis.get("y"),
					redis.set("y", "value y"),
					redis.get("y"),
					redis.set("y", "new value y"),
					redis.get("y"),
					redis.del("x", "y")
			);

			return redis.exec()
					.whenResult(transaction2Result -> {
						assertTrue(multi1Promise.isResult() && multi2Promise.isResult());
						assertDeepEquals(list1Promise.getResult(), exec1Promise.getResult());
						assertDeepEquals(asList(null, "OK", "value x", "OK", "new value x"), exec1Promise.getResult());

						assertDeepEquals(list2Promise.getResult(), transaction2Result);
						assertDeepEquals(asList(null, "OK", "value y", "OK", "new value y", 2L), transaction2Result);
					});
		});
	}

	@Test
	public void discard() {
		String key = "key";
		String value = "value";
		await(redis -> redis.multi().then(redis::discard));

		String finalResult = await(redis -> redis.set(key, value)
				.whenResult(io.activej.redis.TestUtils::assertOk)
				.then(redis::multi)
				.then(() -> {
					Promise<@Nullable String> setPromise = redis.set(key, value + 1);
					return redis.discard()
							.then(() -> {
								assertSame(TRANSACTION_DISCARDED, setPromise.getException());
								return redis.get(key);
							});
				}));

		assertEquals(value, finalResult);
	}

	@Test
	public void discardPipeline() {
		String key = "key";
		String value = "value";
		await(redis -> redis.multi().then(redis::discard));

		String finalResult = await(redis -> {
			Promise<@Nullable String> set1Promise = redis.set(key, value);
			Promise<Void> multiPromise = redis.multi();
			Promise<@Nullable String> set2Promise = redis.set(key, value + 1);
			return redis.discard()
					.then(() -> {
						assertOk(set1Promise.getResult());
						assertTrue(multiPromise.isResult());
						assertSame(TRANSACTION_DISCARDED, set2Promise.getException());
						return redis.get(key);
					});
		});

		assertEquals(value, finalResult);
	}

	@Test
	public void discardPipelineMultipleTransactions() {
		String key = "key";
		String value = "value";
		await(redis -> redis.multi().then(redis::discard));

		String finalResult = await(redis -> {
			Promise<@Nullable String> set1Promise = redis.set(key, value);
			Promise<Void> multi1Promise = redis.multi();
			Promise<@Nullable String> set2Promise = redis.set(key, value + 1);
			Promise<Void> discard1Promise = redis.discard();
			Promise<Void> multi2Promise = redis.multi();
			Promise<@Nullable String> set3Promise = redis.set(key, value + 2);
			return redis.discard()
					.then(() -> {
						assertOk(set1Promise.getResult());
						assertTrue(multi1Promise.isResult());
						assertSame(TRANSACTION_DISCARDED, set2Promise.getException());
						assertTrue(multi2Promise.isResult());
						assertTrue(discard1Promise.isResult());
						assertSame(TRANSACTION_DISCARDED, set3Promise.getException());
						return redis.get(key);
					});
		});

		assertEquals(value, finalResult);
	}

	@Test
	public void discardPipelineMultipleTransactionsFirstSuccessfull() {
		String key = "key";
		String value = "value";
		await(redis -> redis.multi().then(redis::discard));

		String finalResult = await(redis -> {
			Promise<@Nullable String> set1Promise = redis.set(key, value);
			Promise<Void> multi1Promise = redis.multi();
			Promise<@Nullable String> set2Promise = redis.set(key, value + 1);
			Promise<@Nullable List<Object>> execPromise = redis.exec();
			Promise<Void> multi2Promise = redis.multi();
			Promise<@Nullable String> set3Promise = redis.set(key, value + 2);
			return redis.discard()
					.then(() -> {
						assertOk(set1Promise.getResult());
						assertTrue(multi1Promise.isResult());
						assertOk(set2Promise.getResult());
						assertTrue(multi2Promise.isResult());
						assertDeepEquals(execPromise.getResult(), singletonList(set2Promise.getResult()));
						assertSame(TRANSACTION_DISCARDED, set3Promise.getException());
						return redis.get(key);
					});
		});

		assertEquals(value + 1, finalResult);
	}

	@Test
	public void watch() {
		String key1 = "key1";
		String key2 = "key2";
		String value = "value";
		String otherValue = "other value";

		List<Object> execResult = io.activej.redis.TestUtils.await(client, redis -> redis.watch(key1, key2)
				.then(redis::multi)
				.then(() -> client.getConnection())
				.then(otherConnection -> otherConnection.set(key2, otherValue)
						.whenResult(io.activej.redis.TestUtils::assertOk)
						.then(result -> otherConnection.quit()))
				.then(() -> {
					Promise<@Nullable String> setPromise = redis.set(key1, value);
					return redis.exec()
							.whenResult(() -> assertSame(TRANSACTION_FAILED, setPromise.getException()));
				}));
		assertNull(execResult);

		assertNull(await(redis -> redis.get(key1)));
		assertEquals(otherValue, await(redis -> redis.get(key2)));
	}

	@Test
	public void unwatch() {
		String key1 = "key1";
		String key2 = "key2";
		String value = "value";
		String otherValue = "other value";

		List<Object> execResult = io.activej.redis.TestUtils.await(client, redis -> redis.watch(key1, key2)
				.then(redis::unwatch)
				.then(redis::multi)
				.then(() -> client.getConnection())
				.then(otherConnection -> otherConnection.set(key2, otherValue)
						.whenResult(TestUtils::assertOk)
						.then(result -> otherConnection.quit()))
				.then(() -> {
					Promise<@Nullable String> setPromise = redis.set(key1, value);
					return redis.exec()
							.whenResult(() -> assertOk(setPromise.getResult()));
				}));
		assertDeepEquals(singletonList("OK"), execResult);

		assertEquals(value, await(redis -> redis.get(key1)));
		assertEquals(otherValue, await(redis -> redis.get(key2)));
	}

	@Test
	public void sort() {
		String key = "list";
		List<String> list = IntStream.range(0, 100).mapToObj(String::valueOf).collect(toList());
		Collections.shuffle(list);
		await(redis -> redis.lpush(key, list.get(0), list.subList(1, list.size()).toArray(new String[0])));

		list.sort(comparing(Integer::parseInt));
		assertEquals(list, await(redis -> redis.sort(key)));

		list.sort(Comparator.<String, Integer>comparing(Integer::parseInt).reversed());
		assertEquals(list, await(redis -> redis.sort(key, desc())));

		Collections.sort(list);
		assertEquals(list, await(redis -> redis.sort(key, alpha())));

		String dest = "dest";
		assertEquals(list.size(), (long) await(redis -> redis.sort(key, dest, alpha())));
		assertEquals(list, await(redis -> redis.lrange(dest)));
	}

	@Test
	public void scan() {
		byte[] value = new byte[0];
		Set<String> keys = IntStream.range(0, 100).mapToObj(i -> "key" + i).collect(toSet());
		await(redis -> redis.mset(keysToMap(keys, $ -> value)));

		Set<String> result = new HashSet<>();
		await(redis -> Promises.until(ZERO_CURSOR,
				cursor -> redis.scan(cursor)
						.map(scanResult -> {
							result.addAll(scanResult.getElements());
							return scanResult.getCursor();
						}),
				ZERO_CURSOR::equals));
		assertEquals(keys, result);

		Set<String> endsWith0Result = await(redis -> redis.scanStream(match("*0")).toCollector(toSet()));
		Set<String> endsWith0Expected = keys.stream().filter(key -> key.endsWith("0")).collect(toSet());
		assertEquals(endsWith0Expected, endsWith0Result);
	}

	@Test
	public void sscan() {
		String key = "key";
		Set<String> elements = IntStream.range(0, 100).mapToObj(i -> "element" + i).collect(toSet());
		await(redis -> {
			Set<String> toAdd = new HashSet<>(elements);
			String first = first(toAdd);
			toAdd.remove(first);
			return redis.sadd(key, first, toAdd.toArray(new String[0]));
		});

		Set<String> result = new HashSet<>();
		await(redis -> Promises.until(ZERO_CURSOR,
				cursor -> redis.sscan(key, cursor)
						.map(scanResult -> {
							result.addAll(scanResult.getElements());
							return scanResult.getCursor();
						}),
				ZERO_CURSOR::equals));
		assertEquals(elements, result);

		Set<String> endsWith0Result = await(redis -> redis.sscanStream(key, match("*0")).toCollector(toSet()));
		Set<String> endsWith0Expected = elements.stream().filter(element -> element.endsWith("0")).collect(toSet());
		assertEquals(endsWith0Expected, endsWith0Result);
	}

	@Test
	public void zscan() {
		String key = "key";
		Set<String> elements = IntStream.range(0, 100).mapToObj(i -> "element" + i).collect(toSet());
		Map<String, Double> map = keysToMap(elements, el -> (double) el.hashCode());
		await(redis -> redis.zadd(key, map));

		Map<String, Double> result = new HashMap<>();
		await(redis -> Promises.until(ZERO_CURSOR,
				cursor -> redis.zscan(key, cursor)
						.map(scanResult -> {
							List<byte[]> els = scanResult.getElementsAsBinary();
							for (int i = 0; i < els.size(); ) {
								result.put(new String(els.get(i++)), Double.parseDouble(new String(els.get(i++))));
							}
							return scanResult.getCursor();
						}),
				ZERO_CURSOR::equals));
		assertEquals(map, result);

		Map<String, Double> endsWith0Result = await(redis -> redis.zscanStream(key, match("*0")).toCollector(toMap()));
		Map<String, Double> endsWith0Expected = map.entrySet().stream().filter(entry -> entry.getKey().endsWith("0")).collect(toMap());
		assertEquals(endsWith0Expected, endsWith0Result);
	}

	@Test
	public void hscan() {
		String key = "key";
		Set<String> fields = IntStream.range(0, 100).mapToObj(i -> "field" + i).collect(toSet());
		Map<String, byte[]> map = keysToMap(fields, field -> field.getBytes(UTF_8));
		await(redis -> redis.hset(key, map));

		Map<String, byte[]> result = new HashMap<>();
		await(redis -> Promises.until(ZERO_CURSOR,
				cursor -> redis.hscan(key, cursor)
						.map(scanResult -> {
							List<byte[]> elements = scanResult.getElementsAsBinary();
							for (int i = 0; i < elements.size(); ) {
								result.put(new String(elements.get(i++)), elements.get(i++));
							}
							return scanResult.getCursor();
						}),
				ZERO_CURSOR::equals));
		for (Map.Entry<String, byte[]> expectedEntry : map.entrySet()) {
			assertArrayEquals(expectedEntry.getValue(), result.get(expectedEntry.getKey()));
		}

		Map<String, byte[]> endsWith0Result = await(redis -> redis.hscanStreamAsBinary(key, match("*0")).toCollector(toMap()));
		Map<String, byte[]> endsWith0Expected = map.entrySet().stream().filter(entry -> entry.getKey().endsWith("0")).collect(toMap());
		for (Map.Entry<String, byte[]> expectedEntry : endsWith0Expected.entrySet()) {
			assertArrayEquals(expectedEntry.getValue(), endsWith0Result.get(expectedEntry.getKey()));
		}
	}

}
