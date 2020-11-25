package io.activej.redis;

import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static io.activej.redis.RedisConnection.TRANSACTION_DISCARDED;
import static io.activej.redis.RedisConnection.TRANSACTION_FAILED;
import static io.activej.redis.TestUtils.assertEquals;
import static io.activej.redis.TestUtils.assertOk;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public final class TransactionsTest extends RedisLocalhostTest {
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
								assertEquals(listPromise.getResult(), transactionResult);
								assertEquals(asList(null, "OK", "value", "OK", "new value", 1L), transactionResult);
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
						assertEquals(listPromise.getResult(), transactionResult);
						assertEquals(asList(null, "OK", "value", "OK", "new value", 1L), transactionResult);
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
						assertEquals(list1Promise.getResult(), exec1Promise.getResult());
						assertEquals(asList(null, "OK", "value x", "OK", "new value x"), exec1Promise.getResult());

						assertEquals(list2Promise.getResult(), transaction2Result);
						assertEquals(asList(null, "OK", "value y", "OK", "new value y", 2L), transaction2Result);
					});
		});
	}

	@Test
	public void discard() {
		String key = "key";
		String value = "value";
		await(redis -> redis.multi().then(redis::discard));

		String finalResult = await(redis -> redis.set(key, value)
				.whenResult(TestUtils::assertOk)
				.then(redis::multi)
				.then(() -> {
					Promise<@Nullable String> setPromise = redis.set(key, value + 1);
					return redis.discard()
							.then(() -> {
								assertSame(TRANSACTION_DISCARDED, setPromise.getException());
								return redis.get(key);
							});
				}));

		Assert.assertEquals(value, finalResult);
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

		Assert.assertEquals(value, finalResult);
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

		Assert.assertEquals(value, finalResult);
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
						assertEquals(execPromise.getResult(), singletonList(set2Promise.getResult()));
						assertSame(TRANSACTION_DISCARDED, set3Promise.getException());
						return redis.get(key);
					});
		});

		Assert.assertEquals(value + 1, finalResult);
	}

	@Test
	public void watch() {
		String key1 = "key1";
		String key2 = "key2";
		String value = "value";
		String otherValue = "other value";

		List<Object> execResult = TestUtils.await(client, redis -> redis.watch(key1, key2)
				.then(redis::multi)
				.then(() -> client.getConnection())
				.then(otherConnection -> otherConnection.set(key2, otherValue)
						.whenResult(TestUtils::assertOk)
						.then(result -> otherConnection.quit()))
				.then(() -> {
					Promise<@Nullable String> setPromise = redis.set(key1, value);
					return redis.exec()
							.whenResult(() -> assertSame(TRANSACTION_FAILED, setPromise.getException()));
				}));
		assertNull(execResult);

		assertNull(await(redis -> redis.get(key1)));
		Assert.assertEquals(otherValue, await(redis -> redis.get(key2)));
	}

	@Test
	public void unwatch() {
		String key1 = "key1";
		String key2 = "key2";
		String value = "value";
		String otherValue = "other value";

		List<Object> execResult = TestUtils.await(client, redis -> redis.watch(key1, key2)
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
		assertEquals(singletonList("OK"), execResult);

		Assert.assertEquals(value, await(redis -> redis.get(key1)));
		Assert.assertEquals(otherValue, await(redis -> redis.get(key2)));
	}
}
