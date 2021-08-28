package io.activej.redis;

import io.activej.common.ApplicationSettings;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.hamcrest.Matchers;
import org.junit.*;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.promise.TestUtils.awaitException;
import static io.activej.redis.RedisResponse.*;
import static io.activej.redis.TestUtils.assertDeepEquals;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public final class RedisConnectionTestWithReal extends RedisConnectionTestWithStub {
	private static final InetSocketAddress ADDRESS = ApplicationSettings.getInetSocketAddress(RedisConnectionTestWithReal.class, "address", null);

	// Authentication tests modify ACL configuration, run at your own risk
	private static final boolean RUN_AUTH_TESTS = ApplicationSettings.getBoolean(RedisConnectionTestWithReal.class, "runAuthTests", false);

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
		await(connection -> connection.cmd(RedisRequest.of("FLUSHALL"), OK));
	}

	@Test
	public void malformedRequest() {
		String result = await(redis -> redis.cmd(RedisRequest.of("SET", "x", "value"), OK)
				.then(() -> redis.cmd(RedisRequest.of("GETTTTT"), BYTES_ISO_8859_1)
						.then(($, e) -> {
							assertNotNull(e);
							assertThat(e, instanceOf(ServerError.class));
							assertThat(e.getMessage(), containsString("ERR unknown command `GETTTTT`"));
							return redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1);
						})));
		assertEquals("value", result);
	}

	@Test
	public void simpleTransaction() {
		await(redis -> redis.multi()
				.then(() -> {
					Promise<Object[]> listPromise = Promises.toArray(Object.class,
							redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
							redis.cmd(RedisRequest.of("SET", "x", "value"), OK),
							redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
							redis.cmd(RedisRequest.of("SET", "x", "new value"), OK),
							redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
							redis.cmd(RedisRequest.of("DEL", "x", "y"), LONG)
					);
					return redis.exec()
							.whenResult(transactionResult -> {
								assertDeepEquals(listPromise.getResult(), transactionResult);
								assertDeepEquals(new Object[]{null, null, "value", null, "new value", 1L}, transactionResult);
							});
				}));
	}

	@Test
	public void simpleTransactionWithError() {
		String result = await(redis -> redis.multi()
				.then(() -> {
					Promise<Object[]> listPromise = Promises.toArray(Object.class,
							redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
							redis.cmd(RedisRequest.of("SETTTTTTT"), OK),
							redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
							redis.cmd(RedisRequest.of("SET", "x", "new value"), OK),
							redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
							redis.cmd(RedisRequest.of("DEL", "x", "y"), LONG));
					return redis.exec()
							.then(($, e) -> {
								assertNotNull(e);
								assertThat(e, instanceOf(ServerError.class));
								assertThat(e.getMessage(), containsString("EXECABORT Transaction discarded because of previous errors."));

								Exception exception = listPromise.getException();
								assertThat(exception, instanceOf(ServerError.class));
								assertThat(exception.getMessage(), containsString("ERR unknown command `SETTTTTTT`"));
								return redis.cmd(RedisRequest.of("SET", "x", "value"), OK)
										.then(() -> redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1));
							});
				}));
		assertEquals("value", result);
	}

	@Test
	public void pipelinedTransaction() {
		await(redis -> {
			Promise<Void> multiPromise = redis.multi();
			Promise<Object[]> listPromise = Promises.toArray(Object.class,
					redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
					redis.cmd(RedisRequest.of("SET", "x", "value"), OK),
					redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
					redis.cmd(RedisRequest.of("SET", "x", "new value"), OK),
					redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
					redis.cmd(RedisRequest.of("DEL", "x", "y"), LONG)
			);
			return redis.exec()
					.whenResult(transactionResult -> {
						assertTrue(multiPromise.isResult());
						assertDeepEquals(listPromise.getResult(), transactionResult);
						assertDeepEquals(new Object[]{null, null, "value", null, "new value", 1L}, transactionResult);
					});
		});
	}

	@Test
	public void pipelinedMultipleTransactions() {
		await(redis -> {
			Promise<Void> multi1Promise = redis.multi();
			Promise<Object[]> list1Promise = Promises.toArray(Object.class,
					redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
					redis.cmd(RedisRequest.of("SET", "x", "value x"), OK),
					redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
					redis.cmd(RedisRequest.of("SET", "x", "new value x"), OK),
					redis.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1)
			);
			Promise<Object[]> exec1Promise = redis.exec();

			Promise<Void> multi2Promise = redis.multi();
			Promise<Object[]> list2Promise = Promises.toArray(Object.class,
					redis.cmd(RedisRequest.of("GET", "y"), BYTES_ISO_8859_1),
					redis.cmd(RedisRequest.of("SET", "y", "value y"), OK),
					redis.cmd(RedisRequest.of("GET", "y"), BYTES_ISO_8859_1),
					redis.cmd(RedisRequest.of("SET", "y", "new value y"), OK),
					redis.cmd(RedisRequest.of("GET", "y"), BYTES_ISO_8859_1),
					redis.cmd(RedisRequest.of("DEL", "x", "y"), LONG)
			);

			return redis.exec()
					.whenResult(transaction2Result -> {
						assertTrue(multi1Promise.isResult() && multi2Promise.isResult());
						assertDeepEquals(list1Promise.getResult(), exec1Promise.getResult());
						assertDeepEquals(new Object[]{null, null, "value x", null, "new value x"}, exec1Promise.getResult());

						assertDeepEquals(list2Promise.getResult(), transaction2Result);
						assertDeepEquals(new Object[]{null, null, "value y", null, "new value y", 2L}, transaction2Result);
					});
		});
	}

	@Test
	public void discard() {
		String key = "key";
		String value = "value";
		await(redis -> redis.multi().then(redis::discard));

		String finalResult = await(redis -> redis.cmd(RedisRequest.of("SET", key, value), OK)
				.then(redis::multi)
				.then(() -> {
					Promise<Void> setPromise = redis.cmd(RedisRequest.of("SET", key, value + 1), OK);
					return redis.discard()
							.then(() -> {
								assertThat(setPromise.getException(), instanceOf(TransactionDiscardedException.class));
								return redis.cmd(RedisRequest.of("GET", key), BYTES_ISO_8859_1);
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
			Promise<Void> set1Promise = redis.cmd(RedisRequest.of("SET", key, value), OK);
			Promise<Void> multiPromise = redis.multi();
			Promise<Void> set2Promise = redis.cmd(RedisRequest.of("SET", key, value + 1), OK);
			return redis.discard()
					.then(() -> {
						assertTrue(set1Promise.isResult());
						assertTrue(multiPromise.isResult());
						assertThat(set2Promise.getException(), instanceOf(TransactionDiscardedException.class));
						return redis.cmd(RedisRequest.of("GET", "key"), BYTES_ISO_8859_1);
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
			Promise<Void> set1Promise = redis.cmd(RedisRequest.of("SET", key, value), OK);
			Promise<Void> multi1Promise = redis.multi();
			Promise<Void> set2Promise = redis.cmd(RedisRequest.of("SET", key, value + 1), OK);
			Promise<Void> discard1Promise = redis.discard();
			Promise<Void> multi2Promise = redis.multi();
			Promise<Void> set3Promise = redis.cmd(RedisRequest.of("SET", key, value + 2), OK);
			return redis.discard()
					.then(() -> {
						assertTrue(set1Promise.isResult());
						assertTrue(multi1Promise.isResult());
						assertThat(set2Promise.getException(), instanceOf(TransactionDiscardedException.class));
						assertTrue(multi2Promise.isResult());
						assertTrue(discard1Promise.isResult());
						assertThat(set3Promise.getException(), instanceOf(TransactionDiscardedException.class));
						return redis.cmd(RedisRequest.of("GET", key), BYTES_ISO_8859_1);
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
			Promise<Void> set1Promise = redis.cmd(RedisRequest.of("SET", key, value), OK);
			Promise<Void> multi1Promise = redis.multi();
			Promise<Void> set2Promise = redis.cmd(RedisRequest.of("SET", key, value + 1), OK);
			Promise<Object[]> execPromise = redis.exec();
			Promise<Void> multi2Promise = redis.multi();
			Promise<Void> set3Promise = redis.cmd(RedisRequest.of("SET", key, value + 2), OK);
			return redis.discard()
					.then(() -> {
						assertTrue(set1Promise.isResult());
						assertTrue(multi1Promise.isResult());
						assertTrue(set2Promise.isResult());
						assertTrue(multi2Promise.isResult());
						assertDeepEquals(execPromise.getResult(), new Object[]{set2Promise.getResult()});
						assertThat(set3Promise.getException(), instanceOf(TransactionDiscardedException.class));
						return redis.cmd(RedisRequest.of("GET", key), BYTES_ISO_8859_1);
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

		io.activej.redis.TestUtils.await(client, redis -> redis.cmd(RedisRequest.of("WATCH", key1, key2), OK)
				.then(redis::multi)
				.then(() -> client.connect())
				.then(otherConnection -> otherConnection.cmd(RedisRequest.of("SET", key2, otherValue), OK)
						.then(otherConnection::quit))
				.then(() -> {
					Promise<Void> setPromise = redis.cmd(RedisRequest.of("SET", key1, value), OK);
					return redis.exec()
							.then((result, e) -> {
								assertThat(e, instanceOf(TransactionFailedException.class));
								assertThat(setPromise.getException(), instanceOf(TransactionFailedException.class));
								return Promise.complete();
							});
				}));

		assertNull(await(redis -> redis.cmd(RedisRequest.of("GET", key1), BYTES_ISO_8859_1)));
		assertEquals(otherValue, await(redis -> redis.cmd(RedisRequest.of("GET", key2), BYTES_ISO_8859_1)));
	}

	@Test
	public void unwatch() {
		String key1 = "key1";
		String key2 = "key2";
		String value = "value";
		String otherValue = "other value";

		Object[] execResult = io.activej.redis.TestUtils.await(client, redis -> redis.cmd(RedisRequest.of("WATCH", key1, key2), OK)
				.then(() -> redis.cmd(RedisRequest.of("UNWATCH"), OK))
				.then(redis::multi)
				.then(() -> client.connect())
				.then(otherConnection -> otherConnection.cmd(RedisRequest.of("SET", key2, otherValue), OK)
						.then(otherConnection::quit))
				.then(() -> {
					Promise<Void> setPromise = redis.cmd(RedisRequest.of("SET", key1, value), OK);
					return redis.exec()
							.whenResult(() -> assertTrue(setPromise.isResult()));
				}));
		assertArrayEquals(new Object[]{null}, execResult);

		assertEquals(value, await(redis -> redis.cmd(RedisRequest.of("GET", key1), BYTES_ISO_8859_1)));
		assertEquals(otherValue, await(redis -> redis.cmd(RedisRequest.of("GET", key2), BYTES_ISO_8859_1)));
	}

	@Test
	public void quitDuringTransaction() {
		io.activej.promise.TestUtils.await(client.connect()
				.then(connection -> connection.multi()
						.then(() -> {
							List<Promise<?>> promiseList = asList(
									connection.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
									connection.cmd(RedisRequest.of("SET", "x", "value"), OK),
									connection.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
									connection.cmd(RedisRequest.of("SET", "x", "new value"), OK),
									connection.cmd(RedisRequest.of("GET", "x"), BYTES_ISO_8859_1),
									connection.cmd(RedisRequest.of("DEL", "x", "y"), LONG)
							);
							return connection.quit()
									.whenResult(() -> {
										for (Promise<?> promise : promiseList) {
											assertThat(promise.getException(), instanceOf(QuitCalledException.class));
										}
									});
						})
				));
	}

	@Test
	public void passwordAuthentication() {
		assumeTrue(RUN_AUTH_TESTS);

		byte[] password = new byte[100];
		ThreadLocalRandom.current().nextBytes(password);

		await(connection -> connection.cmd(RedisRequest.of("ACL", "SETUSER", "default", "resetpass", setPass(password)), OK));

		try {
			RedisAuthenticationException exception = awaitException(client.connect()
					.then(connection -> connection.cmd(RedisRequest.of("PING"), STRING)
							.whenException(connection::close)));

			assertThat(exception.getMessage(), Matchers.containsString("Authentication required"));

			String response = io.activej.promise.TestUtils.await(client.connect(password)
					.then(connection -> connection.cmd(RedisRequest.of("PING"), STRING)
							.whenComplete(connection::close)));

			assertEquals("PONG", response);
		} finally {
			io.activej.promise.TestUtils.await(client.connect(password)
					.then(connection -> connection.cmd(RedisRequest.of("ACL", "SETUSER", "default", "nopass"), OK)
							.whenComplete(connection::close)));
		}
	}

	@Test
	public void usernamePasswordAuthentication() {
		assumeTrue(RUN_AUTH_TESTS);

		byte[] username = new byte[100];
		byte[] password = new byte[100];
		ThreadLocalRandom.current().nextBytes(username);
		ThreadLocalRandom.current().nextBytes(password);

		escapeUsername(username);

		await(connection -> connection.cmd(RedisRequest.of("ACL", "SETUSER", username, "on", setPass(password), "+PING"), OK));

		try {
			String response = io.activej.promise.TestUtils.await(client.connect(username, password)
					.then(connection -> connection.cmd(RedisRequest.of("PING"), STRING)
							.whenComplete(connection::close)));

			assertEquals("PONG", response);
		} finally {
			Long deleted = await(connection -> connection.cmd(RedisRequest.of("ACL", "DELUSER", username), LONG));
			assertEquals(1, deleted.longValue());
		}
	}

	@Test
	public void invalidCredentials() {
		byte[] password = new byte[100];
		ThreadLocalRandom.current().nextBytes(password);

		Exception exception = awaitException(client.connect(password));
		assertThat(exception, instanceOf(RedisAuthenticationException.class));
	}

	@Test
	public void missingCommandPermissions() {
		assumeTrue(RUN_AUTH_TESTS);

		byte[] username = new byte[100];
		byte[] password = new byte[100];
		ThreadLocalRandom.current().nextBytes(username);
		ThreadLocalRandom.current().nextBytes(password);

		escapeUsername(username);

		await(connection -> connection.cmd(RedisRequest.of("ACL", "SETUSER", username, "on", setPass(password), "+PING"), OK));

		try {
			String response = io.activej.promise.TestUtils.await(client.connect(username, password)
					.then(connection -> connection.cmd(RedisRequest.of("PING"), STRING)
							.whenComplete(connection::close)));

			assertEquals("PONG", response);

			Exception exception = awaitException(client.connect(username, password)
					.then(connection -> connection.cmd(RedisRequest.of("GET", "x"), STRING)
							.whenComplete(connection::close)));

			assertThat(exception, instanceOf(RedisPermissionException.class));
		} finally {
			Long deleted = await(connection -> connection.cmd(RedisRequest.of("ACL", "DELUSER", username), LONG));
			assertEquals(1, deleted.longValue());
		}
	}

	@Test
	public void missingKeyPermissions() {
		assumeTrue(RUN_AUTH_TESTS);

		byte[] username = new byte[100];
		byte[] password = new byte[100];
		ThreadLocalRandom.current().nextBytes(username);
		ThreadLocalRandom.current().nextBytes(password);

		escapeUsername(username);

		await(connection -> connection.cmd(RedisRequest.of("ACL", "SETUSER", username, "on", setPass(password), "allcommands", "~a"), OK));

		try {
			String result = io.activej.promise.TestUtils.await(client.connect(username, password)
					.then(connection -> connection.cmd(RedisRequest.of("GET", "a"), BYTES_ISO_8859_1)
							.whenComplete(connection::close)));

			assertNull(result);

			Exception exception = awaitException(client.connect(username, password)
					.then(connection -> connection.cmd(RedisRequest.of("GET", "b"), BYTES_ISO_8859_1)
							.whenComplete(connection::close)));

			assertThat(exception, instanceOf(RedisPermissionException.class));
		} finally {
			Long deleted = await(connection -> connection.cmd(RedisRequest.of("ACL", "DELUSER", username), LONG));
			assertEquals(1, deleted.longValue());
		}
	}

	// Spaces and NULL are forbidden in usernames
	private static void escapeUsername(byte[] username) {
		for (int i = 0; i < username.length; i++) {
			byte b = username[i];
			if (b == 0 || (b >= 9 && b <= 13) || b == ' ') {
				username[i] = '.';
			}
		}
	}

	private static byte[] setPass(byte[] password) {
		byte[] setPassword = new byte[101];
		setPassword[0] = '>';
		System.arraycopy(password, 0, setPassword, 1, password.length);
		return setPassword;
	}
}
