package io.activej.redis;

import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.net.connection.Connection;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.activej.common.Checks.checkArgument;
import static io.activej.redis.BitOperator.NOT;
import static io.activej.redis.Command.*;
import static io.activej.redis.Utils.list;
import static java.util.stream.Collectors.toList;

public final class RedisConnection implements RedisAPI, Connection {
	private static final RedisException IN_POOL = new RedisException(RedisConnection.class, "Connection is in pool");
	private static final RedisException ACTIVE_COMMANDS = new RedisException(RedisConnection.class, "Cannot return to pool, there are ongoing commands");
	private static final RedisException UNEXPECTED_NIL = new RedisException(RedisConnection.class, "Received unexpected 'NIL' response");

	private final RedisClient client;
	private final MessagingWithBinaryStreaming<RedisResponse, RedisCommand> messaging;
	private final Charset charset;

	@Nullable
	private SettablePromise<RedisResponse> receiveCb;

	private boolean closed;
	boolean inPool;

	RedisConnection(RedisClient client, MessagingWithBinaryStreaming<RedisResponse, RedisCommand> messaging, Charset charset) {
		this.client = client;
		this.messaging = messaging;
		this.charset = charset;
	}

	// region Redis API
	// region connection
	@Override
	public Promise<String> auth(String password) {
		return send(RedisCommand.of(AUTH, charset, password), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> auth(String username, String password) {
		return send(RedisCommand.of(AUTH, charset, username, password), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> clientSetname(String connectionName) {
		return send(RedisCommand.of(CLIENT_SETNAME, charset, connectionName), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<@Nullable String> clientGetname() {
		return send(RedisCommand.of(CLIENT_GETNAME, charset), this::parseBulkString);
	}

	@Override
	public Promise<String> clientPause(Duration pauseDuration) {
		return send(RedisCommand.of(CLIENT_PAUSE, charset, String.valueOf(pauseDuration.toMillis())), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> echo(String message) {
		return send(RedisCommand.of(ECHO, charset, message), this::parseBulkString);
	}

	@Override
	public Promise<String> ping() {
		return send(RedisCommand.of(PING, charset), this::parseString);
	}

	@Override
	public Promise<String> ping(String message) {
		return send(RedisCommand.of(PING, charset, message), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<Void> quit() {
		return send(RedisCommand.of(QUIT, charset), this::expectOk)
				.then(messaging::sendEndOfStream)
				.whenComplete(this::close);
	}

	@Override
	public Promise<String> select(int dbIndex) {
		checkArgument(dbIndex >= 0, "Negative DB index");
		return send(RedisCommand.of(SELECT, charset, String.valueOf(dbIndex)), RedisConnection::parseSimpleString);
	}
	// endregion

	// region keys
	@Override
	public Promise<Long> del(String key, String... otherKeys) {
		return send(RedisCommand.of(DEL, charset, list(key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<byte[]> dump(String key) {
		return send(RedisCommand.of(DUMP, charset, key), RedisConnection::parseBulk);
	}

	@Override
	public Promise<Long> exists(String key, String... otherKeys) {
		return send(RedisCommand.of(EXISTS, charset, list(key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> expire(String key, long ttlSeconds) {
		return send(RedisCommand.of(EXPIRE, charset, key, String.valueOf(ttlSeconds)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> expireat(String key, long unixTimestampSeconds) {
		return send(RedisCommand.of(EXPIREAT, charset, key, String.valueOf(unixTimestampSeconds)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> keys(String pattern) {
		return send(RedisCommand.of(KEYS, charset, pattern), response -> parseArray(response, byte[].class, bytes -> new String(bytes, charset))
				.then(RedisConnection::nonNull));
	}

	@Override
	public Promise<Long> move(String key, int dbIndex) {
		checkArgument(dbIndex >= 0, "Negative DB index");
		return send(RedisCommand.of(MOVE, charset, key, String.valueOf(dbIndex)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> persist(String key) {
		return send(RedisCommand.of(PERSIST, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> pexpire(String key, long ttlMillis) {
		return send(RedisCommand.of(PEXPIRE, charset, key, String.valueOf(ttlMillis)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> pexpireat(String key, long unixTimestampMillis) {
		return send(RedisCommand.of(PEXPIREAT, charset, key, String.valueOf(unixTimestampMillis)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> pttl(String key) {
		return send(RedisCommand.of(PTTL, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<@Nullable String> randomkey() {
		return send(RedisCommand.of(RANDOMKEY, charset), this::parseBulkString);
	}

	@Override
	public Promise<String> rename(String key, String newKey) {
		return send(RedisCommand.of(RENAME, charset, key, newKey), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> renamenx(String key, String newKey) {
		return send(RedisCommand.of(RENAMENX, charset, key, newKey), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<Void> restore(String key, Duration ttl, byte[] dump) {
		return send(RedisCommand.of(RESTORE, key.getBytes(charset), String.valueOf(ttl.toMillis()).getBytes(charset), dump), this::expectOk);
	}

	@Override
	public Promise<Long> touch(String key, String... otherKeys) {
		return send(RedisCommand.of(TOUCH, charset, list(key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> ttl(String key) {
		return send(RedisCommand.of(TTL, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<RedisType> type(String key) {
		return send(RedisCommand.of(TYPE, charset, key), RedisConnection::parseType);
	}

	@Override
	public Promise<Long> unlink(String key, String... otherKeys) {
		return send(RedisCommand.of(UNLINK, charset, list(key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> wait(int numberOfReplicas, long timeoutMillis) {
		return send(RedisCommand.of(WAIT, charset, String.valueOf(numberOfReplicas), String.valueOf(timeoutMillis)), RedisConnection::parseInteger);
	}
	// endregion

	// region strings
	@Override
	public Promise<Long> append(String key, String value) {
		return append(key, value.getBytes(charset));
	}

	@Override
	public Promise<Long> append(String key, byte[] value) {
		return send(RedisCommand.of(APPEND, key.getBytes(charset), value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> bitcount(String key) {
		return send(RedisCommand.of(BITCOUNT, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> bitcount(String key, int start, int end) {
		return send(RedisCommand.of(BITCOUNT, charset, key, String.valueOf(start), String.valueOf(end)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> bitop(BitOperator operator, String destKey, String sourceKey, String... otherSourceKeys) {
		checkArgument(operator != NOT || otherSourceKeys.length == 0, "BITOP NOT must be called with a single source key");
		return send(RedisCommand.of(BITOP, charset, list(operator.name(), destKey, sourceKey, otherSourceKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> bitpos(String key, boolean bitIsSet) {
		return send(RedisCommand.of(BITPOS, charset, key, bitIsSet ? "1" : "0"), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> bitpos(String key, boolean bitIsSet, int start) {
		return send(RedisCommand.of(BITPOS, charset, key, bitIsSet ? "1" : "0", String.valueOf(start)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> bitpos(String key, boolean bitIsSet, int start, int end) {
		return send(RedisCommand.of(BITPOS, charset, key, bitIsSet ? "1" : "0", String.valueOf(start), String.valueOf(end)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> decr(String key) {
		return send(RedisCommand.of(DECR, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> decrby(String key, long decrByValue) {
		return send(RedisCommand.of(DECRBY, charset, key, String.valueOf(decrByValue)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<@Nullable String> get(String key) {
		return send(RedisCommand.of(GET, charset, key), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> getAsBinary(String key) {
		return send(RedisCommand.of(GET, charset, key), RedisConnection::parseBulk);
	}

	@Override
	public Promise<Long> getbit(String key, int offset) {
		return send(RedisCommand.of(GETBIT, charset, key, String.valueOf(offset)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<String> getrange(String key, int start, int end) {
		return send(RedisCommand.of(GETRANGE, charset, key, String.valueOf(start), String.valueOf(end)), this::parseBulkString);
	}

	@Override
	public Promise<byte[]> getrangeAsBinary(String key, int start, int end) {
		return send(RedisCommand.of(GETRANGE, charset, key, String.valueOf(start), String.valueOf(end)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<@Nullable String> getset(String key, String value) {
		return send(RedisCommand.of(GETSET, charset, key, value), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> getset(String key, byte[] value) {
		return send(RedisCommand.of(GETSET, key.getBytes(charset), value), RedisConnection::parseBulk);
	}

	@Override
	public Promise<Long> incr(String key) {
		return send(RedisCommand.of(INCR, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> incrby(String key, long incrByValue) {
		return send(RedisCommand.of(INCRBY, charset, key, String.valueOf(incrByValue)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Double> incrbyfloat(String key, double incrByFloatValue) {
		return send(RedisCommand.of(INCRBYFLOAT, charset, key, String.valueOf(incrByFloatValue)),
				response -> parseBulkString(response)
						.then(RedisConnection::nonNull)
						.map(Double::parseDouble));
	}

	@Override
	public Promise<List<@Nullable String>> mget(String key, String... otherKeys) {
		return send(RedisCommand.of(MGET, charset, list(key, otherKeys)), response -> parseNullableArray(response, byte[].class, bytes -> new String(bytes, charset)))
				.then(RedisConnection::nonNull);
	}

	@Override
	public Promise<List<@Nullable byte[]>> mgetAsBinary(String key, String... otherKeys) {
		return send(RedisCommand.of(MGET, charset, list(key, otherKeys)), response -> parseNullableArray(response, byte[].class, Function.identity()))
				.then(RedisConnection::nonNull);
	}

	@Override
	public Promise<Void> mset(Map<@NotNull String, @NotNull byte[]> entries) {
		checkArgument(!entries.isEmpty(), "No entry to set");
		List<byte[]> args = entries.entrySet().stream()
				.flatMap(entry -> Stream.of(entry.getKey().getBytes(charset), entry.getValue()))
				.collect(toList());
		return send(RedisCommand.of(MSET, args), this::expectOk);
	}

	@Override
	public Promise<Void> mset(String key, String value, String... otherKeysAndValue) {
		checkArgument(otherKeysAndValue.length % 2 == 0, "Number of keys should equal number of values");
		return send(RedisCommand.of(MSET, charset, list(key, value, otherKeysAndValue)), this::expectOk);
	}

	@Override
	public Promise<Long> msetnx(Map<@NotNull String, @NotNull byte[]> entries) {
		checkArgument(!entries.isEmpty(), "No entry to set");
		List<byte[]> args = entries.entrySet().stream()
				.flatMap(entry -> Stream.of(entry.getKey().getBytes(charset), entry.getValue()))
				.collect(toList());
		return send(RedisCommand.of(MSETNX, args), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> msetnx(String key, String value, String... otherKeysAndValue) {
		checkArgument(otherKeysAndValue.length % 2 == 0, "Number of keys should equal number of values");
		return send(RedisCommand.of(MSETNX, charset, list(key, value, otherKeysAndValue)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<String> psetex(String key, long millis, String value) {
		return send(RedisCommand.of(PSETEX, charset, key, String.valueOf(millis), value), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> psetex(String key, long millis, byte[] value) {
		return send(RedisCommand.of(PSETEX, key.getBytes(charset), String.valueOf(millis).getBytes(charset), value), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<@Nullable String> set(String key, String value, SetModifier... modifiers) {
		return set(key, value.getBytes(charset), modifiers);
	}

	@Override
	public Promise<@Nullable String> set(String key, byte[] value, SetModifier... modifiers) {
		RedisCommand command;
		if (modifiers.length == 0) {
			command = RedisCommand.of(SET, key.getBytes(charset), value);
		} else {
			List<byte[]> arguments = new ArrayList<>(modifiers.length + 2);
			arguments.add(key.getBytes(charset));
			arguments.add(value);
			for (SetModifier modifier : modifiers) {
				modifier.getArguments().stream()
						.map(arg -> arg.getBytes(charset))
						.forEach(arguments::add);
			}
			command = RedisCommand.of(SET, arguments);
		}
		return send(command, this::parseString);
	}
	// endregion
	// endregion

	// region connection and pooling
	@Override
	public boolean isClosed() {
		if (closed) return true;
		if (!messaging.isClosed()) return false;
		close();
		return true;
	}

	@Override
	public Promise<Void> returnToPool() {
		if (receiveCb != null) return Promise.ofException(ACTIVE_COMMANDS);
		if (closed) return Promise.ofException(CLOSE_EXCEPTION);
		if (inPool) return Promise.ofException(IN_POOL);
		client.returnConnection(this);
		return Promise.complete();
	}

	@Override
	public Promise<Void> close(@NotNull Throwable e) {
		if (closed) return Promise.complete();
		closed = true;
		return Promise.complete()
				.then(() -> receiveCb == null ? Promise.complete() : receiveCb.toVoid())
				.whenComplete(() -> {
					messaging.closeEx(e);
					client.onConnectionClose(this);
				});
	}
	// endregion

	private <T> Promise<T> send(RedisCommand command, Function<RedisResponse, Promise<@Nullable T>> responseParser) {
		if (isClosed()) return Promise.ofException(CLOSE_EXCEPTION);
		if (inPool) return Promise.ofException(IN_POOL);

		Promise<Void> sendPromise = messaging.send(command);
		Promise<RedisResponse> receivePromise = receive();

		return sendPromise.then(() -> receivePromise)
				.then(responseParser);
	}

	private Promise<RedisResponse> receive() {
		if (receiveCb == null) {
			Promise<RedisResponse> receivePromise = messaging.receive();
			if (receivePromise.isComplete()) return receivePromise;

			SettablePromise<RedisResponse> newCb = new SettablePromise<>();
			receivePromise
					.whenComplete(() -> receiveCb = null)
					.whenComplete(newCb);
			receiveCb = newCb;
			return receivePromise;
		} else {
			SettablePromise<RedisResponse> newCb = new SettablePromise<>();
			receiveCb.whenException(newCb::setException)
					.whenResult(() -> messaging.receive().whenComplete(newCb));
			receiveCb = newCb;
			return newCb;
		}
	}

	private static Promise<String> parseSimpleString(RedisResponse response) {
		if (response.isString()) {
			return Promise.of(response.getString());
		}
		return parseError(response);
	}

	private static Promise<Long> parseInteger(RedisResponse response) {
		if (response.isInteger()) {
			return Promise.of(response.getInteger());
		}
		return parseError(response);
	}

	private static Promise<RedisType> parseType(RedisResponse response) {
		return parseSimpleString(response)
				.then(type -> {
					try {
						return Promise.of(RedisType.valueOf(type.toUpperCase()));
					} catch (IllegalArgumentException e) {
						return Promise.ofException(new RedisException(RedisConnection.class, "Type '" + type + "' is not known"));
					}
				});
	}

	private static Promise<@Nullable byte[]> parseBulk(RedisResponse response) {
		if (response.isBytes()) {
			return Promise.of(response.getBytes());
		}
		if (response.isNil()) {
			return Promise.of(null);
		}
		return parseError(response);
	}

	private Promise<@Nullable String> parseBulkString(RedisResponse response) {
		return parseBulk(response)
				.map(bytes -> bytes == null ? null : new String(bytes, charset));
	}

	private Promise<@Nullable String> parseString(RedisResponse response) {
		return response.isString() ?
				parseSimpleString(response) :
				parseBulk(response)
						.map(bytes -> bytes == null ? null : new String(bytes, charset));
	}

	private static Promise<@Nullable List<?>> parseArray(RedisResponse response) {
		if (response.isArray()) {
			return Promise.of(response.getArray());
		}
		if (response.isNil()) {
			return Promise.of(null);
		}
		return parseError(response);
	}

	private static <T, U> Promise<@Nullable List<T>> parseArray(RedisResponse response, Class<U> expectedClass, Function<U, T> fn) {
		return parseArray(response, expectedClass, fn, false);
	}

	private static <T, U> Promise<@Nullable List<T>> parseNullableArray(RedisResponse response, Class<U> expectedClass, Function<U, T> fn) {
		return parseArray(response, expectedClass, fn, true);
	}

	@SuppressWarnings("unchecked")
	private static <T, U> Promise<@Nullable List<T>> parseArray(RedisResponse response, Class<U> expectedClass, Function<U, T> fn, boolean nullable) {
		return parseArray(response)
				.then(array -> {
					if (array == null || array.isEmpty()) return Promise.of((List<T>) array);

					List<T> result = new ArrayList<>(array.size());
					for (Object element : array) {
						if (element == null && nullable) {
							result.add(null);
							continue;
						}
						if (element == null || !expectedClass.isAssignableFrom(element.getClass())) {
							return Promise.ofException(new RedisException(RedisConnection.class,
									"Expected all array items to be of type '" + expectedClass.getSimpleName() +
											"', but some element was not: " + element));
						}
						result.add(fn.apply(expectedClass.cast(element)));
					}
					return Promise.of(result);
				});
	}

	private static <T> Promise<T> parseError(RedisResponse response) {
		if (response.isError()) {
			return Promise.ofException(response.getError());
		}
		return Promise.ofException(new RedisException(RedisConnection.class, "Unexpected response: " + response));
	}

	private Promise<Void> expectOk(RedisResponse response) {
		return parseString(response)
				.then(result -> "OK".equals(result) ? Promise.complete() :
						Promise.ofException(new RedisException(RedisConnection.class, "Expected result to be 'OK', was: " + result)));
	}

	private static <T> Promise<@NotNull T> nonNull(@Nullable T value) {
		return value == null ?
				Promise.ofException(UNEXPECTED_NIL) :
				Promise.of(value);
	}
}
