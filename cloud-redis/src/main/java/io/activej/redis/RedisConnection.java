package io.activej.redis;

import io.activej.common.api.ParserFunction;
import io.activej.common.collection.Either;
import io.activej.common.exception.parse.ParseException;
import io.activej.net.connection.Connection;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.activej.redis.api.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.common.collection.CollectionUtils.transformIterator;
import static io.activej.redis.Utils.*;
import static io.activej.redis.api.BitOperator.NOT;
import static io.activej.redis.api.Command.*;
import static io.activej.redis.api.GeoradiusModifier.*;
import static java.util.stream.Collectors.toList;

public final class RedisConnection implements RedisAPI, Connection {
	public static final RedisException IN_POOL = new RedisException(RedisConnection.class, "Connection is in pool");
	public static final RedisException ACTIVE_COMMANDS = new RedisException(RedisConnection.class, "Cannot return to pool, there are ongoing commands");
	public static final RedisException UNEXPECTED_NIL = new RedisException(RedisConnection.class, "Received unexpected 'NIL' response");
	public static final RedisException UNEXPECTED_SIZE_OF_ARRAY = new RedisException(RedisConnection.class, "Received array of unexpected size");
	public static final RedisException UNEXPECTED_TYPES_IN_ARRAY = new RedisException(RedisConnection.class, "Received array with elements of unexpected type");
	public static final RedisException UNEXPECTED_RESPONSE = new RedisException(RedisConnection.class, "Server responded with unexpected response");
	public static final RedisException RESPONSES_SIZE_MISMATCH = new RedisException(RedisConnection.class, "Number of responses in transaction does not match number of pending commands");
	public static final RedisException UNEVEN_MAP = new RedisException(RedisConnection.class, "Map with uneven keys and values");
	public static final RedisException QUEUED_EXPECTED = new RedisException(RedisConnection.class, "Expected server to respond with 'QUEUED' response");
	public static final RedisException TRANSACTION_FAILED = new RedisException(RedisConnection.class, "Transaction failed");
	public static final RedisException TRANSACTION_DISCARDED = new RedisException(RedisConnection.class, "Transaction discarded");
	public static final RedisException QUIT_CALLED = new RedisException(RedisConnection.class, "Transaction discarded because QUIT was called");

	private static final long NO_TRANSACTION = 0;

	private final RedisClient client;
	private final RedisMessaging messaging;
	private final Charset charset;

	private long transactions;
	private long completedTransactions;

	@Nullable List<Object> transactionResult;

	private final Queue<SettablePromise<RedisResponse>> receiveQueue = new ArrayDeque<>();
	private final Queue<Callback> multiResultQueue = new ArrayDeque<>();

	private boolean closed;
	boolean inPool;

	RedisConnection(RedisClient client, RedisMessaging messaging, Charset charset) {
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
		transactionResult = null;
		while (completedTransactions++ != transactions) {
			abortTransaction(QUIT_CALLED);
		}
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

	// region server
	@Override
	public Promise<String> flushAll(boolean async) {
		return async ?
				send(RedisCommand.of(FLUSHALL, ASYNC.getBytes(charset)), RedisConnection::parseSimpleString) :
				send(RedisCommand.of(FLUSHALL), RedisConnection::parseSimpleString);
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
		return send(RedisCommand.of(KEYS, charset, pattern), this::parseStrings);
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
		return send(RedisCommand.of(INCRBYFLOAT, charset, key, String.valueOf(incrByFloatValue)), this::parseDouble);
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
	public Promise<Void> mset(String key, String value, String... otherKeysAndValues) {
		checkArgument(otherKeysAndValues.length % 2 == 0, "Number of keys should equal number of values");
		return send(RedisCommand.of(MSET, charset, list(key, value, otherKeysAndValues)), this::expectOk);
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
	public Promise<Long> msetnx(String key, String value, String... otherKeysAndValues) {
		checkArgument(otherKeysAndValues.length % 2 == 0, "Number of keys should equal number of values");
		return send(RedisCommand.of(MSETNX, charset, list(key, value, otherKeysAndValues)), RedisConnection::parseInteger);
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
			checkSetModifiers(modifiers);

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

	@Override
	public Promise<Long> setbit(String key, int offset, int value) {
		checkArgument(value == 0 || value == 1, "Only values 1 and 0 are allowed");
		checkArgument(offset >= 0, "Offset must not be less than 0");
		return send(RedisCommand.of(SETBIT, charset, key, String.valueOf(offset), String.valueOf(value)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<String> setex(String key, long seconds, String value) {
		return send(RedisCommand.of(SETEX, charset, key, String.valueOf(seconds), value), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> setex(String key, long seconds, byte[] value) {
		return send(RedisCommand.of(SETEX, key.getBytes(charset), String.valueOf(seconds).getBytes(charset), value), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<Long> setnx(String key, String value) {
		return send(RedisCommand.of(SETNX, charset, key, value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> setnx(String key, byte[] value) {
		return send(RedisCommand.of(SETNX, key.getBytes(charset), value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> setrange(String key, int offset, String value) {
		return send(RedisCommand.of(SETRANGE, charset, key, String.valueOf(offset), value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> setrange(String key, int offset, byte[] value) {
		return send(RedisCommand.of(SETRANGE, key.getBytes(charset), String.valueOf(offset).getBytes(charset), value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> strlen(String key) {
		return send(RedisCommand.of(STRLEN, charset, key), RedisConnection::parseInteger);
	}
	// endregion

	// region lists
	@Override
	public Promise<@Nullable ListPopResult> blpop(double timeoutSeconds, String key, String... otherKeys) {
		return send(RedisCommand.of(BLPOP, charset, list(key, otherKeys, String.valueOf(timeoutSeconds))), this::parseListPopResult);
	}

	@Override
	public Promise<@Nullable ListPopResult> brpop(double timeoutSeconds, String key, String... otherKeys) {
		return send(RedisCommand.of(BRPOP, charset, list(key, otherKeys, String.valueOf(timeoutSeconds))), this::parseListPopResult);
	}

	@Override
	public Promise<@Nullable String> brpoplpush(String source, String target, double timeoutSeconds) {
		String s = String.valueOf(timeoutSeconds);
		return send(RedisCommand.of(BRPOPLPUSH, charset, source, target, s), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> brpoplpushAsBinary(String source, String target, double timeoutSeconds) {
		return send(RedisCommand.of(BRPOPLPUSH, charset, source, target, String.valueOf(timeoutSeconds)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<@Nullable String> lindex(String key, long index) {
		return send(RedisCommand.of(LINDEX, charset, key, String.valueOf(index)), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> lindexAsBinary(String key, long index) {
		return send(RedisCommand.of(LINDEX, charset, key, String.valueOf(index)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<Long> linsert(String key, InsertPosition position, String pivot, String element) {
		return send(RedisCommand.of(LINSERT, charset, key, position.name(), pivot, element), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> linsert(String key, InsertPosition position, byte[] pivot, byte[] element) {
		return send(RedisCommand.of(LINSERT, key.getBytes(charset), position.name().getBytes(charset), pivot, element), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> llen(String key) {
		return send(RedisCommand.of(LLEN, key.getBytes(charset)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<@Nullable String> lpop(String key) {
		return send(RedisCommand.of(LPOP, key.getBytes(charset)), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> lpopAsBinary(String key) {
		return send(RedisCommand.of(LPOP, key.getBytes(charset)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<@Nullable Long> lpos(String key, String element, LposModifier... modifiers) {
		return lpos(key, element.getBytes(charset), modifiers);
	}

	@Override
	public Promise<@Nullable Long> lpos(String key, byte[] element, LposModifier... modifiers) {
		RedisCommand command;
		if (modifiers.length == 0) {
			command = RedisCommand.of(LPOS, key.getBytes(charset), element);
		} else {
			checkLposModifiers(modifiers);

			List<byte[]> arguments = new ArrayList<>(modifiers.length + 2);
			arguments.add(key.getBytes(charset));
			arguments.add(element);
			for (LposModifier modifier : modifiers) {
				modifier.getArguments().stream()
						.map(arg -> arg.getBytes(charset))
						.forEach(arguments::add);
			}
			command = RedisCommand.of(LPOS, arguments);
		}
		return send(command, RedisConnection::parseNullableInteger);
	}

	@Override
	public Promise<List<Long>> lpos(String key, String element, int count, LposModifier... modifiers) {
		return lpos(key, element.getBytes(charset), count, modifiers);
	}

	@Override
	public Promise<List<Long>> lpos(String key, byte[] element, int count, LposModifier... modifiers) {
		checkArgument(count >= 0, "COUNT cannot be negative");
		checkLposModifiers(modifiers);

		List<byte[]> arguments = new ArrayList<>(modifiers.length + 4);
		arguments.add(key.getBytes(charset));
		arguments.add(element);
		arguments.add(LposModifier.COUNT.getBytes(charset));
		arguments.add(String.valueOf(count).getBytes(charset));
		for (LposModifier modifier : modifiers) {
			modifier.getArguments().stream()
					.map(arg -> arg.getBytes(charset))
					.forEach(arguments::add);
		}
		return send(RedisCommand.of(LPOS, arguments), response -> parseArray(response, Long.class, Function.identity()));
	}

	@Override
	public Promise<Long> lpush(String key, String element, String... otherElements) {
		return send(RedisCommand.of(LPUSH, charset, list(key, element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> lpush(String key, byte[] element, byte[]... otherElements) {
		return send(RedisCommand.of(LPUSH, list(key.getBytes(charset), element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> lpushx(String key, String element, String... otherElements) {
		return send(RedisCommand.of(LPUSHX, charset, list(key, element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> lpushx(String key, byte[] element, byte[]... otherElements) {
		return send(RedisCommand.of(LPUSHX, list(key.getBytes(charset), element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> lrange(String key, long start, long stop) {
		return send(RedisCommand.of(LRANGE, charset, key, String.valueOf(start), String.valueOf(stop)), response ->
				parseArray(response, byte[].class, bytes -> new String(bytes, charset), false));
	}

	@Override
	public Promise<List<byte[]>> lrangeAsBinary(String key, long start, long stop) {
		return send(RedisCommand.of(LRANGE, charset, key, String.valueOf(start), String.valueOf(stop)), response ->
				parseArray(response, byte[].class, Function.identity(), false));
	}

	@Override
	public Promise<Long> lrem(String key, long count, String element) {
		return send(RedisCommand.of(LREM, charset, key, String.valueOf(count), String.valueOf(element)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> lrem(String key, long count, byte[] element) {
		return send(RedisCommand.of(LREM, key.getBytes(charset), String.valueOf(count).getBytes(charset), element), RedisConnection::parseInteger);
	}

	@Override
	public Promise<String> lset(String key, long index, String element) {
		return send(RedisCommand.of(LSET, charset, key, String.valueOf(index), element), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> lset(String key, long index, byte[] element) {
		return send(RedisCommand.of(LSET, key.getBytes(charset), String.valueOf(index).getBytes(charset), element), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> ltrim(String key, long start, long stop) {
		return send(RedisCommand.of(LTRIM, charset, key, String.valueOf(start), String.valueOf(stop)), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<@Nullable String> rpop(String key) {
		return send(RedisCommand.of(RPOP, key.getBytes(charset)), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> rpopAsBinary(String key) {
		return send(RedisCommand.of(RPOP, key.getBytes(charset)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<@Nullable String> rpoplpush(String source, String destination) {
		return send(RedisCommand.of(RPOPLPUSH, charset, source, destination), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> rpoplpushAsBinary(String source, String destination) {
		return send(RedisCommand.of(RPOPLPUSH, charset, source, destination), RedisConnection::parseBulk);
	}

	@Override
	public Promise<Long> rpush(String key, String element, String... otherElements) {
		return send(RedisCommand.of(RPUSH, charset, list(key, element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> rpush(String key, byte[] element, byte[]... otherElements) {
		return send(RedisCommand.of(RPUSH, list(key.getBytes(charset), element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> rpushx(String key, String element, String... otherElements) {
		return send(RedisCommand.of(RPUSHX, charset, list(key, element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> rpushx(String key, byte[] element, byte[]... otherElements) {
		return send(RedisCommand.of(RPUSHX, list(key.getBytes(charset), element, otherElements)), RedisConnection::parseInteger);
	}
	// endregion

	// region sets
	@Override
	public Promise<Long> sadd(String key, String member, String... otherMembers) {
		return send(RedisCommand.of(SADD, charset, list(key, member, otherMembers)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> sadd(String key, byte[] member, byte[]... otherMembers) {
		return send(RedisCommand.of(SADD, list(key.getBytes(charset), member, otherMembers)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> scard(String key) {
		return send(RedisCommand.of(SCARD, charset, key), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> sdiff(String key, String... otherKeys) {
		return send(RedisCommand.of(SDIFF, charset, list(key, otherKeys)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> sdiffAsBinary(String key, String... otherKeys) {
		return send(RedisCommand.of(SDIFF, charset, list(key, otherKeys)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Long> sdiffstore(String destination, String key, String... otherKeys) {
		return send(RedisCommand.of(SDIFFSTORE, charset, list(destination, key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> sinter(String key, String... otherKeys) {
		return send(RedisCommand.of(SINTER, charset, list(key, otherKeys)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> sinterAsBinary(String key, String... otherKeys) {
		return send(RedisCommand.of(SINTER, charset, list(key, otherKeys)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Long> sinterstore(String destination, String key, String... otherKeys) {
		return send(RedisCommand.of(SINTERSTORE, charset, list(destination, key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> sismember(String key, String member) {
		return sismember(key, member.getBytes(charset));
	}

	@Override
	public Promise<Long> sismember(String key, byte[] member) {
		return send(RedisCommand.of(SISMEMBER, key.getBytes(charset), member), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> smembers(String key) {
		return send(RedisCommand.of(SMEMBERS, key.getBytes(charset)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> smembersAsBinary(String key) {
		return send(RedisCommand.of(SMEMBERS, key.getBytes(charset)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Long> smove(String source, String destination, String member) {
		return smove(source, destination, member.getBytes(charset));
	}

	@Override
	public Promise<Long> smove(String source, String destination, byte[] member) {
		return send(RedisCommand.of(SMOVE, source.getBytes(charset), destination.getBytes(charset), member), RedisConnection::parseInteger);
	}

	@Override
	public Promise<@Nullable String> spop(String key) {
		return send(RedisCommand.of(SPOP, key.getBytes(charset)), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> spopAsBinary(String key) {
		return send(RedisCommand.of(SPOP, key.getBytes(charset)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<List<String>> spop(String key, long count) {
		return send(RedisCommand.of(SPOP, charset, key, String.valueOf(count)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> spopAsBinary(String key, long count) {
		return send(RedisCommand.of(SPOP, charset, key, String.valueOf(count)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<@Nullable String> srandmember(String key) {
		return send(RedisCommand.of(SRANDMEMBER, key.getBytes(charset)), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> srandmemberAsBinary(String key) {
		return send(RedisCommand.of(SRANDMEMBER, key.getBytes(charset)), RedisConnection::parseBulk);
	}

	@Override
	public Promise<List<String>> srandmember(String key, long count) {
		return send(RedisCommand.of(SRANDMEMBER, charset, key, String.valueOf(count)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> srandmemberAsBinary(String key, long count) {
		return send(RedisCommand.of(SRANDMEMBER, charset, key, String.valueOf(count)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Long> srem(String key, String member, String... otherMembers) {
		return send(RedisCommand.of(SREM, charset, list(key, member, otherMembers)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> srem(String key, byte[] member, byte[]... otherMembers) {
		return send(RedisCommand.of(SREM, list(key.getBytes(charset), member, otherMembers)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> sunion(String key, String... otherKeys) {
		return send(RedisCommand.of(SUNION, charset, list(key, otherKeys)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> sunionAsBinary(String key, String... otherKeys) {
		return send(RedisCommand.of(SUNION, charset, list(key, otherKeys)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Long> sunionstore(String destination, String key, String... otherKeys) {
		return send(RedisCommand.of(SUNIONSTORE, charset, list(destination, key, otherKeys)), RedisConnection::parseInteger);
	}
	// endregion

	// region hashes
	@Override
	public Promise<Long> hdel(String key, String field, String... otherFields) {
		return send(RedisCommand.of(HDEL, charset, list(key, field, otherFields)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> hexists(String key, String field) {
		return send(RedisCommand.of(HEXISTS, charset, key, field), RedisConnection::parseInteger);
	}

	@Override
	public Promise<@Nullable String> hget(String key, String field) {
		return send(RedisCommand.of(HGET, charset, key, field), this::parseBulkString);
	}

	@Override
	public Promise<@Nullable byte[]> hgetAsBinary(String key, String field) {
		return send(RedisCommand.of(HGET, charset, key, field), RedisConnection::parseBulk);
	}

	@Override
	public Promise<Map<String, String>> hgetall(String key) {
		return send(RedisCommand.of(HGETALL, key.getBytes(charset)), response -> parseMap(response, field -> new String(field, charset)));
	}

	@Override
	public Promise<Map<String, byte[]>> hgetallAsBinary(String key) {
		return send(RedisCommand.of(HGETALL, key.getBytes(charset)), response -> parseMap(response, v -> v));
	}

	@Override
	public Promise<Long> hincrby(String key, String field, long incrByValue) {
		return send(RedisCommand.of(HINCRBY, charset, key, field, String.valueOf(incrByValue)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Double> hincrbyfloat(String key, String field, double incrByValue) {
		return send(RedisCommand.of(HINCRBYFLOAT, charset, key, field, String.valueOf(incrByValue)), this::parseDouble);
	}

	@Override
	public Promise<List<String>> hkeys(String key) {
		return send(RedisCommand.of(HKEYS, key.getBytes(charset)), this::parseStrings);
	}

	@Override
	public Promise<Long> hlen(String key) {
		return send(RedisCommand.of(HLEN, key.getBytes(charset)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<@Nullable String>> hmget(String key, String field, String... otherFields) {
		return send(RedisCommand.of(HMGET, charset, list(key, field, otherFields)), response -> parseNullableArray(response, byte[].class, bytes -> new String(bytes, charset)))
				.then(RedisConnection::nonNull);
	}

	@Override
	public Promise<List<@Nullable byte[]>> hmgetAsBinary(String key, String field, String... otherFields) {
		return send(RedisCommand.of(HMGET, charset, list(key, field, otherFields)), response -> parseNullableArray(response, byte[].class, Function.identity()))
				.then(RedisConnection::nonNull);
	}

	@Override
	public Promise<String> hmset(String key, Map<@NotNull String, @NotNull byte[]> entries) {
		checkArgument(!entries.isEmpty(), "No entry to set");
		List<byte[]> args = new ArrayList<>(entries.size() * 2 + 1);
		args.add(key.getBytes(charset));
		for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
			args.add(entry.getKey().getBytes(charset));
			args.add(entry.getValue());
		}
		return send(RedisCommand.of(HMSET, args), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<String> hmset(String key, String field, String value, String... otherFieldsAndValues) {
		checkArgument(otherFieldsAndValues.length % 2 == 0, "Number of keys should equal number of values");
		return send(RedisCommand.of(HMSET, charset, list(key, field, value, otherFieldsAndValues)), RedisConnection::parseSimpleString);
	}

	@Override
	public Promise<Long> hset(String key, Map<@NotNull String, @NotNull byte[]> entries) {
		checkArgument(!entries.isEmpty(), "No entry to set");
		List<byte[]> args = new ArrayList<>(entries.size() * 2 + 1);
		args.add(key.getBytes(charset));
		for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
			args.add(entry.getKey().getBytes(charset));
			args.add(entry.getValue());
		}
		return send(RedisCommand.of(HSET, args), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> hset(String key, String field, String value, String... otherFieldsAndValues) {
		checkArgument(otherFieldsAndValues.length % 2 == 0, "Number of keys should equal number of values");
		return send(RedisCommand.of(HSET, charset, list(key, field, value, otherFieldsAndValues)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> hsetnx(String key, String field, String value) {
		return send(RedisCommand.of(HSETNX, charset, key, field, value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> hsetnx(String key, String field, byte[] value) {
		return send(RedisCommand.of(HSETNX, key.getBytes(charset), field.getBytes(charset), value), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> hstrlen(String key, String field) {
		return send(RedisCommand.of(HSTRLEN, charset, key, field), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> hvals(String key) {
		return send(RedisCommand.of(HVALS, key.getBytes(charset)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> hvalsAsBinary(String key) {
		return send(RedisCommand.of(HVALS, key.getBytes(charset)), RedisConnection::parseBytes);
	}
	// endregion

	// region sorted sets
	@Override
	public Promise<@Nullable SetBlockingPopResult> bzpopmin(double timeoutSeconds, String key, String... otherKeys) {
		return send(RedisCommand.of(BZPOPMIN, charset, list(key, otherKeys, String.valueOf(timeoutSeconds))), this::parseSetBlockingPopResult);
	}

	@Override
	public Promise<@Nullable SetBlockingPopResult> bzpopmax(double timeoutSeconds, String key, String... otherKeys) {
		return send(RedisCommand.of(BZPOPMAX, charset, list(key, otherKeys, String.valueOf(timeoutSeconds))), this::parseSetBlockingPopResult);
	}

	@Override
	public Promise<Long> zadd(String key, Map<Double, String> entries, ZaddModifier... modifiers) {
		checkArgument(!entries.isEmpty(), "No entry to add");
		checkZaddModifiers(modifiers);

		List<byte[]> args = new ArrayList<>(entries.size() * 2 + modifiers.length + 1);
		args.add(key.getBytes(charset));
		for (ZaddModifier modifier : modifiers) {
			args.add(modifier.getArgument().getBytes(charset));
		}
		for (Map.Entry<Double, String> entry : entries.entrySet()) {
			args.add(String.valueOf(entry.getKey()).getBytes(charset));
			args.add(entry.getValue().getBytes(charset));
		}
		return send(RedisCommand.of(ZADD, args), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Double> zaddIncr(String key, double score, String value, ZaddModifier... modifiers) {
		checkZaddModifiers(modifiers);
		List<byte[]> args = new ArrayList<>(4 + modifiers.length);
		args.add(key.getBytes(charset));
		for (ZaddModifier modifier : modifiers) {
			args.add(modifier.getArgument().getBytes(charset));
		}
		args.add(ZaddModifier.INCR.getBytes(charset));

		args.add(String.valueOf(score).getBytes(charset));
		args.add(value.getBytes(charset));
		return send(RedisCommand.of(ZADD, args), this::parseDouble);
	}

	@Override
	public Promise<Long> zaddBinary(String key, Map<Double, byte[]> entries, ZaddModifier... modifiers) {
		checkArgument(!entries.isEmpty(), "No entry to add");
		checkZaddModifiers(modifiers);

		List<byte[]> args = new ArrayList<>(entries.size() * 2 + modifiers.length + 1);
		args.add(key.getBytes(charset));
		for (ZaddModifier modifier : modifiers) {
			args.add(modifier.getArgument().getBytes(charset));
		}
		for (Map.Entry<Double, byte[]> entry : entries.entrySet()) {
			args.add(String.valueOf(entry.getKey()).getBytes(charset));
			args.add(entry.getValue());
		}
		return send(RedisCommand.of(ZADD, args), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Double> zaddIncr(String key, double score, byte[] value, ZaddModifier... modifiers) {
		checkZaddModifiers(modifiers);
		List<byte[]> args = new ArrayList<>(4 + modifiers.length);
		args.add(key.getBytes(charset));
		for (ZaddModifier modifier : modifiers) {
			args.add(modifier.getArgument().getBytes(charset));
		}
		args.add(ZaddModifier.INCR.getBytes(charset));

		args.add(String.valueOf(score).getBytes(charset));
		args.add(value);
		return send(RedisCommand.of(ZADD, args), this::parseDouble);
	}

	@Override
	public Promise<Long> zcard(String key) {
		return send(RedisCommand.of(ZCARD, key.getBytes(charset)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> zcount(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZCOUNT, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Double> zincrby(String key, double increment, String member) {
		return send(RedisCommand.of(ZINCRBY, charset, key, String.valueOf(increment), member), this::parseDouble);
	}

	@Override
	public Promise<Double> zincrby(String key, double increment, byte[] member) {
		return send(RedisCommand.of(ZINCRBY, key.getBytes(charset), String.valueOf(increment).getBytes(charset), member), this::parseDouble);
	}

	@Override
	public Promise<Long> zinterstore(String destination, Aggregate aggregate, Map<String, Double> entries) {
		return doZstore(ZINTERSTORE, destination, aggregate, entries);
	}

	@Override
	public Promise<Long> zinterstore(String destination, Map<String, Double> entries) {
		return doZstore(ZINTERSTORE, destination, null, entries);
	}

	@Override
	public Promise<Long> zinterstore(String destination, @Nullable Aggregate aggregate, String key, String... otherKeys) {
		return doZstore(ZINTERSTORE, destination, aggregate, key, otherKeys);
	}

	@Override
	public Promise<Long> zinterstore(String destination, String key, String... otherKeys) {
		return doZstore(ZINTERSTORE, destination, null, key, otherKeys);
	}

	@Override
	public Promise<Long> zlexcount(String key, LexInterval interval) {
		return send(RedisCommand.of(ZLEXCOUNT, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseInteger);
	}

	@Override
	public Promise<@Nullable List<SetPopResult>> zpopmax(String key, long count) {
		if (count == 1) return send(RedisCommand.of(ZPOPMAX, key.getBytes(charset)), this::parseSetPopResults);
		return send(RedisCommand.of(ZPOPMAX, charset, key, String.valueOf(count)), this::parseSetPopResults);
	}

	@Override
	public Promise<@Nullable List<SetPopResult>> zpopmin(String key, long count) {
		if (count == 1) return send(RedisCommand.of(ZPOPMIN, key.getBytes(charset)), this::parseSetPopResults);
		return send(RedisCommand.of(ZPOPMIN, charset, key, String.valueOf(count)), this::parseSetPopResults);
	}

	@Override
	public Promise<List<String>> zrange(String key, long start, long stop) {
		return send(RedisCommand.of(ZRANGE, charset, key, String.valueOf(start), String.valueOf(stop)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrangeAsBinary(String key, long start, long stop) {
		return send(RedisCommand.of(ZRANGE, charset, key, String.valueOf(start), String.valueOf(stop)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Map<String, Double>> zrangeWithScores(String key, long start, long stop) {
		return send(RedisCommand.of(ZRANGE, charset, key, String.valueOf(start), String.valueOf(stop), WITHSCORES),
				response -> parseMap(response, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<byte[], Double>> zrangeAsBinaryWithScores(String key, long start, long stop) {
		return send(RedisCommand.of(ZRANGE, charset, key, String.valueOf(start), String.valueOf(stop), WITHSCORES),
				response -> parseMap(response, v -> v, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<List<String>> zrangebylex(String key, LexInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZRANGEBYLEX, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrangebylexAsBinary(String key, LexInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZRANGEBYLEX, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<List<String>> zrangebylex(String key, LexInterval interval) {
		return send(RedisCommand.of(ZRANGEBYLEX, charset, key, interval.getMin(), interval.getMax()), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrangebylexAsBinary(String key, LexInterval interval) {
		return send(RedisCommand.of(ZRANGEBYLEX, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseBytes);
	}

	@Override
	public Promise<List<String>> zrevrangebylex(String key, LexInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZREVRANGEBYLEX, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrevrangebylexAsBinary(String key, LexInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZREVRANGEBYLEX, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<List<String>> zrevrangebylex(String key, LexInterval interval) {
		return send(RedisCommand.of(ZREVRANGEBYLEX, charset, key, interval.getMin(), interval.getMax()), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrevrangebylexAsBinary(String key, LexInterval interval) {
		return send(RedisCommand.of(ZREVRANGEBYLEX, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseBytes);
	}

	@Override
	public Promise<List<String>> zrangebyscore(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), this::parseStrings);
	}

	@Override
	public Promise<List<String>> zrangebyscore(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax()), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrangebyscoreAsBinary(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<List<byte[]>> zrangebyscoreAsBinary(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Map<String, Double>> zrangebyscoreWithScores(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES, LIMIT,
				String.valueOf(offset), String.valueOf(count)),
				response -> parseMap(response, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<String, Double>> zrangebyscoreWithScores(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES),
				response -> parseMap(response, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<byte[], Double>> zrangebyscoreAsBinaryWithScores(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES, LIMIT,
				String.valueOf(offset), String.valueOf(count)),
				response -> parseMap(response, k -> k, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<byte[], Double>> zrangebyscoreAsBinaryWithScores(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES),
				response -> parseMap(response, k -> k, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<@Nullable Long> zrank(String key, String member) {
		return send(RedisCommand.of(ZRANK, charset, key, member), RedisConnection::parseNullableInteger);
	}

	@Override
	public Promise<@Nullable Long> zrank(String key, byte[] member) {
		return send(RedisCommand.of(ZRANK, key.getBytes(charset), member), RedisConnection::parseNullableInteger);
	}

	@Override
	public Promise<Long> zrem(String key, String member, String... otherMembers) {
		return send(RedisCommand.of(ZREM, charset, list(key, member, otherMembers)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> zrem(String key, byte[] member, byte[]... otherMembers) {
		return send(RedisCommand.of(ZREM, list(key.getBytes(charset), member, otherMembers)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> zremrangebylex(String key, LexInterval interval) {
		return send(RedisCommand.of(ZREMRANGEBYLEX, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> zremrangebyrank(String key, long start, long stop) {
		return send(RedisCommand.of(ZREMRANGEBYRANK, charset, key, String.valueOf(start), String.valueOf(stop)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> zremrangebyscore(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZREMRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> zrevrange(String key, long start, long stop) {
		return send(RedisCommand.of(ZREVRANGE, charset, key, String.valueOf(start), String.valueOf(stop)), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrevrangeAsBinary(String key, long start, long stop) {
		return send(RedisCommand.of(ZREVRANGE, charset, key, String.valueOf(start), String.valueOf(stop)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Map<String, Double>> zrevrangeWithScores(String key, long start, long stop) {
		return send(RedisCommand.of(ZREVRANGE, charset, key, String.valueOf(start), String.valueOf(stop), WITHSCORES),
				response -> parseMap(response, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<byte[], Double>> zrevrangeAsBinaryWithScores(String key, long start, long stop) {
		return send(RedisCommand.of(ZREVRANGE, charset, key, String.valueOf(start), String.valueOf(stop), WITHSCORES),
				response -> parseMap(response, v -> v, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<List<String>> zrevrangebyscore(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax()), this::parseStrings);
	}

	@Override
	public Promise<List<byte[]>> zrevrangebyscoreAsBinary(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), LIMIT,
				String.valueOf(offset), String.valueOf(count)), RedisConnection::parseBytes);
	}

	@Override
	public Promise<List<byte[]>> zrevrangebyscoreAsBinary(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax()), RedisConnection::parseBytes);
	}

	@Override
	public Promise<Map<String, Double>> zrevrangebyscoreWithScores(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES, LIMIT,
				String.valueOf(offset), String.valueOf(count)),
				response -> parseMap(response, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<String, Double>> zrevrangebyscoreWithScores(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES),
				response -> parseMap(response, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<byte[], Double>> zrevrangebyscoreAsBinaryWithScores(String key, ScoreInterval interval, long offset, long count) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES, LIMIT,
				String.valueOf(offset), String.valueOf(count)),
				response -> parseMap(response, k -> k, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<Map<byte[], Double>> zrevrangebyscoreAsBinaryWithScores(String key, ScoreInterval interval) {
		return send(RedisCommand.of(ZREVRANGEBYSCORE, charset, key, interval.getMin(), interval.getMax(), WITHSCORES),
				response -> parseMap(response, k -> k, bytes -> Double.parseDouble(new String(bytes, charset))));
	}

	@Override
	public Promise<@Nullable Long> zrevrank(String key, String member) {
		return send(RedisCommand.of(ZREVRANK, charset, key, member), RedisConnection::parseNullableInteger);
	}

	@Override
	public Promise<@Nullable Long> zrevrank(String key, byte[] member) {
		return send(RedisCommand.of(ZREVRANK, key.getBytes(charset), member), RedisConnection::parseNullableInteger);
	}

	@Override
	public Promise<@Nullable Double> zscore(String key, String member) {
		return send(RedisCommand.of(ZSCORE, charset, key, member), this::parseNullableDouble);
	}

	@Override
	public Promise<@Nullable Double> zscore(String key, byte[] member) {
		return send(RedisCommand.of(ZSCORE, key.getBytes(charset), member), this::parseNullableDouble);
	}

	@Override
	public Promise<Long> zunionstore(String destination, Aggregate aggregate, Map<String, Double> entries) {
		return doZstore(ZUNIONSTORE, destination, aggregate, entries);
	}

	@Override
	public Promise<Long> zunionstore(String destination, Map<String, Double> entries) {
		return doZstore(ZUNIONSTORE, destination, null, entries);
	}

	@Override
	public Promise<Long> zunionstore(String destination, Aggregate aggregate, String key, String... otherKeys) {
		return doZstore(ZUNIONSTORE, destination, aggregate, key, otherKeys);
	}

	@Override
	public Promise<Long> zunionstore(String destination, String key, String... otherKeys) {
		return doZstore(ZUNIONSTORE, destination, null, key, otherKeys);
	}
	// endregion

	// region hyperloglog
	@Override
	public Promise<Long> pfadd(String key, String element, String... otherElements) {
		return send(RedisCommand.of(PFADD, charset, list(key, element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> pfadd(String key, byte[] element, byte[]... otherElements) {
		return send(RedisCommand.of(PFADD, list(key.getBytes(charset), element, otherElements)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> pfcount(String key, String... otherKeys) {
		return send(RedisCommand.of(PFCOUNT, charset, list(key, otherKeys)), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Void> pfmerge(String destKey, String sourceKey, String... otherSourceKeys) {
		return send(RedisCommand.of(PFMERGE, charset, list(destKey, sourceKey, otherSourceKeys)), this::expectOk);
	}
	// endregion

	//region geo
	@Override
	public Promise<Long> geoadd(String key, double longitude, double latitude, String member) {
		return send(RedisCommand.of(GEOADD, charset, key, String.valueOf(longitude), String.valueOf(latitude), member), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> geoadd(String key, double longitude, double latitude, byte[] member) {
		return send(RedisCommand.of(GEOADD, key.getBytes(charset), String.valueOf(longitude).getBytes(charset),
				String.valueOf(latitude).getBytes(charset), member),
				RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> geoadd(String key, Map<String, Coordinate> coordinates) {
		checkArgument(!coordinates.isEmpty(), "Nothing to add");
		List<String> arguments = new ArrayList<>(coordinates.size() * 2 + 1);
		arguments.add(key);
		for (Map.Entry<String, Coordinate> entry : coordinates.entrySet()) {
			Coordinate coordinate = entry.getValue();
			arguments.add(String.valueOf(coordinate.getLongitude()));
			arguments.add(String.valueOf(coordinate.getLatitude()));
			arguments.add(entry.getKey());
		}
		return send(RedisCommand.of(GEOADD, charset, arguments), RedisConnection::parseInteger);
	}

	@Override
	public Promise<Long> geoaddBinary(String key, Map<byte[], Coordinate> coordinates) {
		checkArgument(!coordinates.isEmpty(), "Nothing to add");
		List<byte[]> arguments = new ArrayList<>(coordinates.size() * 2 + 1);
		arguments.add(key.getBytes(charset));
		for (Map.Entry<byte[], Coordinate> entry : coordinates.entrySet()) {
			Coordinate coordinate = entry.getValue();
			arguments.add(String.valueOf(coordinate.getLongitude()).getBytes(charset));
			arguments.add(String.valueOf(coordinate.getLatitude()).getBytes(charset));
			arguments.add(entry.getKey());
		}
		return send(RedisCommand.of(GEOADD, arguments), RedisConnection::parseInteger);
	}

	@Override
	public Promise<List<String>> geohash(String key, String member, String... otherMembers) {
		return send(RedisCommand.of(GEOHASH, charset, list(key, member, otherMembers)), this::parseNullableStrings);
	}

	@Override
	public Promise<List<String>> geohash(String key, byte[] member, byte[]... otherMembers) {
		return send(RedisCommand.of(GEOHASH, list(key.getBytes(charset), member, otherMembers)), this::parseNullableStrings);
	}

	@Override
	public Promise<List<@Nullable Coordinate>> geopos(String key, String member, String... otherMembers) {
		return send(RedisCommand.of(GEOPOS, charset, list(key, member, otherMembers)), this::parseCoordinates);
	}

	@Override
	public Promise<List<@Nullable Coordinate>> geopos(String key, byte[] member, byte[]... otherMembers) {
		return send(RedisCommand.of(GEOPOS, list(key.getBytes(charset), member, otherMembers)), this::parseCoordinates);
	}

	@Override
	public Promise<@Nullable Double> geodist(String key, String member1, String member2, DistanceUnit unit) {
		return send(RedisCommand.of(GEODIST, charset, key, member1, member2, unit.getArgument()), this::parseNullableDouble);
	}

	@Override
	public Promise<@Nullable Double> geodist(String key, String member1, String member2) {
		return send(RedisCommand.of(GEODIST, charset, key, member1, member2), this::parseNullableDouble);
	}

	@Override
	public Promise<@Nullable Double> geodist(String key, byte[] member1, byte[] member2, DistanceUnit unit) {
		return send(RedisCommand.of(GEODIST, key.getBytes(charset), member1, member2, unit.getArgument().getBytes(charset)), this::parseNullableDouble);
	}

	@Override
	public Promise<@Nullable Double> geodist(String key, byte[] member1, byte[] member2) {
		return send(RedisCommand.of(GEODIST, key.getBytes(charset), member1, member2), this::parseNullableDouble);
	}

	@Override
	public Promise<Long> georadius(String key, Coordinate coordinate, double radius, DistanceUnit unit, GeoradiusModifier... modifiers) {
		return doGeoradiusStore(key, Either.left(coordinate), radius, unit, modifiers);
	}

	@Override
	public Promise<List<GeoradiusResult>> georadiusReadOnly(String key, Coordinate coordinate, double radius, DistanceUnit unit, GeoradiusModifier... modifiers) {
		return doGeoradiusReadOnly(key, Either.left(coordinate), radius, unit, modifiers);
	}

	@Override
	public Promise<Long> georadiusbymember(String key, String member, double radius, DistanceUnit unit, GeoradiusModifier... modifiers) {
		return doGeoradiusStore(key, Either.right(member), radius, unit, modifiers);
	}

	@Override
	public Promise<List<GeoradiusResult>> georadiusbymemberReadOnly(String key, String member, double radius, DistanceUnit unit, GeoradiusModifier... modifiers) {
		return doGeoradiusReadOnly(key, Either.right(member), radius, unit, modifiers);
	}
	// endregion

	// region transactions
	@Override
	public Promise<Void> discard() {
		if (isClosed()) return Promise.ofException(CLOSE_EXCEPTION);
		checkState(inTransaction(), "DISCARD without MULTI");
		transactionResult = null;
		long transactionId = ++completedTransactions;
		return send(RedisCommand.of(DISCARD), response -> {
			abortTransaction(TRANSACTION_DISCARDED, transactionId);
			return expectOk(response);
		});
	}

	@Override
	public Promise<@Nullable List<Object>> exec() {
		if (isClosed()) return Promise.ofException(CLOSE_EXCEPTION);
		checkState(inTransaction(), "EXEC without MULTI");
		List<Object> transactionResult = this.transactionResult;
		this.transactionResult = null;
		long transactionId = ++completedTransactions;
		return send(RedisCommand.of(EXEC), response -> completeTransaction(response, transactionId)
				.map(completed -> completed ? transactionResult : null));
	}

	@Override
	public Promise<Void> multi() {
		if (isClosed()) return Promise.ofException(CLOSE_EXCEPTION);
		checkState(!inTransaction(), "Nested MULTI call");
		Promise<Void> resultPromise = send(RedisCommand.of(MULTI), this::expectOk);
		transactionResult = new ArrayList<>();
		transactions++;
		return resultPromise;
	}

	@Override
	public Promise<Void> unwatch() {
		if (isClosed()) return Promise.ofException(CLOSE_EXCEPTION);
		return send(RedisCommand.of(UNWATCH), this::expectOk);
	}

	@Override
	public Promise<Void> watch(String key, String... otherKeys) {
		if (isClosed()) return Promise.ofException(CLOSE_EXCEPTION);
		checkState(!inTransaction(), "WATCH inside MULTI");
		return send(RedisCommand.of(WATCH, charset, list(key, otherKeys)), this::expectOk);
	}
	// endregion
	// endregion

	// region connection and pooling
	public boolean inTransaction() {
		return transactionResult != null;
	}

	@Override
	public boolean isClosed() {
		if (closed) return true;
		if (!messaging.isClosed()) return false;
		close();
		return true;
	}

	@Override
	public Promise<Void> returnToPool() {
		if (!receiveQueue.isEmpty()) return Promise.ofException(ACTIVE_COMMANDS);
		if (closed) return Promise.ofException(CLOSE_EXCEPTION);
		if (inPool) return Promise.ofException(IN_POOL);
		client.returnConnection(this);
		return Promise.complete();
	}

	@Override
	public Promise<Void> close(@NotNull Throwable e) {
		if (closed) return Promise.complete();
		closed = true;
		return Promises.all(receiveQueue.iterator())
				.then(() -> Promises.all(transformIterator(multiResultQueue.iterator(), result -> result.cb)))
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
		List<Object> transResult = transactionResult;
		Promise<RedisResponse> receivePromise = transResult != null ? receiveMulti() : receive();

		return sendPromise.then(() -> receivePromise)
				.then(responseParser)
				.whenResult(response -> {
					if (transResult != null) {
						transResult.add(response);
					}
				});
	}

	private Promise<RedisResponse> receive() {
		if (receiveQueue.isEmpty()) {
			Promise<RedisResponse> receivePromise = messaging.receive();
			if (receivePromise.isComplete()) return receivePromise;

			SettablePromise<RedisResponse> newCb = new SettablePromise<>();
			receivePromise
					.whenComplete(receiveQueue::remove)
					.whenComplete(newCb)
					.whenResult(this::onReceive);
			receiveQueue.offer(newCb);
			return newCb;
		} else {
			SettablePromise<RedisResponse> newCb = new SettablePromise<>();
			receiveQueue.offer(newCb);
			return newCb;
		}
	}

	private Promise<RedisResponse> receiveMulti() {
		SettablePromise<RedisResponse> cb = new SettablePromise<>();
		if (receiveQueue.isEmpty()) {
			Promise<RedisResponse> queuedPromise = messaging.receive();

			if (queuedPromise.isComplete()) {
				if (queuedPromise.isException()) {
					abortTransaction(queuedPromise.getException());
					return queuedPromise;
				}
				RedisResponse result = queuedPromise.getResult();
				if (!validateTransaction(result, null)) {
					return Promise.ofException(QUEUED_EXPECTED);
				}
			} else {
				SettablePromise<RedisResponse> queuedCb = new SettablePromise<>();
				queuedPromise
						.whenComplete(receiveQueue::remove)
						.whenComplete(this::validateTransaction)
						.whenComplete(queuedCb)
						.whenResult(this::onReceive);

				receiveQueue.offer(queuedCb);
			}
		} else {
			SettablePromise<RedisResponse> queuedCb = new SettablePromise<>();
			queuedCb.whenComplete(this::validateTransaction);
			receiveQueue.offer(queuedCb);
		}
		multiResultQueue.offer(new Callback(cb));
		return cb;
	}

	private void onReceive() {
		while (!receiveQueue.isEmpty()) {
			Promise<RedisResponse> rcvPromise = messaging.receive()
					.whenComplete((result, e) -> receiveQueue.remove().trySet(result, e));
			if (!rcvPromise.isComplete()) {
				rcvPromise.whenResult(this::onReceive);
				break;
			}
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

	private static Promise<@Nullable Long> parseNullableInteger(RedisResponse response) {
		if (response.isNil()) {
			return Promise.of(null);
		}
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
					try {
						for (Object element : array) {
							if (element == null && nullable) {
								result.add(null);
								continue;
							}
							if (element == null) return unexpectedType(expectedClass);

							result.add(fn.apply(expectedClass.cast(element)));
						}
					} catch (ClassCastException e) {
						return unexpectedType(expectedClass);
					}

					return Promise.of(result);
				});
	}

	private static <T, U> Promise<@Nullable T> unexpectedType(Class<U> expectedClass) {
		return Promise.ofException(unexpectedTypeException(expectedClass));
	}

	private static RedisException unexpectedTypeException(Class<?> expectedClass) {
		return new RedisException(RedisConnection.class,
				"Expected all array items to be of type '" + expectedClass.getSimpleName() +
						"', but some elements were not");
	}

	private <T> Promise<Map<String, T>> parseMap(RedisResponse response, ParserFunction<byte[], T> valueFn) {
		return parseMap(response, keyBytes -> new String(keyBytes, charset), valueFn);
	}

	private static <K, V> Promise<Map<K, V>> parseMap(RedisResponse response, ParserFunction<byte[], K> keyFn, ParserFunction<byte[], V> valueFn) {
		if (response.isArray()) {
			List<?> array = response.getArray();
			if (array.size() % 2 != 0) {
				return Promise.ofException(UNEVEN_MAP);
			}
			Map<K, V> result = new LinkedHashMap<>(array.size());
			try {
				for (int i = 0; i < array.size(); ) {
					byte[] key = (byte[]) array.get(i++);
					byte[] value = (byte[]) array.get(i++);
					K keyK = keyFn.parse(key);
					V valueV = valueFn.parse(value);
					if (result.put(keyK, valueV) != null) {
						return Promise.ofException(new RedisException(RedisConnection.class, "Duplicate field: " + keyK));
					}
				}
				return Promise.of(result);
			} catch (ClassCastException e) {
				return RedisConnection.unexpectedType(byte[].class);
			} catch (ParseException e) {
				return Promise.ofException(new RedisException(RedisConnection.class, "Could not parse value", e));
			}
		}
		return parseError(response);
	}

	private Promise<@Nullable ListPopResult> parseListPopResult(RedisResponse response) {
		return parseArray(response)
				.then(array -> {
					if (array == null) return Promise.of(null);
					if (array.size() != 2) return Promise.ofException(UNEXPECTED_SIZE_OF_ARRAY);
					byte[] foundKey;
					byte[] value;
					try {
						foundKey = (byte[]) array.get(0);
						value = (byte[]) array.get(1);
					} catch (ClassCastException e) {
						return Promise.ofException(UNEXPECTED_TYPES_IN_ARRAY);
					}

					return Promise.of(new ListPopResult(charset, new String(foundKey, charset), value));
				});
	}

	private Promise<@Nullable SetBlockingPopResult> parseSetBlockingPopResult(RedisResponse response) {
		return parseArray(response)
				.then(array -> {
					if (array == null) return Promise.of(null);
					if (array.size() != 3) return Promise.ofException(UNEXPECTED_SIZE_OF_ARRAY);
					byte[] foundKey;
					byte[] value;
					double score;
					try {
						foundKey = (byte[]) array.get(0);
						value = (byte[]) array.get(1);
						score = Double.parseDouble(new String((byte[]) array.get(2), charset));
					} catch (ClassCastException e) {
						return Promise.ofException(UNEXPECTED_TYPES_IN_ARRAY);
					}

					return Promise.of(new SetBlockingPopResult(charset, new String(foundKey, charset), value, score));
				});
	}

	private Promise<@Nullable List<SetPopResult>> parseSetPopResults(RedisResponse response) {
		return parseArray(response)
				.then(array -> {
					if (array == null) return Promise.of(null);
					if (array.size() % 2 != 0) return Promise.ofException(UNEXPECTED_SIZE_OF_ARRAY);

					List<SetPopResult> result = new ArrayList<>();
					for (int i = 0; i < array.size(); ) {
						byte[] value;
						double score;
						try {
							value = (byte[]) array.get(i++);
							score = Double.parseDouble(new String((byte[]) array.get(i++), charset));
						} catch (ClassCastException e) {
							return Promise.ofException(UNEXPECTED_TYPES_IN_ARRAY);
						}

						result.add(new SetPopResult(charset, value, score));
					}
					return Promise.of(result);
				});
	}

	private Promise<List<GeoradiusResult>> parseGeoradiusResults(RedisResponse response, boolean withCoord, boolean withDist, boolean withHash) {
		return parseArray(response)
				.then(array -> {
					if (array == null) return Promise.ofException(UNEXPECTED_NIL);
					int subArrayLength = 1 + (withCoord ? 1 : 0) + (withDist ? 1 : 0) + (withHash ? 1 : 0);
					List<GeoradiusResult> result = new ArrayList<>(array.size());
					for (Object element : array) {
						if (!(element instanceof List)) return unexpectedType(List.class);
						List<?> subArray = (List<?>) element;
						if (subArray.size() != subArrayLength) return Promise.ofException(UNEXPECTED_SIZE_OF_ARRAY);

						int index = 0;
						byte[] member;
						try {
							member = (byte[]) subArray.get(index++);
						} catch (ClassCastException ignored) {
							return unexpectedType(byte[].class);
						}

						Coordinate coordinate = null;
						Double dist = null;
						Long hash = null;
						if (withDist) {
							try {
								dist = Double.parseDouble(new String((byte[]) subArray.get(index++), charset));
							} catch (ClassCastException ignored) {
								return unexpectedType(byte[].class);
							}
						}
						if (withHash) {
							try {
								hash = (Long) subArray.get(index++);
							} catch (ClassCastException ignored) {
								return unexpectedType(Long.class);
							}
						}
						if (withCoord) {
							try {
								coordinate = doParseCoordinate(subArray.get(index));
							} catch (RedisException e) {
								return Promise.ofException(e);
							}
						}

						result.add(new GeoradiusResult(charset, member, coordinate, dist, hash));
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

	private Promise<List<String>> parseStrings(RedisResponse response) {
		return parseArray(response, byte[].class, bytes -> new String(bytes, charset))
				.then(RedisConnection::nonNull);
	}

	private Promise<List<String>> parseNullableStrings(RedisResponse response) {
		return parseArray(response, byte[].class, bytes -> new String(bytes, charset), true)
				.then(RedisConnection::nonNull);
	}

	private static Promise<List<byte[]>> parseBytes(RedisResponse response) {
		return parseArray(response, byte[].class, Function.identity())
				.then(RedisConnection::nonNull);
	}

	private Promise<Double> parseDouble(RedisResponse response) {
		return doParseDouble(response, false);
	}

	private Promise<Double> parseNullableDouble(RedisResponse response) {
		return doParseDouble(response, true);
	}

	private Promise<Double> doParseDouble(RedisResponse response, boolean nullable) {
		return parseBulkString(response)
				.then(string -> {
					if (string == null) {
						if (nullable) return Promise.of(null);
						return Promise.ofException(UNEXPECTED_NIL);
					}
					try {
						return Promise.of(Double.parseDouble(string));
					} catch (NumberFormatException e) {
						return Promise.ofException(new RedisException(RedisConnection.class, "Could not parse result as double: " + string));
					}
				});
	}

	private Promise<Void> expectOk(RedisResponse response) {
		return parseString(response)
				.then(result -> "OK".equals(result) ? Promise.complete() :
						Promise.ofException(new RedisException(RedisConnection.class, "Expected result to be 'OK', was: " + result)));
	}

	private Promise<List<@Nullable Coordinate>> parseCoordinates(RedisResponse response) {
		if (!response.isArray()) return parseError(response);
		List<?> array = response.getArray();
		List<@Nullable Coordinate> result = new ArrayList<>(array.size());
		for (Object element : array) {
			if (element == null) {
				result.add(null);
				continue;
			}

			try {
				result.add(doParseCoordinate(element));
			} catch (RedisException e) {
				return Promise.ofException(e);
			}
		}
		return Promise.of(result);
	}

	@SuppressWarnings("unchecked")
	private Coordinate doParseCoordinate(Object element) throws RedisException {
		try {
			List<byte[]> coordinatesList = (List<byte[]>) element;
			if (coordinatesList.size() != 2) {
				throw UNEXPECTED_SIZE_OF_ARRAY;
			}

			return new Coordinate(
					Double.parseDouble(new String(coordinatesList.get(0), charset)),
					Double.parseDouble(new String(coordinatesList.get(1), charset))
			);
		} catch (ClassCastException e) {
			throw unexpectedTypeException(List.class);
		}

	}

	private static <T> Promise<@NotNull T> nonNull(@Nullable T value) {
		return value == null ?
				Promise.ofException(UNEXPECTED_NIL) :
				Promise.of(value);
	}

	private Promise<Long> doZstore(Command storeCommand, String destination, @Nullable Aggregate aggregate, Map<String, Double> entries) {
		checkArgument(!entries.isEmpty(), "No key specified");

		List<byte[]> args = new ArrayList<>(entries.size() + 5);
		args.add(destination.getBytes(charset));
		args.add(String.valueOf(entries.size()).getBytes(charset));
		List<byte[]> weights = new ArrayList<>();
		for (Map.Entry<String, Double> entry : entries.entrySet()) {
			args.add(entry.getKey().getBytes(charset));
			weights.add(String.valueOf(entry.getValue()).getBytes(charset));
		}
		args.add(WEIGHTS.getBytes(charset));
		args.addAll(weights);
		if (aggregate != null) {
			args.add(AGGREGATE.getBytes(charset));
			args.add(aggregate.name().getBytes(charset));
		}
		return send(RedisCommand.of(storeCommand, args), RedisConnection::parseInteger);
	}

	private Promise<Long> doZstore(Command storeCommand, String destination, @Nullable Aggregate aggregate, String key, String[] otherKeys) {
		List<byte[]> args = new ArrayList<>(otherKeys.length + 4);
		args.add(destination.getBytes(charset));
		args.add(String.valueOf(otherKeys.length + 1).getBytes(charset));
		args.add(key.getBytes(charset));
		for (String otherKey : otherKeys) {
			args.add(otherKey.getBytes(charset));
		}
		if (aggregate != null) {
			args.add(AGGREGATE.getBytes(charset));
			args.add(aggregate.name().getBytes(charset));
		}
		return send(RedisCommand.of(storeCommand, args), RedisConnection::parseInteger);
	}

	private Promise<Long> doGeoradiusStore(String key, Either<Coordinate, String> eitherCoordOrMember, double radius, DistanceUnit unit, GeoradiusModifier[] modifiers) {
		checkGeoradiusModifiers(false, modifiers);
		List<String> arguments = new ArrayList<>(modifiers.length * 2 + 5);
		arguments.add(key);
		Command command;
		if (eitherCoordOrMember.isLeft()) {
			command = GEORADIUS;
			Coordinate coord = eitherCoordOrMember.getLeft();
			arguments.add(String.valueOf(coord.getLongitude()));
			arguments.add(String.valueOf(coord.getLatitude()));
		} else {
			command = GEORADIUSBYMEMBER;
			arguments.add(eitherCoordOrMember.getRight());
		}
		arguments.add(String.valueOf(radius));
		arguments.add(unit.getArgument());
		for (GeoradiusModifier modifier : modifiers) {
			arguments.addAll(modifier.getArguments());
		}
		return send(RedisCommand.of(command, charset, arguments), RedisConnection::parseInteger);
	}

	private Promise<List<GeoradiusResult>> doGeoradiusReadOnly(String key, Either<Coordinate, String> eitherCoordOrMember, double radius, DistanceUnit unit, GeoradiusModifier[] modifiers) {
		checkGeoradiusModifiers(true, modifiers);
		List<String> arguments = new ArrayList<>(modifiers.length * 2 + 5);
		arguments.add(key);
		Command command;
		if (eitherCoordOrMember.isLeft()) {
			command = GEORADIUS;
			Coordinate coord = eitherCoordOrMember.getLeft();
			arguments.add(String.valueOf(coord.getLongitude()));
			arguments.add(String.valueOf(coord.getLatitude()));
		} else {
			command = GEORADIUSBYMEMBER;
			arguments.add(eitherCoordOrMember.getRight());
		}
		arguments.add(String.valueOf(radius));
		arguments.add(unit.getArgument());
		boolean withCoord = false, withDist = false, withHash = false;
		for (GeoradiusModifier modifier : modifiers) {
			List<String> args = modifier.getArguments();
			String modifierType = args.get(0);
			if (WITHCOORD.equals(modifierType)) withCoord = true;
			else if (WITHDIST.equals(modifierType)) withDist = true;
			else if (WITHHASH.equals(modifierType)) withHash = true;
			arguments.addAll(args);
		}
		boolean finalWithCoord = withCoord, finalWithDist = withDist, finalWithHash = withHash;
		return send(RedisCommand.of(command, charset, arguments), response -> parseGeoradiusResults(response, finalWithCoord, finalWithDist, finalWithHash));
	}

	private class Callback {
		private final SettablePromise<RedisResponse> cb;
		private final long transactionId;

		private Callback(SettablePromise<RedisResponse> cb) {
			this.cb = cb;
			this.transactionId = inTransaction() ? transactions : NO_TRANSACTION;
		}
	}

	private boolean validateTransaction(@Nullable RedisResponse response, @Nullable Throwable e) {
		if (e != null) {
			abortTransaction(e);
			return false;
		}
		assert response != null;
		if (response.isError()) {
			abortTransaction(response.getError());
			return false;
		}
		if (!response.isString() || !QUEUED.equals(response.getString())) {
			abortTransaction(QUEUED_EXPECTED);
			return false;
		}
		return true;
	}

	private void abortTransaction(Throwable e) {
		abortTransaction(e, completedTransactions);
	}

	private void abortTransaction(Throwable e, long transactionId) {
		while (!multiResultQueue.isEmpty()) {
			Callback callback = multiResultQueue.peek();
			if (callback.transactionId != transactionId) break;
			multiResultQueue.remove();
			callback.cb.setException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private Promise<Boolean> completeTransaction(RedisResponse transactionResponse, long transactionId) {
		if (transactionResponse.isNil()) {
			abortTransaction(TRANSACTION_FAILED, transactionId);
			return Promise.of(false);
		}

		if (!transactionResponse.isArray()) {
			abortTransaction(UNEXPECTED_RESPONSE, transactionId);
			return Promise.ofException(UNEXPECTED_RESPONSE);
		}

		List<Object> responses = (List<Object>) transactionResponse.getArray();
		List<SettablePromise<RedisResponse>> pending = new ArrayList<>(responses.size());
		while (!multiResultQueue.isEmpty()) {
			Callback callback = multiResultQueue.peek();
			if (callback.transactionId != transactionId) break;
			multiResultQueue.remove();
			pending.add(callback.cb);
		}
		if (responses.size() != pending.size()) {
			for (SettablePromise<RedisResponse> pendingCb : pending) {
				pendingCb.setException(RESPONSES_SIZE_MISMATCH);
			}
			return Promise.ofException(RESPONSES_SIZE_MISMATCH);
		}

		for (int i = 0; i < responses.size(); i++) {
			Object response = responses.get(i);
			SettablePromise<RedisResponse> cb = pending.get(i);

			if (response instanceof Long) {
				cb.set(RedisResponse.integer((Long) response));
			} else if (response instanceof String) {
				cb.set(RedisResponse.string((String) response));
			} else if (response instanceof byte[]) {
				cb.set(RedisResponse.bytes((byte[]) response));
			} else if (response instanceof ServerError) {
				cb.set(RedisResponse.error((ServerError) response));
			} else if (response instanceof List) {
				cb.set(RedisResponse.array((List<?>) response));
			} else {
				cb.set(RedisResponse.nil());
			}
		}

		return Promise.of(true);
	}

}
